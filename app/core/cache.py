import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import AsyncGenerator, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

_cache_root: Path | None = None
_cache_tasks: dict[str, asyncio.Task] = {}
_cache_lock = asyncio.Lock()
_trim_lock = asyncio.Lock()


def cache_enabled() -> bool:
    return bool(getattr(settings, "CACHE_ENABLED", True))


def _cache_max_bytes() -> int:
    try:
        return int(float(getattr(settings, "CACHE_MAX_GB", 20)) * 1024 ** 3)
    except Exception:
        return 20 * 1024 ** 3


def _chunk_bytes() -> int:
    try:
        mb = int(getattr(settings, "CACHE_CHUNK_MB", 8))
        return max(1, mb) * 1024 * 1024
    except Exception:
        return 8 * 1024 * 1024


def _max_workers() -> int:
    try:
        return max(1, int(getattr(settings, "CACHE_MAX_WORKERS", 4)))
    except Exception:
        return 4


def get_cache_root() -> Path:
    global _cache_root
    if _cache_root is not None:
        return _cache_root

    base = (getattr(settings, "CACHE_DIR", "") or "").strip()
    if base:
        root = Path(base)
    else:
        render_disk = Path("/var/data")
        if render_disk.exists():
            root = render_disk / "mystic_cache"
        else:
            root = Path(tempfile.gettempdir()) / "mystic_cache"

    root.mkdir(parents=True, exist_ok=True)
    (root / "files").mkdir(parents=True, exist_ok=True)
    (root / "hls").mkdir(parents=True, exist_ok=True)
    _cache_root = root
    return root


def files_root() -> Path:
    return get_cache_root() / "files"


def hls_root() -> Path:
    return get_cache_root() / "hls"


def file_cache_path(item_id: str) -> Path:
    return files_root() / f"{item_id}.bin"


def is_file_cached(item_id: str, size: Optional[int]) -> bool:
    path = file_cache_path(item_id)
    if not path.exists():
        return False
    if size:
        return path.stat().st_size >= size
    return True


def touch_path(path: Path) -> None:
    try:
        os.utime(path, None)
    except Exception:
        pass


def link_or_copy(src: Path, dest: Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(src, dest)
    except Exception:
        shutil.copy2(src, dest)


async def iter_file_range(
    path: Path, start: int, end: int, chunk_size: int = 1024 * 1024
) -> AsyncGenerator[bytes, None]:
    if end < start:
        return
    with open(path, "rb") as f:
        f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            read_size = min(chunk_size, remaining)
            data = await asyncio.to_thread(f.read, read_size)
            if not data:
                break
            remaining -= len(data)
            yield data


def get_cache_task(item_id: str) -> Optional[asyncio.Task]:
    return _cache_tasks.get(item_id)


async def wait_for_cache(item_id: str, timeout: float | None = None) -> None:
    task = _cache_tasks.get(item_id)
    if not task or task.done():
        return
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except Exception:
        return


async def init_cache() -> None:
    if not cache_enabled():
        return
    get_cache_root()
    await trim_cache()


async def trim_cache() -> None:
    if not cache_enabled():
        return
    async with _trim_lock:
        await asyncio.to_thread(_trim_cache_sync)


def _trim_cache_sync() -> None:
    max_bytes = _cache_max_bytes()
    if max_bytes <= 0:
        return
    root = get_cache_root()
    files = []
    total = 0
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.endswith(".part"):
                continue
            path = Path(dirpath) / name
            try:
                stat = path.stat()
            except Exception:
                continue
            total += stat.st_size
            files.append((stat.st_mtime, stat.st_size, path))

    if total <= max_bytes:
        return

    files.sort(key=lambda x: x[0])
    for _, size, path in files:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            continue
        total -= size
        if total <= max_bytes:
            break


async def warm_cache_for_item(item, chat_id: int | str, user_session_string: Optional[str] = None) -> None:
    if not cache_enabled():
        return
    if not item or getattr(item, "is_folder", False):
        return
    size = int(getattr(item, "size", 0) or 0)
    if size <= 0:
        return
    if size > _cache_max_bytes():
        logger.info("Skipping cache warm: file larger than cache limit.")
        return

    item_id = str(getattr(item, "id"))
    if is_file_cached(item_id, size):
        touch_path(file_cache_path(item_id))
        return

    # Avoid AuthKeyDuplicated for Saved Messages sessions
    if str(chat_id) == "me":
        return

    async with _cache_lock:
        existing = _cache_tasks.get(item_id)
        if existing and not existing.done():
            return
        _cache_tasks[item_id] = asyncio.create_task(
            _download_full_file(item, chat_id, user_session_string)
        )


async def _download_full_file(item, chat_id: int | str, user_session_string: Optional[str]) -> None:
    item_id = str(getattr(item, "id"))
    size = int(getattr(item, "size", 0) or 0)
    if size <= 0:
        return
    if size > _cache_max_bytes():
        return
    if str(chat_id) == "me":
        return

    final_path = file_cache_path(item_id)
    if is_file_cached(item_id, size):
        touch_path(final_path)
        return

    await trim_cache()

    tmp_path = final_path.with_suffix(".part")
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp_path, "wb") as f:
            f.truncate(size)
    except Exception as e:
        logger.error(f"Cache warm failed to allocate file: {e}")
        return

    ok = await _download_parallel_pyrogram(item, chat_id, tmp_path)
    if ok:
        try:
            os.replace(tmp_path, final_path)
            touch_path(final_path)
        except Exception as e:
            logger.error(f"Cache warm rename failed: {e}")
    await trim_cache()


async def _download_parallel_pyrogram(item, chat_id: int | str, dest_path: Path) -> bool:
    from app.core.telegram_bot import (
        bot_client,
        bot_pool,
        ensure_peer_access,
        normalize_chat_id,
        tg_client,
        user_client,
    )

    item_id = str(getattr(item, "id"))
    msg_id = None
    try:
        msg_id = item.parts[0].message_id
    except Exception:
        return False
    if not msg_id:
        return False

    chat_id = normalize_chat_id(chat_id)

    candidates = []
    if bot_pool:
        candidates.extend(bot_pool)
    if bot_client:
        candidates.append(bot_client)
    if user_client:
        candidates.append(user_client)
    if tg_client:
        candidates.append(tg_client)

    # Deduplicate clients
    unique = []
    seen = set()
    for client in candidates:
        if id(client) in seen:
            continue
        seen.add(id(client))
        unique.append(client)

    usable = []
    for client in unique:
        try:
            if await ensure_peer_access(client, chat_id):
                usable.append(client)
        except Exception:
            continue

    if not usable:
        return False

    if not getattr(settings, "CACHE_PARALLEL_CHUNKS", True):
        usable = usable[:1]

    workers = min(len(usable), _max_workers())
    if workers <= 1:
        return await _download_single_client(usable[0], chat_id, msg_id, dest_path)

    queue: asyncio.Queue[int] = asyncio.Queue()
    size = int(getattr(item, "size", 0) or 0)
    if size <= 0:
        return False

    chunk_size = _chunk_bytes()
    for offset in range(0, size, chunk_size):
        queue.put_nowait(offset)

    errors: list[Exception] = []

    async def worker(client):
        try:
            msg = await client.get_messages(chat_id, message_ids=msg_id)
            file_id = _extract_file_id(msg)
            if not file_id:
                raise RuntimeError("Missing file_id for cache warm")
            with open(dest_path, "r+b") as f:
                while True:
                    try:
                        offset = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    limit = min(chunk_size, size - offset)
                    pos = offset
                    async for chunk in client.stream_media(file_id, offset=offset, limit=limit):
                        f.seek(pos)
                        f.write(chunk)
                        pos += len(chunk)
        except Exception as e:
            errors.append(e)

    await asyncio.gather(*(worker(c) for c in usable[:workers]))
    if errors:
        logger.warning(f"Cache warm incomplete for {item_id}: {errors[0]}")
    return len(errors) == 0


async def _download_single_client(client, chat_id: int | str, msg_id: int, dest_path: Path) -> bool:
    try:
        msg = await client.get_messages(chat_id, message_ids=msg_id)
        file_id = _extract_file_id(msg)
        if not file_id:
            return False
        with open(dest_path, "r+b") as f:
            async for chunk in client.stream_media(file_id, offset=0, limit=0):
                f.write(chunk)
        return True
    except Exception as e:
        logger.warning(f"Cache warm failed: {e}")
        return False


def _extract_file_id(msg) -> Optional[str]:
    if not msg:
        return None
    if getattr(msg, "document", None):
        return msg.document.file_id
    if getattr(msg, "video", None):
        return msg.video.file_id
    if getattr(msg, "audio", None):
        return msg.audio.file_id
    if getattr(msg, "photo", None):
        return msg.photo.file_id
    return None
