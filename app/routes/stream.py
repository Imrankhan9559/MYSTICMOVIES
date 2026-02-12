import math
import asyncio
import logging
import os
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException, Header, Body
from fastapi.responses import StreamingResponse, HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client
from app.db.models import FileSystemItem, User, PlaybackProgress
from app.core.config import settings
from app.core.telegram_bot import (
    tg_client,
    bot_client,
    bot_pool,
    user_client,
    get_pool_client,
    ensure_peer_access,
    get_storage_client,
    get_storage_chat_id,
    pick_storage_client,
    normalize_chat_id
)
from app.core.telethon_storage import get_message as tl_get_message, iter_download as tl_iter_download
from app.core.hls import ensure_hls, is_hls_ready, hls_url_for

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

async def get_current_user(request: Request):
    phone = request.cookies.get("user_phone")
    if not phone: return None
    user = await User.find_one(User.phone_number == phone)
    if not user or getattr(user, "status", "approved") != "approved":
        return None
    return user

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    if str(getattr(user, "role", "") or "").strip().lower() == "admin":
        return True
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

def _can_access(user: User, item: FileSystemItem, is_admin: bool) -> bool:
    if is_admin: return True
    return item.owner_phone == user.phone_number or user.phone_number in (item.collaborators or [])

def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg"))

def _pick_align(size: int, for_download: bool = False) -> int:
    # Balance seek responsiveness with throughput to reduce buffering.
    if for_download:
        if size and size >= 200 * 1024 * 1024:
            return 1024 * 1024
        if size and size >= 20 * 1024 * 1024:
            return 512 * 1024
        if size and size >= 2 * 1024 * 1024:
            return 128 * 1024
        return 4096
    if size and size >= 200 * 1024 * 1024:
        return 256 * 1024
    if size and size >= 20 * 1024 * 1024:
        return 128 * 1024
    if size and size >= 2 * 1024 * 1024:
        return 64 * 1024
    return 4096

def _align_offset(start: int, align: int = 4096) -> tuple[int, int]:
    if start < 0:
        start = 0
    aligned = start - (start % align)
    skip = start - aligned
    return aligned, skip

def _align_range(start: int, length: int, align: int = 4096) -> tuple[int, int, int]:
    aligned_start, skip = _align_offset(start, align)
    aligned_length = length + skip
    if aligned_length % align != 0:
        aligned_length = ((aligned_length // align) + 1) * align
    if aligned_length < align:
        aligned_length = align
    return aligned_start, aligned_length, skip


def _extract_file_id(msg):
    if not msg:
        return None
    if msg.document:
        return msg.document.file_id
    if msg.video:
        return msg.video.file_id
    if msg.audio:
        return msg.audio.file_id
    if msg.photo:
        return msg.photo.file_id
    return None


def _extract_file_size(msg) -> int | None:
    if not msg:
        return None
    if msg.document:
        return getattr(msg.document, "file_size", None)
    if msg.video:
        return getattr(msg.video, "file_size", None)
    if msg.audio:
        return getattr(msg.audio, "file_size", None)
    if msg.photo and getattr(msg.photo, "sizes", None):
        sizes = msg.photo.sizes
        if sizes:
            return getattr(sizes[-1], "size", None)
    return None


_client_locks: dict[int, asyncio.Lock] = {}

async def _try_acquire(lock: asyncio.Lock) -> bool:
    if lock.locked():
        return False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=0)
        return True
    except Exception:
        return False

async def _get_parallel_clients(chat_id: int | str, max_workers: int | None = None):
    candidates: list[Client] = []
    if bot_pool:
        candidates.extend(bot_pool)
    if bot_client:
        candidates.append(bot_client)
    if user_client:
        candidates.append(user_client)
    if tg_client:
        candidates.append(tg_client)

    # Deduplicate
    unique: list[Client] = []
    seen = set()
    for client in candidates:
        key = id(client)
        if key in seen:
            continue
        seen.add(key)
        unique.append(client)

    usable: list[Client] = []
    acquired: list[asyncio.Lock] = []
    for client in unique:
        if max_workers is not None and len(usable) >= max_workers:
            break
        try:
            if not await ensure_peer_access(client, chat_id):
                continue
        except Exception:
            continue
        lock = _client_locks.setdefault(id(client), asyncio.Lock())
        if await _try_acquire(lock):
            usable.append(client)
            acquired.append(lock)

    async def release():
        for lock in acquired:
            if lock.locked():
                lock.release()

    return usable, release


async def parallel_stream_generator(
    clients: list[Client],
    chat_id: int | str,
    message_id: int,
    start: int,
    end: int,
    chunk_size: int = 512 * 1024
):
    total = end - start + 1
    if total <= 0:
        return

    align = _pick_align(total, for_download=False)
    if chunk_size < align:
        chunk_size = align
    # Ensure chunk_size is aligned
    if chunk_size % align != 0:
        chunk_size = (chunk_size // align) * align
        if chunk_size < align:
            chunk_size = align

    aligned_start, skip = _align_offset(start, align)
    total_aligned = total + skip
    num_chunks = (total_aligned + chunk_size - 1) // chunk_size

    queue: asyncio.Queue[int | None] = asyncio.Queue()
    for idx in range(num_chunks):
        queue.put_nowait(idx)
    for _ in range(len(clients)):
        queue.put_nowait(None)

    results: dict[int, bytes] = {}
    cond = asyncio.Condition()
    error: Exception | None = None

    async def worker(client: Client):
        nonlocal error
        try:
            msg = await client.get_messages(chat_id, message_ids=message_id)
            file_id = _extract_file_id(msg)
            if not file_id:
                raise RuntimeError("Missing file id for parallel stream.")
            while True:
                idx = await queue.get()
                if idx is None:
                    break
                offset = aligned_start + idx * chunk_size
                limit = min(chunk_size, total_aligned - idx * chunk_size)
                request_limit = limit
                if request_limit % align != 0:
                    request_limit = ((request_limit // align) + 1) * align
                buf = bytearray()
                async for part in client.stream_media(file_id, offset=offset, limit=request_limit):
                    if not part:
                        break
                    buf.extend(part)
                    if len(buf) >= limit:
                        break
                if len(buf) < limit:
                    raise RuntimeError(f"Short read in parallel stream (got {len(buf)} of {limit}).")
                async with cond:
                    results[idx] = bytes(buf)
                    cond.notify_all()
        except Exception as e:
            async with cond:
                if error is None:
                    error = e
                cond.notify_all()

    workers = [asyncio.create_task(worker(c)) for c in clients]
    sent = 0
    try:
        for idx in range(num_chunks):
            async with cond:
                await cond.wait_for(lambda: idx in results or error is not None)
                if error is not None:
                    raise error
                data = results.pop(idx)
            if idx == 0 and skip:
                if skip >= len(data):
                    continue
                data = data[skip:]
            remaining = total - sent
            if remaining <= 0:
                break
            if len(data) > remaining:
                data = data[:remaining]
            sent += len(data)
            if data:
                yield data
    finally:
        for task in workers:
            task.cancel()


async def telegram_stream_generator(
    client: Client,
    chat_id: int | str,
    message_id: int,
    offset: int,
    limit: int | None = None,
    skip_bytes: int = 0
):
    try:
        # Refresh File Reference
        if not await ensure_peer_access(client, chat_id):
            return
        msg = await client.get_messages(chat_id, message_ids=message_id)

        file_id = None
        if msg.document:
            file_id = msg.document.file_id
        elif msg.video:
            file_id = msg.video.file_id
        elif msg.audio:
            file_id = msg.audio.file_id
        elif msg.photo:
            file_id = msg.photo.file_id
        if not file_id:
            return

        remaining_skip = skip_bytes
        async for chunk in client.stream_media(file_id, offset=offset, limit=limit or 0):
            if not chunk:
                break
            if remaining_skip:
                if len(chunk) <= remaining_skip:
                    remaining_skip -= len(chunk)
                    continue
                chunk = chunk[remaining_skip:]
                remaining_skip = 0
            yield chunk
    except Exception as e:
        print(f"Stream Error: {e}")
        return

@router.get("/player/{item_id}", response_class=HTMLResponse)
async def player_page(request: Request, item_id: str):
    user = await get_current_user(request)
    
    # If not logged in, redirect to login page
    if not user: 
        return RedirectResponse("/login")

    item = await FileSystemItem.get(item_id)
    if not item: raise HTTPException(404, "File not found")
    if not _can_access(user, item, _is_admin(user)):
        raise HTTPException(403, "Not authorized")

    resume_at = 0
    progress = await PlaybackProgress.find_one(
        PlaybackProgress.user_type == "user",
        PlaybackProgress.user_key == user.phone_number,
        PlaybackProgress.item_id == str(item.id)
    )
    if progress and progress.position:
        resume_at = progress.position
    else:
        public_name = user.requested_name or user.first_name
        if public_name:
            public_progress = await PlaybackProgress.find_one(
                PlaybackProgress.user_type == "public",
                PlaybackProgress.user_key == public_name,
                PlaybackProgress.item_id == str(item.id)
            )
            if public_progress and public_progress.position:
                resume_at = public_progress.position

    # Prepare HLS if possible
    hls_url = ""
    if _is_video_item(item):
        if item.parts and item.parts[0].chat_id:
            chat_id = normalize_chat_id(item.parts[0].chat_id)
        else:
            chat_id = normalize_chat_id(get_storage_chat_id() or "me")
        await ensure_hls(item, chat_id, user.session_string if chat_id == "me" else None)
        if is_hls_ready(str(item.id)):
            hls_url = hls_url_for(str(item.id))

    return templates.TemplateResponse("player.html", {
        "request": request,
        "item": item,
        "stream_url": f"/stream/data/{item_id}",
        "user": user,  # <--- FIX: This was missing! Now the navbar will show 'Profile'
        "is_admin": _is_admin(user),
        "resume_at": resume_at,
        "hls_url": hls_url
    })

@router.post("/hls/prepare/{item_id}")
async def prepare_hls(request: Request, item_id: str):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401)
    item = await FileSystemItem.get(item_id)
    if not item: raise HTTPException(404)
    if not _can_access(user, item, _is_admin(user)):
        raise HTTPException(403)
    if item.parts and item.parts[0].chat_id:
        chat_id = normalize_chat_id(item.parts[0].chat_id)
    else:
        chat_id = normalize_chat_id(get_storage_chat_id() or "me")
    await ensure_hls(item, chat_id, user.session_string if chat_id == "me" else None)
    return {"status": "started"}

@router.get("/stream/data/{item_id}")
async def stream_data(request: Request, item_id: str, range: str = Header(None)):
    user = await get_current_user(request)
    if not user: raise HTTPException(401)

    item = await FileSystemItem.get(item_id)
    if not item: raise HTTPException(404)
    if not _can_access(user, item, _is_admin(user)):
        raise HTTPException(403)

    if item.parts and item.parts[0].chat_id:
        chat_id = item.parts[0].chat_id
        from_storage = False
    else:
        chat_id = get_storage_chat_id() or "me"
        from_storage = True
    chat_id = normalize_chat_id(chat_id)

    file_size = item.size or 0
    start = 0
    end = file_size - 1 if file_size else 0

    use_ephemeral = False
    storage_client = None
    storage_primary: Client | None = None
    parallel_clients: list[Client] = []
    release_parallel = None
    if chat_id == "me":
        client = Client("streamer", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=user.session_string, in_memory=True)
        await client.connect()
        use_ephemeral = True
    else:
        client = None
        if from_storage:
            chat_id = normalize_chat_id(get_storage_chat_id())
        try:
            if bot_pool:
                candidate = get_pool_client()
                if candidate and await ensure_peer_access(candidate, chat_id):
                    storage_client = candidate
        except Exception:
            storage_client = None
        if not storage_client:
            try:
                storage_client = await pick_storage_client(chat_id)
            except Exception:
                storage_client = None
        try:
            try:
                max_workers = int(os.getenv("DL_WORKERS", "6"))
            except Exception:
                max_workers = 6
            parallel_clients, release_parallel = await _get_parallel_clients(chat_id, max_workers=max_workers)
        except Exception:
            parallel_clients = []
            release_parallel = None
        if storage_client and storage_client not in parallel_clients:
            parallel_clients.append(storage_client)
        storage_primary = storage_client or (parallel_clients[0] if parallel_clients else None)

    msg_id = item.parts[0].message_id
    # Probe actual size to avoid OFFSET_INVALID on wrong DB sizes
    try:
        probe_client = client if chat_id == "me" else storage_primary
        if probe_client and await ensure_peer_access(probe_client, chat_id):
            msg = await probe_client.get_messages(chat_id, message_ids=msg_id)
            actual_size = _extract_file_size(msg)
            if actual_size:
                file_size = actual_size
                if item.size != actual_size:
                    try:
                        item.size = actual_size
                        await item.save()
                    except Exception:
                        pass
    except Exception:
        pass

    def _apply_range(size: int):
        start_local = 0
        end_local = size - 1 if size else 0
        if range:
            try:
                range_val = range.replace("bytes=", "")
                parts = range_val.split("-")
                if parts[0]:
                    start_local = int(parts[0])
                if len(parts) > 1 and parts[1]:
                    end_local = int(parts[1])
                else:
                    end_local = size - 1 if size else 0
                if size and end_local >= size:
                    end_local = size - 1
            except ValueError:
                start_local = 0
                end_local = size - 1 if size else 0
        return start_local, end_local

    start, end = _apply_range(file_size)
    if file_size:
        if start >= file_size:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})
        if end >= file_size:
            end = file_size - 1
    align = _pick_align(file_size, for_download=False)

    async def cleanup_generator():
        try:
            if chat_id == "me":
                sent = 0
                limit = (end - start + 1) if file_size else None
                aligned_offset, aligned_limit, skip = _align_range(start, limit or 0, align)
                async for chunk in telegram_stream_generator(client, chat_id, msg_id, aligned_offset, aligned_limit, skip):
                    if limit is not None:
                        remaining = limit - sent
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            yield chunk[:remaining]
                            break
                        sent += len(chunk)
                    yield chunk
            elif storage_primary:
                limit = (end - start + 1) if file_size else None
                if parallel_clients and len(parallel_clients) > 1 and file_size:
                    sent = 0
                    try:
                        async for chunk in parallel_stream_generator(parallel_clients, chat_id, msg_id, start, end):
                            sent += len(chunk)
                            yield chunk
                    except Exception as e:
                        logger.warning(f"Parallel stream failed, falling back: {e}")
                        resume_start = start + sent
                        if limit is not None and resume_start <= end:
                            remaining_total = end - resume_start + 1
                            aligned_offset, aligned_limit, skip = _align_range(resume_start, remaining_total, align)
                            sent_fallback = 0
                            async for chunk in telegram_stream_generator(storage_primary, chat_id, msg_id, aligned_offset, aligned_limit, skip):
                                remaining = remaining_total - sent_fallback
                                if remaining <= 0:
                                    break
                                if len(chunk) > remaining:
                                    yield chunk[:remaining]
                                    break
                                sent_fallback += len(chunk)
                                yield chunk
                            if sent_fallback < remaining_total:
                                try:
                                    msg = await tl_get_message(msg_id)
                                    async for chunk in tl_iter_download(msg, offset=resume_start + sent_fallback, limit=remaining_total - sent_fallback):
                                        yield chunk
                                except Exception:
                                    pass
                else:
                    sent = 0
                    aligned_offset, aligned_limit, skip = _align_range(start, limit or 0, align)
                    async for chunk in telegram_stream_generator(storage_primary, chat_id, msg_id, aligned_offset, aligned_limit, skip):
                        if limit is not None:
                            remaining = limit - sent
                            if remaining <= 0:
                                break
                            if len(chunk) > remaining:
                                yield chunk[:remaining]
                                break
                            sent += len(chunk)
                        yield chunk
                    if limit is not None and sent < limit:
                        try:
                            msg = await tl_get_message(msg_id)
                            async for chunk in tl_iter_download(msg, offset=start + sent, limit=limit - sent):
                                yield chunk
                        except Exception:
                            pass
            else:
                msg = await tl_get_message(msg_id)
                limit = (end - start + 1) if file_size else None
                async for chunk in tl_iter_download(msg, offset=start, limit=limit):
                    yield chunk
        finally:
            try:
                if release_parallel:
                    await release_parallel()
            except Exception:
                pass
            if use_ephemeral and client:
                await client.disconnect()

    content_length = (end - start + 1) if file_size else None
    headers = {
        'Accept-Ranges': 'bytes',
        'Content-Type': item.mime_type or "application/octet-stream",
        'Content-Disposition': f'inline; filename=\"{item.name}\"'
    }
    if file_size:
        headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
        headers['Content-Length'] = str(content_length)

    return StreamingResponse(cleanup_generator(), status_code=206 if range else 200, headers=headers, media_type=item.mime_type)

@router.post("/progress")
async def update_progress(request: Request, payload: dict = Body(...)):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401)

    item_id = payload.get("item_id")
    position = float(payload.get("position", 0))
    duration = float(payload.get("duration", 0))
    if not item_id:
        return {"error": "Missing item_id"}

    progress = await PlaybackProgress.find_one(
        PlaybackProgress.user_type == "user",
        PlaybackProgress.user_key == user.phone_number,
        PlaybackProgress.item_id == item_id
    )
    if progress:
        progress.position = position
        progress.duration = duration
        progress.updated_at = datetime.now()
        await progress.save()
    else:
        progress = PlaybackProgress(
            user_key=user.phone_number,
            user_type="user",
            item_id=item_id,
            position=position,
            duration=duration,
            updated_at=datetime.now()
        )
        await progress.insert()
    return {"status": "success"}
