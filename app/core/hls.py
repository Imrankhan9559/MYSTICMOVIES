import asyncio
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from pyrogram import Client

from app.core.cache import (
    cache_enabled,
    file_cache_path,
    hls_root,
    is_file_cached,
    link_or_copy,
    wait_for_cache,
)
from app.core.config import settings
from app.core.telethon_storage import get_message as tl_get_message, download_media as tl_download_media

logger = logging.getLogger(__name__)

HLS_ROOT = hls_root() if getattr(settings, "CACHE_HLS", True) else Path("app/static/hls")
HLS_ROOT.mkdir(parents=True, exist_ok=True)

SEGMENT_TIME = 6
_hls_tasks: dict[str, asyncio.Task] = {}
_hls_lock = asyncio.Lock()


def hls_dir(item_id: str) -> Path:
    return HLS_ROOT / str(item_id)


def playlist_path(item_id: str) -> Path:
    return hls_dir(item_id) / "index.m3u8"

def master_playlist_path(item_id: str) -> Path:
    return hls_dir(item_id) / "master.m3u8"


def hls_url_for(item_id: str) -> str:
    if master_playlist_path(item_id).exists():
        return f"/hls/{item_id}/master.m3u8"
    return f"/hls/{item_id}/index.m3u8"


def is_hls_ready(item_id: str) -> bool:
    return playlist_path(item_id).exists() or master_playlist_path(item_id).exists()


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _is_video(name: str, mime_type: Optional[str]) -> bool:
    lower = (name or "").lower()
    if mime_type and mime_type.startswith("video"):
        return True
    return lower.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg"))


async def ensure_hls(item, chat_id: str, user_session_string: Optional[str] = None) -> None:
    if not item or not item.parts:
        return
    if not _is_video(item.name, item.mime_type):
        return
    if not _ffmpeg_available():
        logger.warning("ffmpeg not available; HLS disabled.")
        return

    item_id = str(item.id)
    if is_hls_ready(item_id):
        return

    async with _hls_lock:
        existing = _hls_tasks.get(item_id)
        if existing and not existing.done():
            return
        _hls_tasks[item_id] = asyncio.create_task(
            _build_hls(item, chat_id, user_session_string)
        )


async def _download_source(item, chat_id: str, user_session_string: Optional[str], dest_path: Path) -> None:
    item_id = str(getattr(item, "id"))
    size = int(getattr(item, "size", 0) or 0)
    if cache_enabled() and getattr(settings, "CACHE_HLS", True):
        cache_path = file_cache_path(item_id)
        if is_file_cached(item_id, size):
            link_or_copy(cache_path, dest_path)
            return
        await wait_for_cache(item_id, timeout=30)
        if is_file_cached(item_id, size):
            link_or_copy(cache_path, dest_path)
            return

    msg_id = item.parts[0].message_id
    if chat_id == "me":
        if not user_session_string:
            raise RuntimeError("Missing user session for Saved Messages download.")
        client = Client(
            name=f"hls_dl_{item.id}",
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            session_string=user_session_string,
            in_memory=True
        )
        await client.connect()
        try:
            msg = await client.get_messages("me", msg_id)
            await client.download_media(msg, file_name=str(dest_path))
        finally:
            await client.disconnect()
    else:
        msg = await tl_get_message(msg_id)
        await tl_download_media(msg, str(dest_path))

    if cache_enabled() and getattr(settings, "CACHE_HLS", True):
        cache_path = file_cache_path(item_id)
        if not is_file_cached(item_id, size):
            try:
                link_or_copy(dest_path, cache_path)
            except Exception:
                pass


async def _run_ffmpeg(cmd: list[str]) -> subprocess.CompletedProcess:
    return await asyncio.to_thread(
        subprocess.run,
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )


async def _has_audio(source_path: Path) -> bool:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=index",
        "-of", "csv=p=0",
        str(source_path)
    ]
    result = await _run_ffmpeg(cmd)
    return bool((result.stdout or "").strip())


async def _build_hls(item, chat_id: str, user_session_string: Optional[str]) -> None:
    try:
        folder = hls_dir(str(item.id))
        folder.mkdir(parents=True, exist_ok=True)
        source_path = folder / "source"
        playlist = folder / "index.m3u8"
        master_playlist = folder / "master.m3u8"

        if not source_path.exists():
            await _download_source(item, chat_id, user_session_string, source_path)

        if playlist.exists() or master_playlist.exists():
            return

        # Multi-bitrate HLS (creates quality options)
        try:
            (folder / "v0").mkdir(exist_ok=True)
            (folder / "v1").mkdir(exist_ok=True)
            (folder / "v2").mkdir(exist_ok=True)

            has_audio = await _has_audio(source_path)
            # Map 3 video renditions (1080p/720p/480p) with optional audio
            filter_complex = (
                "[0:v]split=3[v1][v2][v3];"
                "[v1]scale=w=1920:h=1080:force_original_aspect_ratio=decrease[v1out];"
                "[v2]scale=w=1280:h=720:force_original_aspect_ratio=decrease[v2out];"
                "[v3]scale=w=854:h=480:force_original_aspect_ratio=decrease[v3out]"
            )
            cmd = [
                "ffmpeg", "-y",
                "-i", str(source_path),
                "-filter_complex", filter_complex,
                "-map", "[v1out]",
                "-map", "[v2out]",
                "-map", "[v3out]",
            ]
            if has_audio:
                # Duplicate audio stream for each rendition
                cmd += ["-map", "0:a:0?", "-map", "0:a:0?", "-map", "0:a:0?"]

            cmd += [
                "-c:v:0", "libx264", "-preset", "veryfast", "-b:v:0", "4500k", "-maxrate:v:0", "5000k", "-bufsize:v:0", "10000k",
                "-c:v:1", "libx264", "-preset", "veryfast", "-b:v:1", "2500k", "-maxrate:v:1", "3000k", "-bufsize:v:1", "6000k",
                "-c:v:2", "libx264", "-preset", "veryfast", "-b:v:2", "1200k", "-maxrate:v:2", "1500k", "-bufsize:v:2", "3000k",
            ]
            if has_audio:
                cmd += [
                    "-c:a", "aac", "-b:a", "128k"
                ]
                var_map = "v:0,a:0 v:1,a:1 v:2,a:2"
            else:
                var_map = "v:0 v:1 v:2"

            cmd += [
                "-f", "hls",
                "-hls_time", str(SEGMENT_TIME),
                "-hls_list_size", "0",
                "-hls_flags", "independent_segments",
                "-hls_segment_filename", str(folder / "v%v/seg_%05d.ts"),
                "-master_pl_name", "master.m3u8",
                "-var_stream_map", var_map,
                str(folder / "v%v/index.m3u8")
            ]
            result = await _run_ffmpeg(cmd)
            if result.returncode == 0 and master_playlist.exists():
                return
            logger.warning(f"HLS multi-variant failed for {item.name}: {result.stderr[:300]}")
        except Exception as e:
            logger.warning(f"HLS multi-variant error for {item.name}: {e}")

        segment_pattern = str(folder / "seg_%05d.ts")
        cmd = [
            "ffmpeg", "-y",
            "-i", str(source_path),
            "-c", "copy",
            "-f", "hls",
            "-hls_time", str(SEGMENT_TIME),
            "-hls_list_size", "0",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", segment_pattern,
            str(playlist)
        ]

        result = await _run_ffmpeg(cmd)
        if result.returncode != 0:
            logger.warning(f"HLS copy failed for {item.name}: {result.stderr[:300]}")
            # Fallback to transcode (CPU heavy but more compatible)
            cmd = [
                "ffmpeg", "-y",
                "-i", str(source_path),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-crf", "23",
                "-c:a", "aac",
                "-f", "hls",
                "-hls_time", str(SEGMENT_TIME),
                "-hls_list_size", "0",
                "-hls_flags", "independent_segments",
                "-hls_segment_filename", segment_pattern,
                str(playlist)
            ]
            result = await _run_ffmpeg(cmd)
            if result.returncode != 0:
                logger.error(f"HLS transcode failed for {item.name}: {result.stderr[:300]}")
    except Exception as e:
        logger.error(f"HLS build error for {getattr(item, 'name', 'item')}: {e}")
