import asyncio
import os
from typing import AsyncGenerator, Optional

from telethon import TelegramClient
from telethon.tl.types import Message

from app.core.config import settings

_client: TelegramClient | None = None
_storage_entity = None
_lock = asyncio.Lock()


def _normalize_target(target: int | str) -> int | str:
    if isinstance(target, str):
        raw = target.strip()
        if raw == "me":
            return raw
        if raw.lstrip("-").isdigit():
            try:
                return int(raw)
            except Exception:
                return raw
        if raw.startswith("@") or raw.startswith("https://"):
            return raw
        return f"@{raw}"
    return target


def _storage_target() -> int | str | None:
    if getattr(settings, "STORAGE_CHANNEL_USERNAME", ""):
        return settings.STORAGE_CHANNEL_USERNAME
    return settings.STORAGE_CHANNEL_ID


async def get_client() -> TelegramClient:
    global _client
    async with _lock:
        if _client is None:
            # Disable update handling to avoid stealing bot updates from Pyrogram
            _client = TelegramClient(
                "morganxmystic_telethon_bot",
                settings.API_ID,
                settings.API_HASH,
                receive_updates=False
            )
            await _client.start(bot_token=settings.BOT_TOKEN)
        elif not _client.is_connected():
            await _client.connect()
    return _client


async def stop_client() -> None:
    global _client
    if _client and _client.is_connected():
        await _client.disconnect()


async def _resolve_storage_entity():
    global _storage_entity
    if _storage_entity is not None:
        return _storage_entity

    target = _storage_target()
    client = await get_client()

    if target:
        try:
            _storage_entity = await client.get_entity(_normalize_target(target))
            return _storage_entity
        except Exception:
            pass

    title = (getattr(settings, "STORAGE_CHANNEL_TITLE", "") or "").strip().lower()
    if title:
        async for dialog in client.iter_dialogs():
            if dialog.is_channel and dialog.title and dialog.title.strip().lower() == title:
                _storage_entity = dialog.entity
                return _storage_entity

    raise RuntimeError("Storage channel not found. Set STORAGE_CHANNEL_ID/USERNAME or STORAGE_CHANNEL_TITLE.")


async def get_storage_entity():
    return await _resolve_storage_entity()


async def check_storage_access() -> bool:
    try:
        client = await get_client()
        entity = await get_storage_entity()
        test_msg = await client.send_message(entity, "MorganXMystic storage check OK")
        await client.delete_messages(entity, test_msg.id)
        return True
    except Exception:
        return False


async def send_text(message: str):
    client = await get_client()
    entity = await get_storage_entity()
    return await client.send_message(entity, message)


async def send_file(
    file: str | Message,
    file_name: Optional[str] = None,
    caption: Optional[str] = None,
    progress_cb=None
):
    client = await get_client()
    entity = await get_storage_entity()
    return await client.send_file(
        entity,
        file,
        caption=caption,
        force_document=True,
        progress_callback=progress_cb,
        file_name=file_name
    )


async def get_message(message_id: int) -> Message:
    client = await get_client()
    entity = await get_storage_entity()
    msg = await client.get_messages(entity, ids=message_id)
    if not msg:
        raise RuntimeError("Message not found in storage channel")
    return msg


async def delete_message(message_id: int) -> None:
    client = await get_client()
    entity = await get_storage_entity()
    await client.delete_messages(entity, message_id)


async def download_media(message: Message, dest_path: str) -> None:
    client = await get_client()
    await client.download_media(message, file=dest_path)


async def iter_download(
    message: Message,
    offset: int = 0,
    limit: Optional[int] = None,
    chunk_size: int = 4 * 1024 * 1024
) -> AsyncGenerator[bytes, None]:
    client = await get_client()
    async for chunk in client.iter_download(
        message,
        offset=offset,
        limit=limit,
        request_size=chunk_size
    ):
        yield chunk


async def forward_message_to(user_id: int, message: Message) -> None:
    client = await get_client()
    await client.forward_messages(user_id, message)


async def iter_storage_messages(limit: int | None = 200):
    client = await get_client()
    entity = await get_storage_entity()
    async for msg in client.iter_messages(entity, limit=limit):
        yield msg
