import logging
import os
import tempfile
import urllib.request
import urllib.parse
import json
import asyncio
from itertools import cycle
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler
from app.core.config import settings
from app.db.models import FileSystemItem, FilePart, User, SharedCollection
from beanie import PydanticObjectId
from beanie.operators import In
from app.core.telethon_storage import check_storage_access as tl_check_storage, get_message as tl_get_message, forward_message_to as tl_forward_to_user, send_file as tl_send_file, send_text as tl_send_text

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Pyrogram Clients
user_client = None
bot_client = None
_storage_chat_id_override: int | None = None
_storage_access_notified = False
_bot_handler_clients: set[str] = set()
_bot_api_task: asyncio.Task | None = None

if settings.SESSION_STRING:
    user_client = Client(
        "morganxmystic_user",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        session_string=settings.SESSION_STRING
    )
    tg_client = user_client
    if settings.BOT_TOKEN:
        bot_client = Client(
            "morganxmystic_bot",
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            bot_token=settings.BOT_TOKEN
        )
else:
    tg_client = Client(
        "morganxmystic_bot",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        bot_token=settings.BOT_TOKEN
    )
    bot_client = tg_client

# Optional bot pool for parallel streaming/download
bot_pool: list[Client] = []
_bot_cycle = None

def _get_pool_tokens() -> list[str]:
    raw = getattr(settings, "BOT_POOL_TOKENS", "") or ""
    return [t.strip() for t in raw.split(",") if t.strip()]

def get_pool_client() -> Client | None:
    global _bot_cycle
    if not bot_pool:
        return None
    if _bot_cycle is None:
        _bot_cycle = cycle(bot_pool)
    return next(_bot_cycle)

def get_storage_client() -> Client:
    """Choose a client that can access the storage channel."""
    return get_pool_client() or bot_client or tg_client

def normalize_chat_id(chat_id: int | str) -> int | str:
    if isinstance(chat_id, str):
        raw = chat_id.strip()
        if raw == "me":
            return raw
        # Numeric ids should stay numeric for Pyrogram
        if raw.lstrip("-").isdigit():
            try:
                return int(raw)
            except Exception:
                return raw
        if raw.startswith("@") or raw.startswith("https://"):
            return raw
        return f"@{raw}"
    return chat_id


def _cast_ids(raw_ids: list[str]) -> list:
    casted = []
    for value in raw_ids or []:
        try:
            casted.append(PydanticObjectId(str(value)))
        except Exception:
            pass
    return casted or (raw_ids or [])


def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi"))


async def _collect_folder_files(folder_id: str) -> list[FileSystemItem]:
    items: list[FileSystemItem] = []
    children = await FileSystemItem.find(FileSystemItem.parent_id == str(folder_id)).to_list()
    for child in children:
        if child.is_folder:
            items.extend(await _collect_folder_files(str(child.id)))
        else:
            items.append(child)
    return items


async def _resolve_shared_items(token: str) -> list[FileSystemItem]:
    folder = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == True)
    if folder:
        items = await _collect_folder_files(str(folder.id))
        return items

    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if collection:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(collection.item_ids))).to_list()
        if any(item.is_folder for item in items):
            expanded: list[FileSystemItem] = []
            for item in items:
                if item.is_folder:
                    expanded.extend(await _collect_folder_files(str(item.id)))
                else:
                    expanded.append(item)
            items = expanded
        # Deduplicate while preserving order
        seen = set()
        unique_items: list[FileSystemItem] = []
        for item in items:
            key = str(item.id)
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(item)
        items = unique_items
        return items

    item = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == False)
    if item:
        return [item]

    return []

def get_storage_chat_id() -> int | str:
    global _storage_chat_id_override
    if _storage_chat_id_override:
        return _storage_chat_id_override
    if getattr(settings, "STORAGE_CHANNEL_USERNAME", ""):
        return settings.STORAGE_CHANNEL_USERNAME
    return settings.STORAGE_CHANNEL_ID or "me"

async def ensure_peer_access(client: Client, chat_id: int | str) -> bool:
    """Ensure the client has access to the given chat id."""
    chat_id = normalize_chat_id(chat_id)
    if chat_id == "me":
        return True
    try:
        await client.get_chat(chat_id)
        return True
    except Exception as e:
        logger.error(f"Peer access check failed for {chat_id}: {e}")
        return False

async def verify_storage_access_v2(client: Client):
    """Check storage channel access using Telethon first, then log Pyrogram status if needed."""
    global _storage_access_notified
    try:
        if await tl_check_storage():
            logger.info("Telethon storage check: OK")
            if not _storage_access_notified:
                try:
                    await tl_send_text("MorganXMystic: bot can access the storage channel.")
                    _storage_access_notified = True
                except Exception as notify_err:
                    logger.warning(f"Storage notify failed: {notify_err}")
            return
        logger.error("Telethon storage check failed.")
    except Exception as tele_err:
        logger.error(f"Telethon storage check error: {tele_err}")

    # Optional Pyrogram check (non-fatal)
    try:
        chat_id = normalize_chat_id(get_storage_chat_id())
        if chat_id == "me":
            logger.info("STORAGE_CHANNEL_ID/USERNAME not set. Using Saved Messages (me).")
            return
        chat = await client.get_chat(chat_id)
        logger.info(f"Storage channel reachable (Pyrogram): {getattr(chat, 'title', '') or chat.id}")
    except Exception as e:
        logger.error(f"Pyrogram storage check failed: {e}")

async def _try_join_storage(client: Client, chat_id: int | str) -> bool:
    invite = getattr(settings, "STORAGE_CHANNEL_INVITE", "")
    if not invite:
        return False
    if getattr(client, "_is_bot", False):
        return False
    try:
        await client.join_chat(invite)
    except Exception:
        try:
            await client.get_chat(invite)
        except Exception:
            return False
    return await ensure_peer_access(client, chat_id)

async def resolve_storage_chat_id(client: Client):
    global _storage_chat_id_override
    if _storage_chat_id_override:
        return
    invite = getattr(settings, "STORAGE_CHANNEL_INVITE", "")
    if not invite:
        return
    if getattr(client, "_is_bot", False):
        return
    try:
        chat = await client.join_chat(invite)
    except Exception:
        try:
            chat = await client.get_chat(invite)
        except Exception as e:
            logger.error(f"Storage invite resolve failed: {e}")
            return
    if chat and getattr(chat, "id", None):
        _storage_chat_id_override = chat.id
        logger.info(f"Resolved storage channel id via invite: {_storage_chat_id_override}")

async def ensure_bot_member(user: Client):
    if not bot_client:
        return
    bot_username = getattr(settings, "BOT_USERNAME", "") or ""
    if not bot_username:
        return
    chat_id = normalize_chat_id(get_storage_chat_id())
    if chat_id == "me":
        return
    try:
        member = await user.get_chat_member(chat_id, bot_username)
        if member:
            return
    except Exception:
        pass
    try:
        await user.add_chat_members(chat_id, bot_username)
        logger.info("Added bot to storage channel via user session.")
    except Exception as e:
        logger.error(f"Failed to add bot to storage channel: {e}")

async def pick_storage_client(chat_id: int | str) -> Client:
    candidates = []
    if bot_pool:
        candidates.extend(bot_pool)
    if bot_client:
        candidates.append(bot_client)
    if user_client:
        candidates.append(user_client)
    if tg_client not in candidates:
        candidates.append(tg_client)

    for client in candidates:
        if await ensure_peer_access(client, chat_id):
            return client
        if await _try_join_storage(client, chat_id):
            return client
    # Retry after resolving via invite link (user session only)
    if user_client:
        await resolve_storage_chat_id(user_client)
        new_chat_id = normalize_chat_id(get_storage_chat_id())
        if new_chat_id != chat_id:
            for client in candidates:
                if await ensure_peer_access(client, new_chat_id):
                    return client
                if await _try_join_storage(client, new_chat_id):
                    return client

    raise Exception("Storage channel not accessible for any client. Check channel membership or invite.")

async def verify_storage_access(client: Client):
    """Check if the client can access and post to the storage channel."""
    chat_id = get_storage_chat_id()
    if chat_id == "me":
        logger.info("STORAGE_CHANNEL_ID/USERNAME not set. Using Saved Messages (me).")
        return
    try:
        chat = await client.get_chat(chat_id)
        logger.info(f"Storage channel reachable: {getattr(chat, 'title', '') or chat.id}")
        try:
            test_msg = await client.send_message(chat_id, "MorganXMystic storage check ✅")
            await client.delete_messages(chat_id, test_msg.id)
            logger.info("Storage channel post check: OK")
        except Exception as post_err:
            logger.error(f"Storage channel post check failed: {post_err}")
    except Exception as e:
        logger.error(f"Storage channel access failed: {e}")

async def handle_private_upload(client: Client, message):
    """Forward user files sent to bot into storage channel and create DB items."""
    try:
        logger.info(
            "Bot upload received: chat_id=%s msg_id=%s from_user=%s",
            getattr(message.chat, "id", None),
            getattr(message, "id", None),
            getattr(message.from_user, "id", None) if message.from_user else None
        )
        if not (message.document or message.video or message.audio or message.photo):
            return

        storage_target = get_storage_chat_id()
        storage_chat_id = normalize_chat_id(storage_target)
        if not storage_chat_id or storage_chat_id == "me":
            await client.send_message(
                message.chat.id,
                "Storage channel not configured. Please set STORAGE_CHANNEL_ID/USERNAME and try again."
            )
            return

        # Quick ack so user knows the bot received the file
        try:
            await client.send_message(message.chat.id, "Got it! Uploading to storage…")
        except Exception:
            pass

        if not await ensure_peer_access(client, storage_chat_id):
            logger.warning("Storage channel not reachable by Pyrogram client; will still try Telethon upload.")

        owner = None
        owner_phone = ""
        if message.from_user:
            owner = await User.find_one(User.telegram_user_id == message.from_user.id)
        if owner:
            owner_phone = owner.phone_number
        else:
            # Fallback: attach to admin if user mapping not found
            admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
            owner_phone = admin_phone
        if not owner_phone:
            logger.warning("Telegram upload ignored: no matching user found.")
            try:
                await client.send_message(
                    message.chat.id,
                    "Please login on the website first to link your Telegram, then send the file again."
                )
            except Exception:
                pass
            return

        forwarded = None
        # Prefer a native copy to storage channel (fast, no download)
        try:
            forwarded = await client.copy_message(storage_chat_id, message.chat.id, message.id)
        except Exception as copy_err:
            logger.warning(f"Copy to storage failed, falling back to upload: {copy_err}")
            # Download via Pyrogram and re-upload via Telethon to storage channel
            original_name = None
            if message.document:
                original_name = message.document.file_name
            elif message.video:
                original_name = message.video.file_name
            elif message.audio:
                original_name = message.audio.file_name
            if not original_name:
                original_name = "file"

            fd, tmp_path = tempfile.mkstemp()
            os.close(fd)
            try:
                await client.download_media(message, file_name=tmp_path)
                forwarded = await tl_send_file(
                    tmp_path,
                    file_name=original_name,
                    caption="Uploaded via MorganXMystic"
                )
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        if not forwarded:
            await client.send_message(
                message.chat.id,
                "Upload failed: storage channel is not reachable by the bot."
            )
            return

        if hasattr(forwarded, "document") or hasattr(forwarded, "video") or hasattr(forwarded, "audio") or hasattr(forwarded, "photo"):
            # Pyrogram Message
            if forwarded.document:
                file_id = forwarded.document.file_id
                size = forwarded.document.file_size
                mime_type = forwarded.document.mime_type or "application/octet-stream"
                name = forwarded.document.file_name or "file"
            elif forwarded.video:
                file_id = forwarded.video.file_id
                size = forwarded.video.file_size
                mime_type = forwarded.video.mime_type or "video/mp4"
                name = forwarded.video.file_name or "video"
            elif forwarded.audio:
                file_id = forwarded.audio.file_id
                size = forwarded.audio.file_size
                mime_type = forwarded.audio.mime_type or "audio/mpeg"
                name = forwarded.audio.file_name or "audio"
            elif forwarded.photo:
                file_id = forwarded.photo.file_id
                size = 0
                mime_type = "image/jpeg"
                name = "photo.jpg"
            else:
                return
        else:
            # Telethon Message
            file_id = str(forwarded.id)
            size = getattr(forwarded.file, "size", 0)
            mime_type = getattr(forwarded.file, "mime_type", None) or "application/octet-stream"
            name = getattr(forwarded.file, "name", None) or "file"

        # Ensure Bot Uploads folder exists for this user (rename legacy if needed)
        folder = await FileSystemItem.find_one(
            FileSystemItem.owner_phone == owner_phone,
            FileSystemItem.parent_id == None,
            FileSystemItem.is_folder == True,
            FileSystemItem.name == "Bot Uploads"
        )
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Uploads"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Shared"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            folder = FileSystemItem(
                name="Bot Uploads",
                is_folder=True,
                parent_id=None,
                owner_phone=owner_phone,
                source="bot"
            )
            await folder.insert()

        new_file = FileSystemItem(
            name=name,
            is_folder=False,
            parent_id=str(folder.id),
            owner_phone=owner_phone,
            size=size,
            mime_type=mime_type,
            source="bot",
            parts=[FilePart(
                telegram_file_id=file_id,
                message_id=forwarded.id,
                chat_id=storage_chat_id,
                part_number=1,
                size=size
            )]
        )
        await new_file.insert()
        logger.info(f"Bot-ingested file for {owner_phone}: {name}")
        try:
            await client.send_message(message.chat.id, "File added to Bot Uploads folder.")
        except Exception:
            pass
    except Exception as e:
        logger.exception(f"Bot ingestion failed: {e}")
        try:
            await client.send_message(message.chat.id, f"Upload failed: {e}")
        except Exception:
            pass

async def handle_start_command(client: Client, message):
    """Handle deep links for shared files."""
    try:
        if not message.text:
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await client.send_message(
                message.chat.id,
                "Send me a file and I’ll add it to your Bot Uploads folder."
            )
            return
        payload = parts[1]
        if payload.startswith("share_"):
            token = payload.replace("share_", "", 1)
            items = await _resolve_shared_items(token)
            if not items:
                await client.send_message(message.chat.id, "File not found or expired.")
                return

            items = [i for i in items if _is_video_item(i)]
            if not items:
                await client.send_message(message.chat.id, "No video files found.")
                return

            unavailable = 0
            sent = 0
            for item in items:
                if not item.parts:
                    continue
                chat_id = (item.parts[0].chat_id if item.parts else None) or get_storage_chat_id() or "me"
                chat_id = normalize_chat_id(chat_id)
                if chat_id == "me":
                    unavailable += 1
                    continue
                msg = await tl_get_message(item.parts[0].message_id)
                await tl_forward_to_user(message.chat.id, msg)
                sent += 1
                await asyncio.sleep(0.35)

            if sent:
                await client.send_message(message.chat.id, f"Sent {sent} file(s).")
            if unavailable:
                await client.send_message(message.chat.id, "Some files were not available from storage.")
    except Exception as e:
        logger.error(f"Start command failed: {e}")


async def handle_bot_message(client: Client, message):
    """Single entrypoint for bot messages to avoid filter mismatches."""
    try:
        if message.text and message.text.strip().startswith("/start"):
            await handle_start_command(client, message)
            return
        if message.document or message.video or message.audio or message.photo:
            await handle_private_upload(client, message)
            return
    except Exception as e:
        logger.exception(f"Bot message handler failed: {e}")
        try:
            await client.send_message(message.chat.id, f"Upload failed: {e}")
        except Exception:
            pass

def _register_bot_handlers(client: Client):
    client_key = getattr(client, "name", None) or str(id(client))
    if client_key in _bot_handler_clients:
        return
    try:
        # Single handler for all incoming messages (simpler, more reliable)
        client.add_handler(MessageHandler(handle_bot_message, filters.incoming))
        _bot_handler_clients.add(client_key)
        logger.info("Bot handlers registered.")
    except Exception as e:
        logger.error(f"Failed to register bot handlers: {e}")


def _clear_bot_webhook_http(token: str) -> None:
    if not token:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=true"
        with urllib.request.urlopen(url, timeout=10) as resp:
            _ = resp.read()
        logger.info("Cleared bot webhook via HTTP.")
    except Exception as e:
        logger.warning(f"Failed to clear bot webhook via HTTP: {e}")


async def _bot_api_call(token: str, method: str, params: dict | None = None) -> dict:
    params = params or {}
    def _do():
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/{method}",
            data=data
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload)
    return await asyncio.to_thread(_do)


async def _handle_bot_api_message(message: dict):
    try:
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        msg_id = message.get("message_id")
        text = (message.get("text") or "").strip()

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                payload = parts[1].strip()
                if payload.startswith("share_"):
                    token = payload.replace("share_", "", 1)
                    items = await _resolve_shared_items(token)
                    if not items:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "File not found or expired."}
                        )
                        return

                    items = [i for i in items if _is_video_item(i)]
                    if not items:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "No video files found."}
                        )
                        return

                    sent = 0
                    unavailable = 0
                    for item in items:
                        if not item.parts:
                            continue
                        part = item.parts[0]
                        source_chat = part.chat_id or normalize_chat_id(get_storage_chat_id())
                        if not source_chat or source_chat == "me":
                            unavailable += 1
                            continue
                        copy_resp = await _bot_api_call(
                            settings.BOT_TOKEN,
                            "copyMessage",
                            {
                                "chat_id": chat_id,
                                "from_chat_id": source_chat,
                                "message_id": part.message_id
                            }
                        )
                        if not copy_resp.get("ok"):
                            await _bot_api_call(
                                settings.BOT_TOKEN,
                                "sendMessage",
                                {"chat_id": chat_id, "text": f"Failed to send file: {copy_resp.get('description', 'unknown error')}"}
                            )
                            continue
                        sent += 1
                        await asyncio.sleep(0.35)

                    if sent:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": f"Sent {sent} file(s)."}
                        )
                    if unavailable:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "Some files were not available from storage."}
                        )
                    return

            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Send me a file and I’ll add it to your Bot Uploads folder."}
            )
            return

        # Only handle media uploads
        media = None
        media_type = None
        if message.get("document"):
            media = message["document"]
            media_type = "document"
        elif message.get("video"):
            media = message["video"]
            media_type = "video"
        elif message.get("audio"):
            media = message["audio"]
            media_type = "audio"
        elif message.get("photo"):
            media = message["photo"][-1]
            media_type = "photo"
        else:
            return

        storage_chat_id = normalize_chat_id(get_storage_chat_id())
        if not storage_chat_id or storage_chat_id == "me":
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Storage channel not configured. Please set STORAGE_CHANNEL_ID/USERNAME."}
            )
            return

        await _bot_api_call(
            settings.BOT_TOKEN,
            "sendMessage",
            {"chat_id": chat_id, "text": "Got it! Uploading to storage…"}
        )

        copy_resp = await _bot_api_call(
            settings.BOT_TOKEN,
            "copyMessage",
            {"chat_id": storage_chat_id, "from_chat_id": chat_id, "message_id": msg_id}
        )
        if not copy_resp.get("ok"):
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": f"Upload failed: {copy_resp.get('description', 'unknown error')}"}
            )
            return

        forwarded_msg_id = copy_resp["result"]["message_id"]

        owner = None
        from_user = message.get("from") or {}
        if from_user.get("id"):
            owner = await User.find_one(User.telegram_user_id == from_user.get("id"))
        owner_phone = owner.phone_number if owner else (getattr(settings, "ADMIN_PHONE", "") or "")
        if not owner_phone:
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Please login on the website first so I can link your account."}
            )
            return

        # Ensure Bot Uploads folder exists for this user
        folder = await FileSystemItem.find_one(
            FileSystemItem.owner_phone == owner_phone,
            FileSystemItem.parent_id == None,
            FileSystemItem.is_folder == True,
            FileSystemItem.name == "Bot Uploads"
        )
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Uploads"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            folder = FileSystemItem(
                name="Bot Uploads",
                is_folder=True,
                parent_id=None,
                owner_phone=owner_phone,
                source="bot"
            )
            await folder.insert()

        file_id = media.get("file_id", "")
        file_name = media.get("file_name") or ("photo.jpg" if media_type == "photo" else "file")
        file_size = media.get("file_size", 0) or 0
        mime_type = media.get("mime_type") or ("image/jpeg" if media_type == "photo" else "application/octet-stream")

        new_file = FileSystemItem(
            name=file_name,
            is_folder=False,
            parent_id=str(folder.id),
            owner_phone=owner_phone,
            size=file_size,
            mime_type=mime_type,
            source="bot",
            parts=[FilePart(
                telegram_file_id=file_id or str(forwarded_msg_id),
                message_id=forwarded_msg_id,
                chat_id=storage_chat_id,
                part_number=1,
                size=file_size
            )]
        )
        await new_file.insert()

        await _bot_api_call(
            settings.BOT_TOKEN,
            "sendMessage",
            {"chat_id": chat_id, "text": "File added to Bot Uploads folder."}
        )
    except Exception as e:
        logger.exception(f"Bot API handler failed: {e}")
        try:
            chat_id = (message.get("chat") or {}).get("id")
            if chat_id:
                await _bot_api_call(
                    settings.BOT_TOKEN,
                    "sendMessage",
                    {"chat_id": chat_id, "text": f"Upload failed: {e}"}
                )
        except Exception:
            pass


async def _bot_api_poll_loop():
    if not settings.BOT_TOKEN:
        return
    offset = 0
    while True:
        try:
            resp = await _bot_api_call(
                settings.BOT_TOKEN,
                "getUpdates",
                {"timeout": 25, "offset": offset}
            )
            if not resp.get("ok"):
                await asyncio.sleep(2)
                continue
            for upd in resp.get("result", []):
                offset = upd.get("update_id", offset) + 1
                msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post")
                if msg:
                    await _handle_bot_api_message(msg)
        except Exception as e:
            logger.warning(f"Bot API polling error: {e}")
            await asyncio.sleep(2)


async def start_telegram():
    logger.info("Connecting to Telegram...")
    await tg_client.start()
    me = await tg_client.get_me()
    tg_client._is_bot = getattr(me, "is_bot", False)
    logger.info(f"Connected as {me.first_name} (@{me.username})")
    if getattr(tg_client, "_is_bot", False):
        try:
            await tg_client.delete_webhook(drop_pending_updates=True)
            logger.info("Cleared bot webhook for long polling (tg_client).")
        except Exception as e:
            logger.warning(f"Failed to clear bot webhook (tg_client): {e}")
            _clear_bot_webhook_http(settings.BOT_TOKEN)
    await resolve_storage_chat_id(tg_client)
    if getattr(tg_client, "_is_bot", False):
        _register_bot_handlers(tg_client)
    if user_client and tg_client is user_client:
        await ensure_bot_member(user_client)
    await verify_storage_access_v2(tg_client)

    if bot_client and bot_client is not tg_client:
        try:
            await bot_client.start()
            bot_me = await bot_client.get_me()
            bot_client._is_bot = getattr(bot_me, "is_bot", False)
            logger.info(f"Bot client connected as {bot_me.first_name} (@{bot_me.username})")
            try:
                await bot_client.delete_webhook(drop_pending_updates=True)
                logger.info("Cleared bot webhook for long polling (bot_client).")
            except Exception as e:
                logger.warning(f"Failed to clear bot webhook (bot_client): {e}")
                _clear_bot_webhook_http(settings.BOT_TOKEN)
            await verify_storage_access_v2(bot_client)
            _register_bot_handlers(bot_client)
        except FloodWait as e:
            logger.warning(f"Bot client flood-wait {e.value}s on start; skipping bot client this run.")
            bot_client = None
        except Exception as e:
            logger.warning(f"Bot client start failed: {e}")
            bot_client = None

    # Start bot pool (if any)
    tokens = _get_pool_tokens()
    for idx, token in enumerate(tokens):
        try:
            bot = Client(f"morganxmystic_pool_{idx}", api_id=settings.API_ID, api_hash=settings.API_HASH, bot_token=token)
            await bot.start()
            bot_me = await bot.get_me()
            bot._is_bot = getattr(bot_me, "is_bot", False)
            bot_pool.append(bot)
            logger.info(f"Started bot pool #{idx}")
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"Cleared bot webhook for pool #{idx}.")
            except Exception as e:
                logger.warning(f"Failed to clear webhook for pool #{idx}: {e}")
                _clear_bot_webhook_http(token)
            await verify_storage_access_v2(bot)
            _register_bot_handlers(bot)
        except FloodWait as e:
            logger.warning(f"Pool bot #{idx} flood-wait {e.value}s; skipping this bot.")
        except Exception as e:
            logger.error(f"Failed to start bot pool #{idx}: {e}")

    # Start Bot API polling fallback (more reliable for updates)
    global _bot_api_task
    if _bot_api_task is None:
        enable_polling = os.getenv("BOT_API_POLLING", "").lower() in ("1", "true", "yes")
        if enable_polling:
            _bot_api_task = asyncio.create_task(_bot_api_poll_loop())

async def stop_telegram():
    logger.info("Stopping Telegram Client...")
    global _bot_api_task
    if _bot_api_task:
        _bot_api_task.cancel()
        try:
            await _bot_api_task
        except Exception:
            pass
        _bot_api_task = None

    for bot in bot_pool:
        try:
            await bot.stop()
        except Exception:
            pass
    if bot_client and bot_client is not tg_client:
        try:
            await bot_client.stop()
        except Exception:
            pass
    try:
        await tg_client.stop()
    except Exception as e:
        logger.warning(f"tg_client stop failed: {e}")
