import math
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException, Header, Body
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client
from app.db.models import FileSystemItem, User, PlaybackProgress
from app.core.config import settings
from app.core.telegram_bot import tg_client, get_pool_client, ensure_peer_access, get_storage_client, get_storage_chat_id, pick_storage_client, normalize_chat_id
from app.core.cache import cache_enabled, file_cache_path, is_file_cached, iter_file_range, touch_path, schedule_cache_warm
from app.core.telethon_storage import get_message as tl_get_message, iter_download as tl_iter_download
from app.core.hls import ensure_hls, is_hls_ready, hls_url_for

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

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
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

def _can_access(user: User, item: FileSystemItem, is_admin: bool) -> bool:
    if is_admin: return True
    return item.owner_phone == user.phone_number or user.phone_number in (item.collaborators or [])

def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg"))

async def telegram_stream_generator(
    client: Client,
    chat_id: int | str,
    message_id: int,
    offset: int,
    limit: int | None = None
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

        async for chunk in client.stream_media(file_id, offset=offset, limit=limit or 0):
            if not chunk:
                break
            yield chunk
    except Exception as e:
        print(f"Stream Error: {e}")
        return

@router.get("/player/{item_id}", response_class=HTMLResponse)
async def player_page(request: Request, item_id: str):
    user = await get_current_user(request)
    
    # If not logged in, redirect to login page
    if not user: 
        return templates.TemplateResponse("login.html", {"request": request, "step": "phone"})

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

    if range:
        try:
            range_val = range.replace("bytes=", "")
            parts = range_val.split("-")
            if parts[0]:
                start = int(parts[0])
            if len(parts) > 1 and parts[1]:
                end = int(parts[1])
            else:
                end = file_size - 1 if file_size else 0
            if file_size and end >= file_size:
                end = file_size - 1
        except ValueError:
            start = 0
            end = file_size - 1 if file_size else 0

    if cache_enabled():
        cache_path = file_cache_path(str(item.id))
        if is_file_cached(str(item.id), file_size):
            touch_path(cache_path)
            headers = {
                'Accept-Ranges': 'bytes',
                'Content-Type': item.mime_type or "application/octet-stream",
                'Content-Disposition': f'inline; filename=\"{item.name}\"'
            }
            if file_size:
                headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
                headers['Content-Length'] = str(max((end - start + 1), 0))
            return StreamingResponse(
                iter_file_range(cache_path, start, end),
                status_code=206 if range else 200,
                headers=headers,
                media_type=item.mime_type
            )
        schedule_cache_warm(
            item,
            chat_id,
            user.session_string if chat_id == "me" else None
        )

    use_ephemeral = False
    storage_client = None
    if chat_id == "me":
        client = Client("streamer", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=user.session_string, in_memory=True)
        await client.connect()
        use_ephemeral = True
    else:
        client = None
        if from_storage:
            chat_id = normalize_chat_id(get_storage_chat_id())
        try:
            storage_client = await pick_storage_client(chat_id)
        except Exception:
            storage_client = None

    async def cleanup_generator():
        try:
            msg_id = item.parts[0].message_id
            if chat_id == "me":
                sent = 0
                limit = (end - start + 1) if file_size else None
                async for chunk in telegram_stream_generator(client, chat_id, msg_id, start, limit):
                    if limit is not None:
                        remaining = limit - sent
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            yield chunk[:remaining]
                            break
                        sent += len(chunk)
                    yield chunk
            elif storage_client:
                sent = 0
                limit = (end - start + 1) if file_size else None
                async for chunk in telegram_stream_generator(storage_client, chat_id, msg_id, start, limit):
                    if limit is not None:
                        remaining = limit - sent
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            yield chunk[:remaining]
                            break
                        sent += len(chunk)
                    yield chunk
            else:
                msg = await tl_get_message(msg_id)
                limit = (end - start + 1) if file_size else None
                async for chunk in tl_iter_download(msg, offset=start, limit=limit):
                    yield chunk
        finally:
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
