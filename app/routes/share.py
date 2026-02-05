import uuid
import re
from datetime import datetime
from typing import List
from fastapi import APIRouter, Request, HTTPException, Body, Header
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client
from beanie.operators import In, Or
from beanie import PydanticObjectId
from app.db.models import FileSystemItem, User, SharedCollection, PlaybackProgress
from app.core.config import settings
from app.routes.stream import telegram_stream_generator
from app.core.telegram_bot import tg_client, get_pool_client, get_storage_client, get_storage_chat_id, pick_storage_client, normalize_chat_id
from app.core.telethon_storage import get_message as tl_get_message, iter_download as tl_iter_download
from app.core.hls import ensure_hls, is_hls_ready, hls_url_for
from app.utils.file_utils import format_size, get_icon_for_mime
from app.routes.dashboard import get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def _natural_key(value: str):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", value or "")]

def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi"))

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

def _order_items(items: List[FileSystemItem], preferred_ids: List[str] | None = None) -> List[FileSystemItem]:
    if not items:
        return []
    if not preferred_ids:
        return sorted(items, key=lambda i: _natural_key(i.name))
    items_by_id = {str(i.id): i for i in items}
    ordered = [items_by_id[iid] for iid in preferred_ids if iid in items_by_id]
    remaining = [i for i in items if str(i.id) not in set(preferred_ids)]
    remaining_sorted = sorted(remaining, key=lambda i: _natural_key(i.name))
    return ordered + remaining_sorted

def _cast_ids(raw_ids: List[str]) -> List:
    casted = []
    for value in raw_ids or []:
        try:
            casted.append(PydanticObjectId(str(value)))
        except Exception:
            pass
    return casted or (raw_ids or [])

async def _collect_folder_files(folder_id: str) -> List[FileSystemItem]:
    items = []
    children = await FileSystemItem.find(FileSystemItem.parent_id == str(folder_id)).to_list()
    for child in children:
        if child.is_folder:
            items.extend(await _collect_folder_files(str(child.id)))
        else:
            items.append(child)
    return items

@router.post("/share/bundle")
async def create_bundle(request: Request, item_ids: List[str] = Body(...)):
    user = await get_current_user(request)
    if not user: return {"error": "Unauthorized"}
    if not item_ids:
        return {"error": "No items selected"}
    token = str(uuid.uuid4())
    items = await FileSystemItem.find(In(FileSystemItem.id, item_ids)).to_list()
    expanded_items: List[FileSystemItem] = []
    for item in items:
        if item.is_folder:
            expanded_items.extend(await _collect_folder_files(str(item.id)))
        else:
            expanded_items.append(item)

    # Deduplicate while preserving order
    seen = set()
    unique_items = []
    for item in expanded_items:
        if str(item.id) not in seen:
            unique_items.append(item)
            seen.add(str(item.id))

    if not unique_items:
        return {"error": "No files found in selected folders"}

    items_sorted = sorted(unique_items, key=lambda i: _natural_key(i.name))
    sorted_ids = [str(i.id) for i in items_sorted]
    bundle = SharedCollection(token=token, item_ids=sorted_ids, owner_phone=user.phone_number, name=f"Shared by {user.first_name or 'User'}")
    await bundle.insert()
    base_url = str(request.base_url).rstrip("/")
    return {"link": f"{base_url}/s/{token}"}

@router.get("/s/{token}")
async def public_view(request: Request, token: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    viewer_name = (request.query_params.get("u") or "").strip()
    episode_override = request.query_params.get("e")

    # Folder Share (stable link)
    folder = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == True)
    if folder:
        files = await _collect_folder_files(str(folder.id))
        if not files:
            raise HTTPException(404, "No items found")
        collection = await SharedCollection.find_one(SharedCollection.token == token)
        ordered_items = _order_items(files, collection.item_ids if collection else None)

        # Keep bundle in sync for admin reorder (and new episodes)
        if collection:
            collection.item_ids = [str(i.id) for i in ordered_items]
            collection.name = folder.name or collection.name
            await collection.save()
        else:
            collection = SharedCollection(
                token=token,
                item_ids=[str(i.id) for i in ordered_items],
                owner_phone=folder.owner_phone,
                name=folder.name or "Shared Folder"
            )
            await collection.insert()

        for item in ordered_items:
            if not item.share_token:
                item.share_token = str(uuid.uuid4())
                await item.save()
            item.formatted_size = format_size(item.size)
            item.icon = get_icon_for_mime(item.mime_type)
            item.is_video = _is_video_item(item)

        active_item = None
        if episode_override:
            active_item = next((i for i in ordered_items if str(i.id) == episode_override), None)
        if not active_item:
            active_item = next((i for i in ordered_items if _is_video_item(i)), ordered_items[0])

        active_hls = ""
        if active_item and active_item.is_video:
            storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
            if storage_chat_id != "me":
                await ensure_hls(active_item, storage_chat_id)
                if is_hls_ready(str(active_item.id)):
                    active_hls = hls_url_for(str(active_item.id))

        return templates.TemplateResponse("shared_folder.html", {
            "request": request,
            "episodes": ordered_items,
            "active_item": active_item,
            "stream_url": f"/s/stream/file/{active_item.id}",
            "hls_url": active_hls,
            "bundle_name": folder.name or (collection.name if collection else "Shared Folder"),
            "viewer_name": viewer_name,
            "token": token,
            "is_admin": is_admin,
            "user": user,
            "bot_username": getattr(settings, "BOT_USERNAME", "")
        })

    # Bundle Check
    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if collection:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(collection.item_ids))).to_list()
        if not items:
            # Fallback: try to recover legacy folder-based links
            fallback_folder = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == collection.owner_phone,
                FileSystemItem.name == collection.name,
                FileSystemItem.is_folder == True
            )
            if not fallback_folder:
                raise HTTPException(404, "No items found")

            if not fallback_folder.share_token:
                fallback_folder.share_token = token
                await fallback_folder.save()

            files = await _collect_folder_files(str(fallback_folder.id))
            if not files:
                raise HTTPException(404, "No items found")

            ordered_items = _order_items(files, None)
            collection.item_ids = [str(i.id) for i in ordered_items]
            collection.name = fallback_folder.name or collection.name
            await collection.save()
            items = files

        # If legacy bundles contain folders, expand them here
        if any(item.is_folder for item in items):
            expanded: List[FileSystemItem] = []
            for item in items:
                if item.is_folder:
                    expanded.extend(await _collect_folder_files(str(item.id)))
                else:
                    expanded.append(item)

            # Deduplicate while preserving order
            seen = set()
            unique_items = []
            for item in expanded:
                if str(item.id) not in seen:
                    unique_items.append(item)
                    seen.add(str(item.id))

            if not unique_items:
                raise HTTPException(404, "No items found")

            ordered_items = _order_items(unique_items, collection.item_ids)
        else:
            items_by_id = {str(i.id): i for i in items}
            ordered_items = [items_by_id[iid] for iid in collection.item_ids if iid in items_by_id]
            if not ordered_items:
                raise HTTPException(404, "No items found")

        # Ensure share tokens exist for Telegram deep links
        for item in ordered_items:
            if not item.share_token:
                item.share_token = str(uuid.uuid4())
                await item.save()
            item.formatted_size = format_size(item.size)
            item.icon = get_icon_for_mime(item.mime_type)
            item.is_video = _is_video_item(item)

        active_item = None
        if episode_override:
            active_item = next((i for i in ordered_items if str(i.id) == episode_override), None)
        if not active_item:
            active_item = next((i for i in ordered_items if _is_video_item(i)), ordered_items[0])

        active_hls = ""
        if active_item and active_item.is_video:
            storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
            if storage_chat_id != "me":
                await ensure_hls(active_item, storage_chat_id)
                if is_hls_ready(str(active_item.id)):
                    active_hls = hls_url_for(str(active_item.id))

        return templates.TemplateResponse("shared_folder.html", {
            "request": request,
            "episodes": ordered_items,
            "active_item": active_item,
            "stream_url": f"/s/stream/file/{active_item.id}",
            "hls_url": active_hls,
            "bundle_name": collection.name,
            "viewer_name": viewer_name,
            "token": token,
            "is_admin": is_admin,
            "user": user,
            "bot_username": getattr(settings, "BOT_USERNAME", "")
        })

    # Single File Check
    item = await FileSystemItem.find_one(FileSystemItem.share_token == token)
    if item:
        item.formatted_size = format_size(item.size)
        item.icon = get_icon_for_mime(item.mime_type)
        active_hls = ""
        if "video" in (item.mime_type or "") or (item.name or "").lower().endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg")):
            storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
            if storage_chat_id != "me":
                await ensure_hls(item, storage_chat_id)
                if is_hls_ready(str(item.id)):
                    active_hls = hls_url_for(str(item.id))
        return templates.TemplateResponse("shared.html", {
            "request": request,
            "item": item,
            "stream_url": f"/s/stream/{token}",
            "hls_url": active_hls,
            "viewer_name": viewer_name,
            "token": token,
            "bot_username": getattr(settings, "BOT_USERNAME", ""),
            "user": user,
            "is_admin": is_admin
        })

    raise HTTPException(404, "Link expired")

@router.get("/s/{token}/u={username}")
async def public_view_with_user(token: str, username: str):
    return RedirectResponse(url=f"/s/{token}?u={username}")

@router.get("/s/stream/file/{item_id}")
async def public_stream_by_id(item_id: str, request: Request, range: str = Header(None)):
    item = await FileSystemItem.get(item_id)
    if not item: raise HTTPException(404)
    if item.parts and item.parts[0].chat_id:
        chat_id = item.parts[0].chat_id
        from_storage = False
    else:
        chat_id = get_storage_chat_id() or "me"
        from_storage = True
    chat_id = normalize_chat_id(chat_id)
    use_ephemeral = False
    if chat_id == "me":
        owner = await User.find_one(User.phone_number == item.owner_phone)
        client = Client("pub_stream", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=owner.session_string, in_memory=True)
        await client.connect()
        use_ephemeral = True
    else:
        client = None
        if from_storage:
            chat_id = normalize_chat_id(get_storage_chat_id())
        try:
            client = await pick_storage_client(chat_id)
        except Exception:
            client = None

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
                end = file_size - 1
            if file_size and end >= file_size:
                end = file_size - 1
        except ValueError:
            start = 0
            end = file_size - 1 if file_size else 0

    async def cleanup():
        try:
            msg_id = item.parts[0].message_id
            if chat_id == "me":
                sent = 0
                limit = (end - start + 1) if file_size else None
                async for chunk in telegram_stream_generator(client, chat_id, msg_id, start):
                    if limit is not None:
                        remaining = limit - sent
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            yield chunk[:remaining]
                            break
                        sent += len(chunk)
                    yield chunk
            elif client:
                sent = 0
                limit = (end - start + 1) if file_size else None
                async for chunk in telegram_stream_generator(client, chat_id, msg_id, start):
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

    headers = {
        'Content-Disposition': f'inline; filename="{item.name}"',
        'Accept-Ranges': 'bytes',
    }
    if file_size:
        headers.update({
            'Content-Range': f'bytes {start}-{end}/{file_size}',
            'Content-Length': str(max((end - start + 1), 0)),
        })
    if item.mime_type:
        headers['Content-Type'] = item.mime_type

    return StreamingResponse(cleanup(), status_code=206 if range else 200, headers=headers, media_type=item.mime_type)

@router.get("/s/resolve_user")
async def resolve_user(name: str):
    raw = (name or "").strip()
    if not raw:
        return {"ok": False, "error": "Missing name"}
    pattern = re.compile(f"^{re.escape(raw)}$", re.I)
    user = await User.find_one(Or(User.phone_number == raw, User.requested_name == pattern, User.first_name == pattern))
    if user:
        resolved = user.requested_name or user.first_name or user.phone_number
        return {"ok": True, "resolved": resolved, "exists": True}
    return {"ok": True, "resolved": raw, "exists": False}

@router.get("/s/stream/{token}")
async def public_stream_token(request: Request, token: str, range: str = Header(None)):
    item = await FileSystemItem.find_one(FileSystemItem.share_token == token)
    if not item: raise HTTPException(404)
    return await public_stream_by_id(str(item.id), request, range)

@router.post("/s/hls/prepare/{item_id}")
async def prepare_public_hls(item_id: str):
    item = await FileSystemItem.get(item_id)
    if not item:
        raise HTTPException(404)
    storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
    if storage_chat_id == "me":
        return {"status": "unsupported"}
    await ensure_hls(item, storage_chat_id)
    return {"status": "started"}

@router.get("/s/progress/all")
async def get_public_progress_all(token: str, user: str):
    if not user:
        return {"items": {}}
    progresses = await PlaybackProgress.find(
        PlaybackProgress.user_type == "public",
        PlaybackProgress.user_key == user,
        PlaybackProgress.collection_token == token
    ).to_list()
    return {"items": {p.item_id: {"position": p.position, "duration": p.duration} for p in progresses}}

@router.get("/s/progress")
async def get_public_progress(token: str, item_id: str, user: str):
    if not user:
        return {"position": 0, "duration": 0}
    progress = await PlaybackProgress.find_one(
        PlaybackProgress.user_type == "public",
        PlaybackProgress.user_key == user,
        PlaybackProgress.item_id == item_id,
        PlaybackProgress.collection_token == token
    )
    if not progress:
        return {"position": 0, "duration": 0}
    return {"position": progress.position, "duration": progress.duration}

@router.post("/s/progress")
async def update_public_progress(payload: dict = Body(...)):
    token = payload.get("token")
    item_id = payload.get("item_id")
    user = payload.get("user_name")
    position = float(payload.get("position", 0))
    duration = float(payload.get("duration", 0))
    if not token or not item_id or not user:
        return {"error": "Missing data"}

    progress = await PlaybackProgress.find_one(
        PlaybackProgress.user_type == "public",
        PlaybackProgress.user_key == user,
        PlaybackProgress.item_id == item_id,
        PlaybackProgress.collection_token == token
    )
    if progress:
        progress.position = position
        progress.duration = duration
        progress.updated_at = datetime.now()
        await progress.save()
    else:
        progress = PlaybackProgress(
            user_key=user,
            user_type="public",
            item_id=item_id,
            collection_token=token,
            position=position,
            duration=duration,
            updated_at=datetime.now()
        )
        await progress.insert()
    return {"status": "success"}

@router.post("/share/reorder")
async def reorder_bundle(request: Request, payload: dict = Body(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized")

    token = payload.get("token")
    ordered_ids = payload.get("item_ids", [])
    if not token or not ordered_ids:
        return {"error": "Missing data"}

    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if not collection:
        return {"error": "Bundle not found"}

    # Only allow reordering of existing items
    allowed = set(collection.item_ids)
    filtered = [item_id for item_id in ordered_ids if item_id in allowed]
    if not filtered:
        return {"error": "Invalid order"}

    collection.item_ids = filtered
    await collection.save()
    return {"status": "success"}
