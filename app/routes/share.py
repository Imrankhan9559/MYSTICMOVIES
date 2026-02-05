import uuid
import re
import os
import shutil
import tempfile
import logging
from datetime import datetime
from typing import List
from fastapi import APIRouter, Request, HTTPException, Body, Header
from fastapi.responses import StreamingResponse, RedirectResponse, HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from pyrogram import Client
from beanie.operators import In, Or
from beanie import PydanticObjectId
from app.db.models import FileSystemItem, User, SharedCollection, PlaybackProgress
from app.core.config import settings
from app.routes.stream import telegram_stream_generator, _align_offset, parallel_stream_generator, _get_parallel_clients
from app.core.telegram_bot import tg_client, get_pool_client, get_storage_client, get_storage_chat_id, pick_storage_client, normalize_chat_id
from app.core.telethon_storage import get_message as tl_get_message, iter_download as tl_iter_download, download_media as tl_download_media
from app.core.hls import ensure_hls, is_hls_ready, hls_url_for
from app.utils.file_utils import format_size, get_icon_for_mime
from app.routes.dashboard import get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

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


async def _resolve_shared_items(token: str) -> tuple[str | None, List[FileSystemItem]]:
    folder = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == True)
    if folder:
        items = await _collect_folder_files(str(folder.id))
        return "folder", items

    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if collection:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(collection.item_ids))).to_list()
        if any(item.is_folder for item in items):
            expanded: List[FileSystemItem] = []
            for item in items:
                if item.is_folder:
                    expanded.extend(await _collect_folder_files(str(item.id)))
                else:
                    expanded.append(item)
            items = expanded
        # Deduplicate while preserving order
        seen = set()
        unique_items: List[FileSystemItem] = []
        for item in items:
            key = str(item.id)
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(item)
        items = unique_items
        return "collection", items

    item = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == False)
    if item:
        return "file", [item]

    return None, []


async def _download_item_recursive_public(item: FileSystemItem, base_path: str):
    try:
        if item.is_folder:
            new_folder_path = os.path.join(base_path, item.name)
            os.makedirs(new_folder_path, exist_ok=True)
            children = await FileSystemItem.find(FileSystemItem.parent_id == str(item.id)).to_list()
            for child in children:
                await _download_item_recursive_public(child, new_folder_path)
            return

        if not item.parts:
            return

        if item.parts and item.parts[0].chat_id:
            chat_id = item.parts[0].chat_id
        else:
            chat_id = get_storage_chat_id() or "me"
        chat_id = normalize_chat_id(chat_id)

        if chat_id == "me":
            owner = await User.find_one(User.phone_number == item.owner_phone)
            if not owner or not owner.session_string:
                return
            async with Client(
                "pub_downloader",
                api_id=settings.API_ID,
                api_hash=settings.API_HASH,
                session_string=owner.session_string,
                in_memory=True
            ) as app:
                msg = await app.get_messages("me", message_ids=item.parts[0].message_id)
                file_id = None
                if msg.document:
                    file_id = msg.document.file_id
                elif msg.video:
                    file_id = msg.video.file_id
                elif msg.audio:
                    file_id = msg.audio.file_id
                elif msg.photo:
                    file_id = msg.photo.file_id
                if file_id:
                    await app.download_media(file_id, file_name=os.path.join(base_path, item.name))
            return

        if chat_id == "me":
            return
        msg = await tl_get_message(item.parts[0].message_id)
        await tl_download_media(msg, os.path.join(base_path, item.name))
    except Exception:
        return


def _build_bulk_download_page(base_url: str, items: List[FileSystemItem]) -> str:
    links = [f"{base_url}/d/file/{item.id}" for item in items]
    links_html = "\n".join([f'<li><a href="{url}" target="_blank" rel="noopener">{url}</a></li>' for url in links])
    links_js = ",\n".join([f'"{url}"' for url in links])

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Preparing Downloads</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 24px; background: #0f172a; color: #e2e8f0; }}
    h2 {{ margin-bottom: 8px; }}
    p {{ margin-top: 0; color: #94a3b8; }}
    ul {{ margin-top: 12px; }}
    a {{ color: #38bdf8; }}
  </style>
</head>
<body>
  <h2>Starting your downloads…</h2>
  <p>If your browser blocks multiple downloads, use the links below.</p>
  <ul>{links_html}</ul>
  <script>
    const links = [{links_js}];
    let idx = 0;
    function triggerNext() {{
      if (idx >= links.length) return;
      const a = document.createElement('a');
      a.href = links[idx];
      a.download = '';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      idx += 1;
      setTimeout(triggerNext, 700);
    }}
    triggerNext();
  </script>
</body>
</html>
"""

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
    links = {
        "view": f"{base_url}/s/{token}",
        "download": f"{base_url}/d/{token}",
        "telegram": f"{base_url}/t/{token}",
    }
    return {"code": token, "links": links, "link": links["view"]}

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
        if not ordered_items:
            raise HTTPException(404, "No items found")

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
            active_item = next((i for i in ordered_items if _is_video_item(i)), ordered_items[0] if ordered_items else None)
        if not active_item:
            raise HTTPException(404, "No items found")

        active_hls = ""
        if active_item and active_item.is_video:
            storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
            if storage_chat_id != "me":
                try:
                    await ensure_hls(active_item, storage_chat_id)
                    if is_hls_ready(str(active_item.id)):
                        active_hls = hls_url_for(str(active_item.id))
                except Exception as e:
                    logger.warning(f"HLS prep failed for shared folder item: {e}")

        return templates.TemplateResponse("shared_folder.html", {
            "request": request,
            "episodes": ordered_items,
            "active_item": active_item,
            "item": active_item,
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
            active_item = next((i for i in ordered_items if _is_video_item(i)), ordered_items[0] if ordered_items else None)
        if not active_item:
            raise HTTPException(404, "No items found")

        active_hls = ""
        if active_item and active_item.is_video:
            storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
            if storage_chat_id != "me":
                try:
                    await ensure_hls(active_item, storage_chat_id)
                    if is_hls_ready(str(active_item.id)):
                        active_hls = hls_url_for(str(active_item.id))
                except Exception as e:
                    logger.warning(f"HLS prep failed for shared collection item: {e}")

        return templates.TemplateResponse("shared_folder.html", {
            "request": request,
            "episodes": ordered_items,
            "active_item": active_item,
            "item": active_item,
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
                try:
                    await ensure_hls(item, storage_chat_id)
                    if is_hls_ready(str(item.id)):
                        active_hls = hls_url_for(str(item.id))
                except Exception as e:
                    logger.warning(f"HLS prep failed for shared item: {e}")
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
async def public_stream_by_id(item_id: str, request: Request, range: str = Header(None), download: bool = False):
    item = await FileSystemItem.get(item_id)
    if not item: raise HTTPException(404)
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

    disposition = "attachment" if download else "inline"

    use_ephemeral = False
    parallel_clients: list[Client] = []
    storage_primary: Client | None = None
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
        try:
            parallel_clients = await _get_parallel_clients(chat_id)
        except Exception:
            parallel_clients = []
        if client and client not in parallel_clients:
            parallel_clients.append(client)
        storage_primary = client or (parallel_clients[0] if parallel_clients else None)

    async def cleanup():
        try:
            msg_id = item.parts[0].message_id
            if chat_id == "me":
                sent = 0
                limit = (end - start + 1) if file_size else None
                aligned_offset, skip = _align_offset(start)
                aligned_limit = (limit + skip) if limit is not None else None
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
                    async for chunk in parallel_stream_generator(parallel_clients, chat_id, msg_id, start, end):
                        yield chunk
                else:
                    sent = 0
                    aligned_offset, skip = _align_offset(start)
                    aligned_limit = (limit + skip) if limit is not None else None
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
            else:
                msg = await tl_get_message(msg_id)
                limit = (end - start + 1) if file_size else None
                async for chunk in tl_iter_download(msg, offset=start, limit=limit):
                    yield chunk
        finally:
            if use_ephemeral and client:
                await client.disconnect()

    headers = {
        'Content-Disposition': f'{disposition}; filename="{item.name}"',
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


@router.get("/d/file/{item_id}")
async def public_download_by_id(item_id: str, request: Request, range: str = Header(None)):
    return await public_stream_by_id(item_id, request, range, download=True)


@router.get("/d/{token}")
async def public_download_token(request: Request, token: str, range: str = Header(None)):
    kind, items = await _resolve_shared_items(token)
    if not kind or not items:
        raise HTTPException(404)

    if kind == "file":
        return await public_stream_by_id(str(items[0].id), request, range, download=True)

    video_items = [i for i in items if _is_video_item(i)]
    if not video_items:
        raise HTTPException(404, "No video files found.")

    video_items = sorted(video_items, key=lambda i: _natural_key(i.name))

    if len(video_items) > 30:
        temp_dir = tempfile.mkdtemp()
        zip_filename = f"Mystic_Bundle_{uuid.uuid4().hex[:6]}.zip"
        zip_path = os.path.join(tempfile.gettempdir(), zip_filename)
        try:
            for item in video_items:
                await _download_item_recursive_public(item, temp_dir)
            shutil.make_archive(zip_path.replace(".zip", ""), "zip", temp_dir)
            shutil.rmtree(temp_dir)
            return FileResponse(
                zip_path,
                filename=zip_filename,
                background=BackgroundTask(lambda: os.remove(zip_path))
            )
        except Exception:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise HTTPException(500, "Download failed.")

    base_url = str(request.base_url).rstrip("/")
    html = _build_bulk_download_page(base_url, video_items)
    return HTMLResponse(html)


@router.get("/t/{token}")
async def telegram_redirect(token: str):
    kind, items = await _resolve_shared_items(token)
    if not kind:
        raise HTTPException(404)
    bot_username = (getattr(settings, "BOT_USERNAME", "") or "").lstrip("@")
    if not bot_username:
        return HTMLResponse("Bot username is not configured.", status_code=400)
    tg_url = f"https://t.me/{bot_username}?start=share_{token}"
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="refresh" content="0; url={tg_url}" />
  <title>Redirecting…</title>
</head>
<body>
  <p>Redirecting to Telegram…</p>
  <script>window.location.href = "{tg_url}";</script>
</body>
</html>
"""
    return HTMLResponse(html)

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
