import os
import shutil
import tempfile
import traceback
import mimetypes 
import uuid
import re
import zipfile
import asyncio
import logging
from typing import Optional, Dict, List, Any

from fastapi import APIRouter, Request, UploadFile, File, Form, BackgroundTasks, Body
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from pyrogram import Client
from beanie import PydanticObjectId
from beanie.operators import Or, In
from app.db.models import FileSystemItem, FilePart, User, SharedCollection, TokenSetting
from app.core.config import settings
from app.core.telegram_bot import tg_client, user_client, get_pool_client, get_storage_chat_id, ensure_peer_access, get_storage_client, pick_storage_client, normalize_chat_id
from app.core.telethon_storage import send_file as tl_send_file, get_message as tl_get_message, iter_download as tl_iter_download, download_media as tl_download_media, delete_message as tl_delete_message
from app.utils.file_utils import format_size, get_icon_for_mime
from starlette.background import BackgroundTask

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
mimetypes.init()
logger = logging.getLogger(__name__)
ITEMS_PAGE_SIZE = 200

# --- IN-MEMORY JOB TRACKER ---
upload_jobs: Dict[str, dict] = {}

async def get_current_user(request: Request):
    """
    Resolve the current user from the session cookie, but be lenient:
    - If the cookie is present but the user is pending/blocked, treat as unauthenticated.
    - If the cookie is present and approved, always return that user without redirecting.
    """
    phone = request.cookies.get("user_phone")
    if not phone:
        return None
    user = await User.find_one(User.phone_number == phone)
    if not user or getattr(user, "status", "approved") != "approved":
        return None
    return user

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _natural_key(value: str):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", value or "")]

async def _get_link_token() -> str:
    token = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if not token:
        token = TokenSetting(key="link_token", value=str(uuid.uuid4()))
        await token.insert()
    return token.value

def _tokenize_search(text: str) -> List[str]:
    return [t for t in re.split(r"[^a-zA-Z0-9]+", (text or "").lower()) if t]

def _build_search_regex(text: str) -> Optional[str]:
    tokens = _tokenize_search(text)
    if not tokens:
        return None
    return ".*".join(re.escape(token) for token in tokens)

def _safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default

def _cast_ids(raw_ids: List[str]) -> List[PydanticObjectId]:
    casted: List[PydanticObjectId] = []
    for value in raw_ids or []:
        try:
            casted.append(PydanticObjectId(str(value)))
        except Exception:
            pass
    return casted

async def _build_folder_path_list(folder: FileSystemItem, user: User, is_admin: bool) -> List[Dict[str, Any]]:
    chain: List[FileSystemItem] = []
    current = folder
    broken = False
    while current:
        chain.append(current)
        if not current.parent_id:
            break
        parent = await FileSystemItem.get(current.parent_id)
        if not parent or (not is_admin and not _can_access(user, parent, is_admin)):
            broken = True
            break
        current = parent

    chain.reverse()
    path: List[Dict[str, Any]] = [{"id": "", "name": "Root"}]
    if broken and chain and chain[0].parent_id:
        path.append({"id": None, "name": "...", "disabled": True})
    for node in chain:
        path.append({"id": str(node.id), "name": node.name})
    return path

async def _build_folder_path_string(folder: FileSystemItem, user: User, is_admin: bool) -> str:
    parts: List[str] = []
    current = folder
    broken = False
    while current and current.parent_id:
        parent = await FileSystemItem.get(current.parent_id)
        if not parent or (not is_admin and not _can_access(user, parent, is_admin)):
            broken = True
            break
        parts.append(parent.name or "")
        current = parent
    parts.reverse()
    prefix = "Root"
    if broken:
        if parts:
            return f"{prefix} / ... / " + " / ".join(parts)
        return f"{prefix} / ..."
    if parts:
        return f"{prefix} / " + " / ".join(parts)
    return prefix

async def _has_selected_ancestor(item: FileSystemItem, selected_ids: set[str]) -> bool:
    current = item.parent_id
    while current:
        if current in selected_ids:
            return True
        node = await FileSystemItem.get(current)
        if not node:
            return False
        current = node.parent_id
    return False

async def _collect_folder_files(folder_id: str) -> List[FileSystemItem]:
    items: List[FileSystemItem] = []
    children = await FileSystemItem.find(FileSystemItem.parent_id == str(folder_id)).to_list()
    for child in children:
        if child.is_folder:
            items.extend(await _collect_folder_files(str(child.id)))
        else:
            items.append(child)
    return items

def _is_video_name(name: str, mime_type: str | None) -> bool:
    lower = (name or "").lower()
    if mime_type and mime_type.startswith("video"):
        return True
    return lower.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg"))

async def _ensure_storage_folder(admin_phone: str) -> FileSystemItem:
    folder = await FileSystemItem.find_one(
        FileSystemItem.owner_phone == admin_phone,
        FileSystemItem.parent_id == None,
        FileSystemItem.is_folder == True,
        FileSystemItem.name == "Videos in Storage"
    )
    if not folder:
        folder = FileSystemItem(
            name="Videos in Storage",
            is_folder=True,
            parent_id=None,
            owner_phone=admin_phone,
            source="storage"
        )
        await folder.insert()
    return folder

async def _sync_storage_folder(folder: FileSystemItem, limit: int | None = 200) -> int:
    storage_chat_id = normalize_chat_id(get_storage_chat_id())
    if storage_chat_id in (None, "me"):
        return 0

    history_client = user_client or (tg_client if not getattr(tg_client, "_is_bot", False) else None)
    if not history_client:
        logger.warning("Storage sync skipped: no user session available.")
        return 0

    try:
        if not await ensure_peer_access(history_client, storage_chat_id):
            logger.warning("Storage sync skipped: user client lacks access to storage channel.")
            return 0
    except Exception as e:
        logger.warning(f"Storage sync skipped: access check failed ({e}).")
        return 0

    existing_items = await FileSystemItem.find(FileSystemItem.parent_id == str(folder.id)).to_list()
    existing_ids = set()
    for item in existing_items:
        for part in item.parts or []:
            if part and getattr(part, "message_id", None):
                existing_ids.add(part.message_id)

    count = 0
    try:
        history_iter = history_client.get_chat_history(storage_chat_id, limit=limit) if limit is not None else history_client.get_chat_history(storage_chat_id)
        async for msg in history_iter:
            msg_id = getattr(msg, "id", None)
            if not msg_id or msg_id in existing_ids:
                continue

            file_obj = getattr(msg, "video", None) or getattr(msg, "document", None)
            if not file_obj:
                continue

            name = getattr(file_obj, "file_name", None) or getattr(msg, "file_name", None)
            mime_type = getattr(file_obj, "mime_type", None) or "application/octet-stream"
            if not name:
                name = f"video_{msg_id}.mp4" if mime_type.startswith("video") else f"file_{msg_id}"

            if not _is_video_name(name, mime_type):
                continue

            size = getattr(file_obj, "file_size", 0) or 0
            file_id = getattr(file_obj, "file_id", None) or str(msg_id)

            new_file = FileSystemItem(
                name=name,
                is_folder=False,
                parent_id=str(folder.id),
                owner_phone=folder.owner_phone,
                size=size,
                mime_type=mime_type,
                source="storage",
                parts=[FilePart(
                    telegram_file_id=str(file_id),
                    message_id=msg_id,
                    chat_id=storage_chat_id,
                    part_number=1,
                    size=size
                )]
            )
            await new_file.insert()
            count += 1
    except Exception as e:
        logger.warning(f"Storage sync failed: {e}")
    return count

@router.post("/storage/sync_all")
async def sync_storage_all(request: Request):
    user = await get_current_user(request)
    if not user or not _is_admin(user):
        return JSONResponse({"error": "Unauthorized"}, 403)
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if not admin_phone:
        return JSONResponse({"error": "Admin not configured"}, 400)
    folder = await _ensure_storage_folder(admin_phone)
    # Fire and forget sync (can be long)
    asyncio.create_task(_sync_storage_folder(folder, limit=None))
    return JSONResponse({"status": "started"})

def _is_admin(user: Optional[User]) -> bool:
    if not user: return False
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

def _can_access(user: User, item: FileSystemItem, is_admin: bool) -> bool:
    if is_admin: return True
    return item.owner_phone == user.phone_number or user.phone_number in (item.collaborators or [])

def _clone_parts(parts: List[FilePart]) -> List[FilePart]:
    cloned = []
    for part in parts or []:
        if isinstance(part, FilePart):
            data = part.dict()
        else:
            data = dict(part)
        cloned.append(FilePart(**data))
    return cloned

async def _is_descendant(parent_id: str, target_id: Optional[str]) -> bool:
    current = target_id
    while current:
        if current == parent_id:
            return True
        node = await FileSystemItem.get(current)
        if not node:
            return False
        current = node.parent_id
    return False

# --- HELPER 1: Recursively Create Folder Structure (Uploads) ---
async def get_or_create_folder_path(user_phone: str, start_parent_id: Optional[str], path_parts: list) -> Optional[str]:
    current_parent_id = start_parent_id
    for folder_name in path_parts:
        existing = await FileSystemItem.find_one(
            FileSystemItem.owner_phone == user_phone,
            FileSystemItem.parent_id == current_parent_id,
            FileSystemItem.name == folder_name,
            FileSystemItem.is_folder == True
        )
        if existing:
            current_parent_id = str(existing.id)
        else:
            new_folder = FileSystemItem(name=folder_name, is_folder=True, parent_id=current_parent_id, owner_phone=user_phone)
            await new_folder.insert()
            current_parent_id = str(new_folder.id)
    return current_parent_id

# --- HELPER 2: Recursive Download for Zip (Downloads) ---
async def download_item_recursive(bot_client, item, base_path, user_session_string: Optional[str] = None):
    """
    Downloads a file OR recursively downloads a folder contents to the base_path.
    """
    try:
        if item.is_folder:
            # 1. Create the folder locally
            new_folder_path = os.path.join(base_path, item.name)
            os.makedirs(new_folder_path, exist_ok=True)
            
            # 2. Find children
            children = await FileSystemItem.find(FileSystemItem.parent_id == str(item.id)).to_list()
            
            # 3. Recurse for each child
            for child in children:
                await download_item_recursive(bot_client, child, new_folder_path, user_session_string)
        else:
            # It's a file, download it
            # Refresh file ref by getting message again
            try:
                from_storage = False
                if item.parts and item.parts[0].chat_id:
                    chat_id = item.parts[0].chat_id
                else:
                    chat_id = get_storage_chat_id() or "me"
                    from_storage = True
                chat_id = normalize_chat_id(chat_id)
                if chat_id == "me" and user_session_string:
                    async with Client("downloader", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=user_session_string, in_memory=True) as app:
                        msg = await app.get_messages("me", message_ids=item.parts[0].message_id)
                        file_id = None
                        if msg.document: file_id = msg.document.file_id
                        elif msg.video: file_id = msg.video.file_id
                        elif msg.audio: file_id = msg.audio.file_id
                        elif msg.photo: file_id = msg.photo.file_id
                        if file_id:
                            await app.download_media(file_id, file_name=os.path.join(base_path, item.name))
                    return

                if chat_id == "me":
                    # Should have been handled above, but keep safe
                    return
                msg = await tl_get_message(item.parts[0].message_id)
                await tl_download_media(msg, os.path.join(base_path, item.name))
            except Exception as inner_e:
                print(f"Failed to refresh/download {item.name}: {inner_e}")
                
    except Exception as e:
        print(f"Error processing {item.name}: {e}")

# --- BACKGROUND UPLOAD TASK ---
async def process_telegram_upload(job_id: str, file_path: str, filename: str, mime_type: str, parent_id: Optional[str], user_phone: str, session_string: str):
    try:
        upload_jobs[job_id]["status"] = "uploading"
        async def progress(current, total):
            percent = (current / total) * 100
            upload_jobs[job_id]["progress"] = round(percent, 2)

        storage_chat_id = normalize_chat_id(get_storage_chat_id())
        if storage_chat_id == "me":
            async with Client("uploader", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=session_string, in_memory=True) as app:
                msg = await app.send_document(
                    chat_id="me",
                    document=file_path,
                    file_name=filename,
                    caption="Uploaded via MorganXMystic",
                    force_document=True,
                    progress=progress
                )
                new_file = FileSystemItem(
                    name=filename,
                    is_folder=False,
                    parent_id=parent_id,
                    owner_phone=user_phone,
                    size=msg.document.file_size,
                    mime_type=mime_type,
                    source="upload",
                    parts=[FilePart(telegram_file_id=msg.document.file_id, message_id=msg.id, chat_id=None, part_number=1, size=msg.document.file_size)]
                )
        else:
            msg = await tl_send_file(
                file_path,
                file_name=filename,
                caption="Uploaded via MorganXMystic",
                progress_cb=progress
            )
            msg_size = getattr(msg.file, "size", None) or os.path.getsize(file_path)
            msg_mime = getattr(msg.file, "mime_type", None) or mime_type
            new_file = FileSystemItem(
                name=filename,
                is_folder=False,
                parent_id=parent_id,
                owner_phone=user_phone,
                size=msg_size,
                mime_type=msg_mime,
                source="upload",
                parts=[FilePart(telegram_file_id=str(msg.id), message_id=msg.id, chat_id=storage_chat_id, part_number=1, size=msg_size)]
            )
        await new_file.insert()
        upload_jobs[job_id]["status"] = "completed"
        upload_jobs[job_id]["progress"] = 100
    except Exception as e:
        print(f"Upload Failed: {e}")
        upload_jobs[job_id]["status"] = "failed"
        upload_jobs[job_id]["error"] = str(e)
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass

@router.get("/")
async def root(): return RedirectResponse(url="/dashboard")

# --- DASHBOARD ---
@router.get("/dashboard")
async def dashboard(request: Request, folder_id: Optional[str] = None):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    is_admin = _is_admin(user)
    search_query = (request.query_params.get("q") or "").strip()
    if folder_id == "None" or folder_id == "": folder_id = None
    raw_scope = (request.query_params.get("scope") or "").strip().lower()
    search_scope = raw_scope if raw_scope in ("all", "folder") else ("folder" if folder_id else "all")
    offset = _safe_int(request.query_params.get("offset") or "0", 0)
    limit = _safe_int(request.query_params.get("limit") or str(ITEMS_PAGE_SIZE), ITEMS_PAGE_SIZE)
    if limit < 20: limit = 20
    if limit > 500: limit = 500

    storage_folder = None
    if is_admin:
        admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
        if admin_phone:
            storage_folder = await _ensure_storage_folder(admin_phone)

    if search_query:
        search_regex = _build_search_regex(search_query)
        if not search_regex:
            items = []
            current_folder = None
        else:
            query: Dict = {"name": {"$regex": search_regex, "$options": "i"}}
            if not is_admin:
                query["$or"] = [
                    {"owner_phone": user.phone_number},
                    {"collaborators": user.phone_number}
                ]

            current_folder = None
            if search_scope == "folder":
                if folder_id:
                    current_folder = await FileSystemItem.get(folder_id)
                    if not current_folder or not _can_access(user, current_folder, is_admin):
                        return RedirectResponse("/dashboard")
                    query["parent_id"] = folder_id
                else:
                    query["parent_id"] = None

            items = await FileSystemItem.find(query).skip(offset).limit(limit + 1).to_list()
    elif folder_id:
        current_folder = await FileSystemItem.get(folder_id)
        if not current_folder:
            return RedirectResponse("/dashboard")
        if not _can_access(user, current_folder, is_admin):
            return RedirectResponse("/dashboard")

        if storage_folder and str(storage_folder.id) == str(folder_id):
            try:
                existing_count = await FileSystemItem.find(FileSystemItem.parent_id == folder_id).count()
                if existing_count == 0:
                    await _sync_storage_folder(storage_folder, limit=200)
            except Exception as e:
                logger.warning(f"Storage folder sync failed: {e}")

        items = await FileSystemItem.find(FileSystemItem.parent_id == folder_id).skip(offset).limit(limit + 1).to_list()
    else:
        if is_admin:
            items = await FileSystemItem.find(FileSystemItem.parent_id == None).skip(offset).limit(limit + 1).to_list()
        else:
            items = await FileSystemItem.find(
                Or(FileSystemItem.owner_phone == user.phone_number, FileSystemItem.collaborators == user.phone_number),
                FileSystemItem.parent_id == None
            ).skip(offset).limit(limit + 1).to_list()

        current_folder = None
    visible_items = []
    has_more = False
    if len(items) > limit:
        has_more = True
        items = items[:limit]
    
    for item in items:
        if _can_access(user, item, is_admin) or folder_id:
            item.formatted_size = format_size(item.size)
            item.icon = "fa-folder" if item.is_folder else get_icon_for_mime(item.mime_type)
            if not item.share_token: item.share_token = ""
            visible_items.append(item)

    return templates.TemplateResponse("dashboard.html", {
        "request": request, "items": visible_items, "current_folder": current_folder, "user": user, "is_admin": is_admin,
        "search_query": search_query,
        "search_scope": search_scope,
        "offset": offset,
        "limit": limit,
        "has_more": has_more
    })

@router.get("/search/suggest")
async def search_suggest(request: Request, q: str = "", scope: str = "all", folder_id: str = ""):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"suggestions": []}, 401)
    q = (q or "").strip()
    if not q:
        return JSONResponse({"suggestions": []})

    scope = (scope or "").strip().lower()
    if scope not in ("all", "folder"):
        scope = "all"
    if folder_id == "None" or folder_id == "":
        folder_id = None

    search_regex = _build_search_regex(q)
    if not search_regex:
        return JSONResponse({"suggestions": []})

    is_admin = _is_admin(user)
    query: Dict = {"name": {"$regex": search_regex, "$options": "i"}}
    if not is_admin:
        query["$or"] = [
            {"owner_phone": user.phone_number},
            {"collaborators": user.phone_number}
        ]
    if scope == "folder":
        if folder_id:
            folder = await FileSystemItem.get(folder_id)
            if not folder or not _can_access(user, folder, is_admin):
                return JSONResponse({"suggestions": []}, 403)
            query["parent_id"] = folder_id
        else:
            query["parent_id"] = None

    items = await FileSystemItem.find(query).sort("name").limit(12).to_list()

    # Deduplicate while preserving order
    seen = set()
    suggestions = []
    payload_items = []
    for item in items:
        name = item.name or ""
        if not name:
            continue
        dedupe_key = name.lower()
        if dedupe_key in seen:
            continue
        suggestions.append(name)
        seen.add(dedupe_key)
        payload_items.append({"id": str(item.id), "name": name, "is_folder": item.is_folder})
        if len(suggestions) >= 10:
            break
    return JSONResponse({"suggestions": suggestions, "items": payload_items})

@router.get("/folders/list")
async def list_folders(request: Request, parent_id: str = "", q: str = ""):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"folders": []}, 401)
    is_admin = _is_admin(user)

    parent_id = parent_id if parent_id and parent_id != "None" else None
    q = (q or "").strip()

    if q:
        search_regex = _build_search_regex(q)
        if not search_regex:
            return JSONResponse({"mode": "search", "q": q, "folders": []})
        query: Dict[str, Any] = {
            "is_folder": True,
            "name": {"$regex": search_regex, "$options": "i"}
        }
        if not is_admin:
            query["$or"] = [
                {"owner_phone": user.phone_number},
                {"collaborators": user.phone_number}
            ]
        items = await FileSystemItem.find(query).sort("name").limit(80).to_list()
        payload = []
        for item in items:
            path = await _build_folder_path_string(item, user, is_admin)
            payload.append({
                "id": str(item.id),
                "name": item.name,
                "parent_id": item.parent_id,
                "path": path
            })
        return JSONResponse({"mode": "search", "q": q, "folders": payload})

    # Browse mode
    current = None
    if parent_id:
        current = await FileSystemItem.get(parent_id)
        if not current or not current.is_folder:
            return JSONResponse({"mode": "browse", "folders": [], "current": None}, 404)
        if not _can_access(user, current, is_admin):
            return JSONResponse({"mode": "browse", "folders": [], "current": None}, 403)

    query: Dict[str, Any] = {"is_folder": True, "parent_id": parent_id}
    if not is_admin:
        query["$or"] = [
            {"owner_phone": user.phone_number},
            {"collaborators": user.phone_number}
        ]
    children = await FileSystemItem.find(query).sort("name").to_list()

    if current:
        path = await _build_folder_path_list(current, user, is_admin)
        current_payload = {"id": str(current.id), "name": current.name, "parent_id": current.parent_id}
    else:
        path = [{"id": "", "name": "Root"}]
        current_payload = {"id": "", "name": "Root", "parent_id": None}

    payload = [{"id": str(item.id), "name": item.name, "parent_id": item.parent_id} for item in children]
    return JSONResponse({
        "mode": "browse",
        "current": current_payload,
        "path": path,
        "folders": payload
    })

# --- UPLOAD ROUTES ---
@router.get("/upload_zone")
async def upload_page(request: Request, folder_id: Optional[str] = None):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    is_admin = _is_admin(user)
    user_jobs = {k: v for k, v in upload_jobs.items() if v.get("owner") == user.phone_number}
    return templates.TemplateResponse("upload.html", {"request": request, "folder_id": folder_id, "user": user, "jobs": user_jobs, "is_admin": is_admin})

@router.post("/upload")
async def upload_file(request: Request, background_tasks: BackgroundTasks, file: UploadFile = File(...), parent_id: str = Form(""), relative_path: str = Form("")):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)

    try:
        # Clean Filename (No paths)
        original_name = file.filename or "unknown_file"
        safe_filename = os.path.basename(file.filename or "unknown_file")
        mime_type, _ = mimetypes.guess_type(safe_filename)
        if not mime_type: mime_type = file.content_type or "application/octet-stream"
        
        final_parent_id = parent_id if parent_id and parent_id != "None" else None

        # Folder Logic
        if relative_path and "/" in relative_path:
            path_parts = relative_path.split("/")[:-1]
            if path_parts:
                final_parent_id = await get_or_create_folder_path(user.phone_number, final_parent_id, path_parts)

        job_id = str(uuid.uuid4())
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        with open(tmp_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)

        upload_jobs[job_id] = {"id": job_id, "filename": safe_filename, "status": "queued", "progress": 0, "owner": user.phone_number}
        background_tasks.add_task(process_telegram_upload, job_id, tmp_path, safe_filename, mime_type, final_parent_id, user.phone_number, user.session_string)
        return JSONResponse({"status": "queued", "job_id": job_id})
    except Exception as e: return JSONResponse({"error": str(e)}, 500)

@router.get("/upload/status")
async def get_upload_status(request: Request):
    user = await get_current_user(request)
    if not user: return JSONResponse({})
    user_jobs = {k: v for k, v in upload_jobs.items() if v.get("owner") == user.phone_number}
    return JSONResponse(user_jobs)

# --- BULK DOWNLOAD (ZIP) ---
@router.post("/download/zip")
async def download_zip(request: Request, item_ids: List[str] = Body(...)):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)

    items = await FileSystemItem.find(In(FileSystemItem.id, item_ids)).to_list()
    if not items: return JSONResponse({"error": "No items found"}, 404)
    is_admin = _is_admin(user)
    for item in items:
        if not _can_access(user, item, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)

    temp_dir = tempfile.mkdtemp()
    zip_filename = f"MorganCloud_Bundle_{uuid.uuid4().hex[:6]}.zip"
    zip_path = os.path.join(tempfile.gettempdir(), zip_filename)

    try:
        # Prefer bot pool for storage channel access
        app = get_storage_client()
        for item in items:
            # Use recursive downloader to handle folders
            await download_item_recursive(app, item, temp_dir, user.session_string)

        shutil.make_archive(zip_path.replace('.zip', ''), 'zip', temp_dir)
        shutil.rmtree(temp_dir)

        return FileResponse(zip_path, filename=zip_filename, background=BackgroundTask(lambda: os.remove(zip_path)))

    except Exception as e:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, 500)

# --- BULK DELETE ---
@router.post("/delete/bundle")
async def delete_bundle(request: Request, item_ids: List[str] = Body(...)):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    items = await FileSystemItem.find(In(FileSystemItem.id, item_ids)).to_list()
    for item in items:
        if not _can_access(user, item, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)
    await FileSystemItem.find(In(FileSystemItem.id, item_ids)).delete()
    return JSONResponse({"status": "success"})

# --- STANDARD ACTIONS ---
@router.post("/delete/{item_id}")
async def delete_item(request: Request, item_id: str):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    item = await FileSystemItem.get(item_id)
    is_admin = _is_admin(user)
    if item and _can_access(user, item, is_admin):
        await item.delete()
    return RedirectResponse(f"/dashboard?folder_id={item.parent_id if item and item.parent_id else ''}", 303)

@router.post("/share/{item_id}")
async def share_item(request: Request, item_id: str):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Auth required"}, 401)
    item = await FileSystemItem.get(item_id)
    if not item: return JSONResponse({"error": "Not found"}, 404)
    is_admin = _is_admin(user)
    if not _can_access(user, item, is_admin):
        return JSONResponse({"error": "Unauthorized"}, 403)
    base_url = str(request.base_url).rstrip("/")
    if item.is_folder:
        # Stable link for folders: reuse share_token
        if not item.share_token:
            item.share_token = str(uuid.uuid4())
            await item.save()
        files = await _collect_folder_files(str(item.id))
        if not files:
            return JSONResponse({"error": "Folder is empty"}, 400)
        items_sorted = sorted(files, key=lambda i: _natural_key(i.name))
        token = item.share_token
        bundle = await SharedCollection.find_one(SharedCollection.token == token)
        if bundle:
            bundle.item_ids = [str(i.id) for i in items_sorted]
            bundle.name = item.name or bundle.name
            await bundle.save()
        else:
            bundle = SharedCollection(
                token=token,
                item_ids=[str(i.id) for i in items_sorted],
                owner_phone=user.phone_number,
                name=item.name or "Shared Folder"
            )
            await bundle.insert()
        link_token = await _get_link_token()
        links = {
            "view": f"{base_url}/s/{token}?t={link_token}&U=",
            "download": f"{base_url}/d/{token}?t={link_token}&U=",
            "telegram": f"{base_url}/t/{token}?t={link_token}&U=",
        }
        return JSONResponse({"code": token, "links": links, "link": links["view"]})

    if not item.share_token:
        item.share_token = str(uuid.uuid4())
        await item.save()
    token = item.share_token
    link_token = await _get_link_token()
    links = {
        "view": f"{base_url}/s/{token}?t={link_token}&U=",
        "download": f"{base_url}/d/{token}?t={link_token}&U=",
        "telegram": f"{base_url}/t/{token}?t={link_token}&U=",
    }
    return JSONResponse({"code": token, "links": links, "link": links["view"]})

@router.post("/create_folder")
async def create_folder(request: Request, folder_name: str = Form(...), parent_id: str = Form("")):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    final_parent_id = parent_id if parent_id and parent_id != "None" else None
    await FileSystemItem(name=folder_name, is_folder=True, parent_id=final_parent_id, owner_phone=user.phone_number).insert()
    return RedirectResponse(url=f"/dashboard?folder_id={final_parent_id}" if final_parent_id else "/dashboard", status_code=303)

@router.post("/folder/create_json")
async def create_folder_json(request: Request, payload: Dict = Body(...)):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, 401)
    folder_name = (payload.get("name") or "").strip()
    parent_id = payload.get("parent_id") or ""
    parent_id = parent_id if parent_id and parent_id != "None" else None
    if not folder_name:
        return JSONResponse({"error": "Invalid name"}, 400)
    if parent_id:
        parent = await FileSystemItem.get(parent_id)
        if not parent or not parent.is_folder:
            return JSONResponse({"error": "Parent not found"}, 404)
        if not _can_access(user, parent, _is_admin(user)):
            return JSONResponse({"error": "Unauthorized"}, 403)
    folder = FileSystemItem(name=folder_name, is_folder=True, parent_id=parent_id, owner_phone=user.phone_number)
    await folder.insert()
    return JSONResponse({"status": "success", "folder": {"id": str(folder.id), "name": folder.name, "parent_id": folder.parent_id}})

# --- FILE OPERATIONS ---
@router.post("/item/rename")
async def rename_item(request: Request, item_id: str = Form(...), new_name: str = Form(...), rename_mode: str = Form("fast")):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    item = await FileSystemItem.get(item_id)
    if not item: return JSONResponse({"error": "Not found"}, 404)
    if not _can_access(user, item, is_admin): return JSONResponse({"error": "Unauthorized"}, 403)

    new_name = new_name.strip()
    if not new_name:
        return JSONResponse({"error": "Invalid name"}, 400)
    if new_name == item.name:
        return JSONResponse({"status": "success", "name": new_name})

    if item.is_folder:
        item.name = new_name
        await item.save()
        return JSONResponse({"status": "success", "name": new_name})

    if not item.parts:
        return JSONResponse({"error": "Missing file parts"}, 400)

    rename_mode = (rename_mode or "fast").lower()
    if rename_mode == "fast":
        item.name = new_name
        await item.save()
        return JSONResponse({"status": "success", "name": new_name, "mode": "fast"})

    if item.parts and item.parts[0].chat_id:
        chat_id = item.parts[0].chat_id
        from_storage = False
    else:
        chat_id = get_storage_chat_id() or "me"
        from_storage = True
    chat_id = normalize_chat_id(chat_id)
    use_ephemeral = False
    if chat_id == "me":
        client = Client("renamer", api_id=settings.API_ID, api_hash=settings.API_HASH, session_string=user.session_string, in_memory=True)
        await client.connect()
        use_ephemeral = True
    else:
        client = None
        if from_storage:
            chat_id = normalize_chat_id(get_storage_chat_id())

    old_msg_id = item.parts[0].message_id
    try:
        if chat_id == "me":
            msg = await client.send_document(
                chat_id="me",
                document=item.parts[0].telegram_file_id,
                file_name=new_name,
                caption="Renamed via MorganXMystic",
                force_document=True
            )
            try:
                if old_msg_id:
                    await client.delete_messages("me", old_msg_id)
            except Exception:
                pass

            if msg.document:
                item.size = msg.document.file_size
                item.mime_type = msg.document.mime_type or item.mime_type
                item.parts[0].telegram_file_id = msg.document.file_id
                item.parts[0].message_id = msg.id
                item.parts[0].chat_id = None
        else:
            old_msg = await tl_get_message(old_msg_id)
            msg = await tl_send_file(
                old_msg,
                file_name=new_name,
                caption="Renamed via MorganXMystic"
            )
            await tl_delete_message(old_msg_id)

            msg_size = getattr(msg.file, "size", None) or item.size
            msg_mime = getattr(msg.file, "mime_type", None) or item.mime_type
            item.size = msg_size
            item.mime_type = msg_mime
            item.parts[0].telegram_file_id = str(msg.id)
            item.parts[0].message_id = msg.id
            item.parts[0].chat_id = chat_id

        item.name = new_name
        await item.save()
        return JSONResponse({"status": "success", "name": new_name})
    finally:
        if use_ephemeral and client:
            await client.disconnect()

@router.post("/item/move")
async def move_item(request: Request, item_id: str = Form(...), target_parent_id: str = Form("")):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    item = await FileSystemItem.get(item_id)
    if not item: return JSONResponse({"error": "Not found"}, 404)
    if not _can_access(user, item, is_admin): return JSONResponse({"error": "Unauthorized"}, 403)

    target_parent_id = target_parent_id if target_parent_id and target_parent_id != "None" else None
    if target_parent_id:
        target = await FileSystemItem.get(target_parent_id)
        if not target or not target.is_folder:
            return JSONResponse({"error": "Target folder not found"}, 404)
        if not _can_access(user, target, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)
        if item.is_folder and await _is_descendant(str(item.id), target_parent_id):
            return JSONResponse({"error": "Cannot move folder into itself"}, 400)

    item.parent_id = target_parent_id
    await item.save()
    return JSONResponse({"status": "success"})

@router.post("/item/copy")
async def copy_item(request: Request, item_id: str = Form(...), target_parent_id: str = Form("")):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    item = await FileSystemItem.get(item_id)
    if not item: return JSONResponse({"error": "Not found"}, 404)
    if not _can_access(user, item, is_admin): return JSONResponse({"error": "Unauthorized"}, 403)

    target_parent_id = target_parent_id if target_parent_id and target_parent_id != "None" else None
    if target_parent_id:
        target = await FileSystemItem.get(target_parent_id)
        if not target or not target.is_folder:
            return JSONResponse({"error": "Target folder not found"}, 404)
        if not _can_access(user, target, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)

    async def _copy_recursive(source_item: FileSystemItem, new_parent_id: Optional[str]):
        if source_item.is_folder:
            new_folder = FileSystemItem(
                name=f"{source_item.name} Copy" if source_item.id == item.id else source_item.name,
                is_folder=True,
                parent_id=new_parent_id,
                owner_phone=user.phone_number,
                source="copy"
            )
            await new_folder.insert()
            children = await FileSystemItem.find(FileSystemItem.parent_id == str(source_item.id)).to_list()
            for child in children:
                await _copy_recursive(child, str(new_folder.id))
        else:
            new_file = FileSystemItem(
                name=f"{source_item.name} Copy" if source_item.id == item.id else source_item.name,
                is_folder=False,
                parent_id=new_parent_id,
                owner_phone=user.phone_number,
                size=source_item.size,
                mime_type=source_item.mime_type,
                source="copy",
                parts=_clone_parts(source_item.parts)
            )
            await new_file.insert()

    await _copy_recursive(item, target_parent_id)
    return JSONResponse({"status": "success"})

@router.post("/item/move/bundle")
async def move_bundle(request: Request, payload: Dict = Body(...)):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    item_ids = payload.get("item_ids") or []
    target_parent_id = payload.get("target_parent_id") or ""
    target_parent_id = target_parent_id if target_parent_id and target_parent_id != "None" else None

    if not item_ids:
        return JSONResponse({"error": "No items selected"}, 400)

    target = None
    if target_parent_id:
        target = await FileSystemItem.get(target_parent_id)
        if not target or not target.is_folder:
            return JSONResponse({"error": "Target folder not found"}, 404)
        if not _can_access(user, target, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)

    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
    if not items:
        return JSONResponse({"error": "No items found"}, 404)

    selected_ids = {str(i.id) for i in items}
    effective_items: List[FileSystemItem] = []
    for item in items:
        if not _can_access(user, item, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)
        if await _has_selected_ancestor(item, selected_ids):
            continue
        effective_items.append(item)

    for item in effective_items:
        if item.is_folder and target_parent_id:
            if str(item.id) == target_parent_id:
                return JSONResponse({"error": "Cannot move folder into itself"}, 400)
            if await _is_descendant(str(item.id), target_parent_id):
                return JSONResponse({"error": "Cannot move folder into its child"}, 400)

    for item in effective_items:
        if item.parent_id == target_parent_id:
            continue
        item.parent_id = target_parent_id
        await item.save()

    return JSONResponse({"status": "success"})

@router.post("/item/copy/bundle")
async def copy_bundle(request: Request, payload: Dict = Body(...)):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, 401)
    is_admin = _is_admin(user)
    item_ids = payload.get("item_ids") or []
    target_parent_id = payload.get("target_parent_id") or ""
    target_parent_id = target_parent_id if target_parent_id and target_parent_id != "None" else None

    if not item_ids:
        return JSONResponse({"error": "No items selected"}, 400)

    target = None
    if target_parent_id:
        target = await FileSystemItem.get(target_parent_id)
        if not target or not target.is_folder:
            return JSONResponse({"error": "Target folder not found"}, 404)
        if not _can_access(user, target, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)

    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
    if not items:
        return JSONResponse({"error": "No items found"}, 404)

    selected_ids = {str(i.id) for i in items}
    effective_items: List[FileSystemItem] = []
    for item in items:
        if not _can_access(user, item, is_admin):
            return JSONResponse({"error": "Unauthorized"}, 403)
        if await _has_selected_ancestor(item, selected_ids):
            continue
        effective_items.append(item)

    async def _copy_recursive(source_item: FileSystemItem, new_parent_id: Optional[str], root_id: str):
        if source_item.is_folder:
            new_folder = FileSystemItem(
                name=f"{source_item.name} Copy" if str(source_item.id) == root_id else source_item.name,
                is_folder=True,
                parent_id=new_parent_id,
                owner_phone=user.phone_number,
                source="copy"
            )
            await new_folder.insert()
            children = await FileSystemItem.find(FileSystemItem.parent_id == str(source_item.id)).to_list()
            for child in children:
                await _copy_recursive(child, str(new_folder.id), root_id)
        else:
            new_file = FileSystemItem(
                name=f"{source_item.name} Copy" if str(source_item.id) == root_id else source_item.name,
                is_folder=False,
                parent_id=new_parent_id,
                owner_phone=user.phone_number,
                size=source_item.size,
                mime_type=source_item.mime_type,
                source="copy",
                parts=_clone_parts(source_item.parts)
            )
            await new_file.insert()

    for item in effective_items:
        if item.is_folder and target_parent_id:
            if str(item.id) == target_parent_id:
                return JSONResponse({"error": "Cannot copy folder into itself"}, 400)
            if await _is_descendant(str(item.id), target_parent_id):
                return JSONResponse({"error": "Cannot copy folder into its child"}, 400)

    for item in effective_items:
        await _copy_recursive(item, target_parent_id, str(item.id))

    return JSONResponse({"status": "success"})

# --- COLLAB ROUTES ---
@router.get("/folder/team/{folder_id}")
async def get_folder_team(request: Request, folder_id: str):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Auth required"}, 401)
    folder = await FileSystemItem.get(folder_id)
    if not folder: return JSONResponse({"error": "Not found"}, 404)
    if folder.owner_phone != user.phone_number and user.phone_number not in folder.collaborators:
        return JSONResponse({"error": "Unauthorized"}, 403)
    return JSONResponse({"collaborators": folder.collaborators, "owner": folder.owner_phone})

@router.post("/folder/add_collaborator")
async def add_collaborator(request: Request, folder_id: str = Form(...), phone: str = Form(...)):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Auth required"}, 401)
    folder = await FileSystemItem.get(folder_id)
    if not folder or folder.owner_phone != user.phone_number: return JSONResponse({"error": "Owner only"}, 403)
    if phone not in folder.collaborators:
        folder.collaborators.append(phone)
        await folder.save()
    return JSONResponse({"status": "success"})

@router.post("/folder/remove_collaborator")
async def remove_collaborator(request: Request, folder_id: str = Form(...), phone: str = Form(...)):
    user = await get_current_user(request)
    if not user: return JSONResponse({"error": "Auth required"}, 401)
    folder = await FileSystemItem.get(folder_id)
    if not folder or folder.owner_phone != user.phone_number: return JSONResponse({"error": "Owner only"}, 403)
    if phone in folder.collaborators:
        folder.collaborators.remove(phone)
        await folder.save()
        return JSONResponse({"status": "success"})
    return JSONResponse({"error": "User not found"}, 404)

@router.post("/share/bundle")
async def create_bundle(request: Request, item_ids: List[str] = Body(...)):
    user = await get_current_user(request)
    if not user: return {"error": "Unauthorized"}
    token = str(uuid.uuid4())
    bundle = SharedCollection(token=token, item_ids=item_ids, owner_phone=user.phone_number, name=f"Shared by {user.first_name or 'User'}")
    await bundle.insert()
    base_url = str(request.base_url).rstrip("/")
    link_token = await _get_link_token()
    links = {
        "view": f"{base_url}/s/{token}?t={link_token}&U=",
        "download": f"{base_url}/d/{token}?t={link_token}&U=",
        "telegram": f"{base_url}/t/{token}?t={link_token}&U=",
    }
    return {"code": token, "links": links, "link": links["view"]}

@router.get("/profile")
async def profile_page(request: Request):
    user = await get_current_user(request)
    # If not resolved, still try one last lookup but do not send to register form;
    # instead show login with a next pointer so logged-in users won't see account-request.
    if not user:
        phone = request.cookies.get("user_phone")
        if phone:
            user = await User.find_one(User.phone_number == phone)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "step": "phone", "next_url": "/profile"}
        )
    is_admin = _is_admin(user)
    total_files = await FileSystemItem.find(FileSystemItem.owner_phone == user.phone_number, FileSystemItem.is_folder == False).count()
    all_files = await FileSystemItem.find(FileSystemItem.owner_phone == user.phone_number, FileSystemItem.is_folder == False).sort("-created_at").to_list()
    for item in all_files:
        item.formatted_size = format_size(item.size)
        item.icon = get_icon_for_mime(item.mime_type)
    return templates.TemplateResponse("profile.html", {"request": request, "user": user, "total_files": total_files, "files": all_files, "is_admin": is_admin})
