import uuid
import random
import re
import os
import shutil
import tempfile
import logging
import urllib.parse
from datetime import datetime
from typing import List
from fastapi import APIRouter, Request, HTTPException, Body, Header
from fastapi.responses import StreamingResponse, RedirectResponse, HTMLResponse, FileResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from pyrogram import Client
from beanie.operators import In, Or
from beanie import PydanticObjectId
from app.db.models import FileSystemItem, User, SharedCollection, PlaybackProgress, TokenSetting, WatchParty, WatchPartyMember, WatchPartyMessage, UserActivityEvent, SiteSettings
from app.core.config import settings
from app.routes.stream import telegram_stream_generator, _align_offset, _align_range, parallel_stream_generator, _get_parallel_clients, _extract_file_size, _pick_align
from app.core.telegram_bot import tg_client, get_pool_client, get_storage_client, get_storage_chat_id, pick_storage_client, normalize_chat_id, ensure_peer_access
from app.core.telethon_storage import get_message as tl_get_message, iter_download as tl_iter_download, download_media as tl_download_media
from app.core.hls import ensure_hls, is_hls_ready, hls_url_for
from app.utils.file_utils import format_size, get_icon_for_mime
from app.routes.dashboard import get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)


async def _site_settings() -> SiteSettings:
    row = await SiteSettings.find_one(SiteSettings.key == "main")
    if not row:
        row = SiteSettings(key="main")
        await row.insert()
    return row


def _event_title(item: FileSystemItem | None) -> str:
    if not item:
        return ""
    return (
        (getattr(item, "series_title", "") or "").strip()
        or (getattr(item, "title", "") or "").strip()
        or (item.name or "").strip()
    )


QUALITY_HINT_RE = re.compile(
    r"(8k|4k|2k|4320p|2160p|1440p|1080p|720p|480p|380p|360p|240p|144p)",
    re.I,
)


def _quality_label(value: str) -> str:
    raw = (value or "").strip().upper()
    if not raw:
        return ""
    compact = re.sub(r"\s+", "", raw)
    if "8K" in compact or "4320P" in compact:
        return "8K"
    if "4K" in compact or "2160P" in compact or "UHD" in compact:
        return "4K"
    if "2K" in compact or "1440P" in compact:
        return "2K"
    p_match = re.search(r"(1080|720|480|380|360|240|144)P", compact)
    if p_match:
        return f"{p_match.group(1)}P"
    if "FHD" in compact:
        return "1080P"
    if compact == "HD":
        return "HD"
    token = re.split(r"[\s/_|,-]+", raw)[0].strip()
    return token[:8] if len(token) > 8 else token


def _item_quality(item: FileSystemItem | None) -> str:
    if not item:
        return ""
    from_meta = _quality_label((getattr(item, "quality", "") or "").strip())
    if from_meta:
        return from_meta
    name = (item.name or "").strip()
    m = QUALITY_HINT_RE.search(name)
    return _quality_label(m.group(1)) if m else ""


def _display_title(item: FileSystemItem | None) -> str:
    if not item:
        return "Shared Content"
    title = _event_title(item) or (item.name or "").strip() or "Shared Content"
    quality = _item_quality(item)
    if quality and quality.lower() not in title.lower():
        return f"{title} ({quality})"
    return title


def _share_query(link_token: str, viewer_name: str) -> str:
    params: list[str] = []
    if link_token:
        params.append(f"t={urllib.parse.quote(link_token, safe='')}")
    if viewer_name:
        params.append(f"U={urllib.parse.quote(viewer_name, safe='')}")
    return "&".join(params)


async def _log_activity(
    action: str,
    request: Request | None = None,
    viewer_name: str = "",
    token: str = "",
    item: FileSystemItem | None = None,
    items: List[FileSystemItem] | None = None,
    meta: dict | None = None,
) -> None:
    try:
        user = await get_current_user(request) if request else None
        user_type = "user" if user else ("public" if viewer_name else "guest")
        user_phone = user.phone_number if user else None
        user_name = (
            (user.requested_name or user.first_name or user.phone_number)
            if user else (viewer_name or None)
        )
        chosen = item or (items[0] if items else None)
        event = UserActivityEvent(
            action=(action or "").strip().lower(),
            user_type=user_type,
            user_key=user_phone or user_name or "guest",
            user_phone=user_phone,
            user_name=user_name,
            token=(token or "").strip() or None,
            item_id=str(chosen.id) if chosen else None,
            content_title=_event_title(chosen) if chosen else None,
            meta=meta or {},
            created_at=datetime.now(),
        )
        await event.insert()
    except Exception as exc:
        logger.debug("Activity log failed for action=%s: %s", action, exc)

def _natural_key(value: str):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", value or "")]

def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi"))

def _parallel_conf(download: bool = False) -> tuple[int, int]:
    workers_key = "DL_WORKERS_DOWNLOAD" if download else "DL_WORKERS"
    stripe_key = "DL_STRIPE_MB_DOWNLOAD" if download else "DL_STRIPE_MB"
    try:
        workers = int(os.getenv(workers_key, "7"))
        if workers < 1: workers = 1
        if workers > 8: workers = 8
    except Exception:
        workers = 7
    try:
        default_stripe = "8" if download else "4"
        stripe_mb = int(os.getenv(stripe_key, default_stripe))
        if stripe_mb < 1: stripe_mb = 1
        if stripe_mb > 32: stripe_mb = 32
    except Exception:
        stripe_mb = 8 if download else 2
    return workers, stripe_mb * 1024 * 1024

def _extract_file_size(msg) -> int | None:
    if not msg:
        return None
    if getattr(msg, "document", None):
        return getattr(msg.document, "size", None) or getattr(msg.document, "file_size", None)
    if getattr(msg, "video", None):
        return getattr(msg.video, "size", None) or getattr(msg.video, "file_size", None)
    if getattr(msg, "audio", None):
        return getattr(msg.audio, "size", None) or getattr(msg.audio, "file_size", None)
    if getattr(msg, "photo", None) and getattr(msg.photo, "sizes", None):
        sizes = msg.photo.sizes
        if sizes:
            return getattr(sizes[-1], "size", None)
    if getattr(msg, "file", None):
        return getattr(msg.file, "size", None)
    return None

async def _get_link_token() -> str:
    token = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if token and token.value:
        return token.value
    new_val = str(uuid.uuid4())
    if token:
        token.value = new_val
        await token.save()
    else:
        token = TokenSetting(key="link_token", value=new_val)
        await token.insert()
    return new_val

async def _validate_link_token(request: Request):
    provided = (request.query_params.get("t") or "").strip()
    token = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if not token:
        if provided:
            await TokenSetting(key="link_token", value=provided).insert()
        else:
            await TokenSetting(key="link_token", value=str(uuid.uuid4())).insert()
        return
    if provided and provided != token.value:
        token.value = provided
        await token.save()
    # If no provided token, allow access

async def _require_token_and_username(request: Request, login_url: str):
    """Strict gate: valid link token AND non-empty username; otherwise redirect."""
    provided = (request.query_params.get("t") or "").strip()
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if not token_doc or token_doc.value != provided:
        return RedirectResponse(login_url)
    viewer_name = (request.query_params.get("u") or request.query_params.get("U") or "").strip()
    if not viewer_name:
        return RedirectResponse(login_url)
    return viewer_name

PARTY_HOST_TIMEOUT_SECONDS = 60

def _generate_room_code() -> str:
    return f"{random.randint(0, 999999):06d}"

async def _get_or_create_party(token: str, user_name: str) -> WatchParty:
    now = datetime.now()
    party = await WatchParty.find_one(WatchParty.token == token)
    if not party:
        party = WatchParty(token=token, room_code=_generate_room_code(), host_name=user_name, host_last_seen=now, updated_at=now)
        await party.insert()
        return party
    if not getattr(party, "room_code", None):
        party.room_code = _generate_room_code()
    host_age = (now - party.host_last_seen).total_seconds() if party.host_last_seen else PARTY_HOST_TIMEOUT_SECONDS + 1
    if host_age > PARTY_HOST_TIMEOUT_SECONDS:
        party.host_name = user_name
    if party.host_name == user_name:
        party.host_last_seen = now
    party.updated_at = now
    await party.save()
    return party

def _select_default_item(items: List[FileSystemItem]) -> FileSystemItem | None:
    if not items:
        return None
    videos = [i for i in items if _is_video_item(i)]
    if not videos:
        return items[0]
    patterns = [
        r"(?:^|\\b)(?:episode|ep)[\\s._-]*0*1\\b",
        r"(?:^|\\b)e0*1\\b",
        r"s\\d+e0*1\\b"
    ]
    for item in videos:
        name = (item.name or "").lower()
        if any(re.search(pat, name) for pat in patterns):
            return item
    return videos[0]

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    if str(getattr(user, "role", "") or "").strip().lower() == "admin":
        return True
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
    link_token = await _get_link_token()
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
        "view": f"{base_url}/s/{token}?t={link_token}",
        "download": f"{base_url}/d/{token}?t={link_token}",
        "telegram": f"{base_url}/t/{token}?t={link_token}",
        "watch": f"{base_url}/w/{token}?t={link_token}&U=",
    }
    return {"code": token, "links": links, "link": links["view"]}

@router.get("/s/{token}")
async def public_view(request: Request, token: str):
    # Strict gate: require valid link token and viewer name
    viewer_name = await _require_token_and_username(request, "/login")
    if isinstance(viewer_name, RedirectResponse):
        return viewer_name
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    site = await _site_settings()
    link_token = await _get_link_token()
    share_query = _share_query(link_token, viewer_name)
    share_suffix = f"?{share_query}" if share_query else ""
    hide_auth = user is None
    banner_title = "Want Unlimited Cloud Storage?"
    banner_sub = "Store videos, files, and folders without limits. Stream anywhere, anytime."
    banner_link = "/login"
    # Token-specific overrides
    if token == "7b2c8c87-1891-4f11-9c31-1a1e61821028":
        hide_auth = True
        banner_title = "Watch and download movies & web-series for free"
        banner_sub = "Visit mysticmovies.rf.gd to explore."
        banner_link = "https://mysticmovies.rf.gd"
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
            active_item = _select_default_item(ordered_items)
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
            "site": site,
            "episodes": ordered_items,
            "active_item": active_item,
            "item": active_item,
            "display_title": _display_title(active_item),
            "stream_url": f"/s/stream/file/{active_item.id}{share_suffix}",
            "download_url": f"/d/{token}{share_suffix}",
            "telegram_url": f"/t/{token}{share_suffix}",
            "hls_url": active_hls,
            "bundle_name": folder.name or (collection.name if collection else "Shared Folder"),
            "viewer_name": viewer_name,
            "token": token,
            "is_admin": is_admin,
            "user": user,
            "bot_username": getattr(settings, "BOT_USERNAME", ""),
            "hide_auth": hide_auth,
            "banner_title": banner_title,
            "banner_sub": banner_sub,
            "banner_link": banner_link,
            "link_token": link_token,
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
            active_item = _select_default_item(ordered_items)
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
            "site": site,
            "episodes": ordered_items,
            "active_item": active_item,
            "item": active_item,
            "display_title": _display_title(active_item),
            "stream_url": f"/s/stream/file/{active_item.id}{share_suffix}",
            "download_url": f"/d/{token}{share_suffix}",
            "telegram_url": f"/t/{token}{share_suffix}",
            "hls_url": active_hls,
            "bundle_name": collection.name,
            "viewer_name": viewer_name,
            "token": token,
            "is_admin": is_admin,
            "user": user,
            "bot_username": getattr(settings, "BOT_USERNAME", ""),
            "link_token": link_token,
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
            "site": site,
            "item": item,
            "display_title": _display_title(item),
            "stream_url": f"/s/stream/{token}{share_suffix}",
            "download_url": f"/d/{token}{share_suffix}",
            "telegram_url": f"/t/{token}{share_suffix}",
            "hls_url": active_hls,
            "viewer_name": viewer_name,
            "token": token,
            "bot_username": getattr(settings, "BOT_USERNAME", ""),
            "user": user,
            "is_admin": is_admin,
            "hide_auth": hide_auth,
            "banner_title": banner_title,
            "banner_sub": banner_sub,
            "banner_link": banner_link,
            "link_token": link_token
        })

    raise HTTPException(404, "Link expired")


@router.get("/w/{token}")
async def watch_party_view(request: Request, token: str):
    # Require valid link token, but allow missing username so we can prompt on-page.
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    provided = (request.query_params.get("t") or "").strip()
    if not token_doc or token_doc.value != provided:
        return RedirectResponse("/login")
    viewer_name = (request.query_params.get("u") or request.query_params.get("U") or "").strip()
    party = await _get_or_create_party(token, viewer_name) if viewer_name else None

    user = await get_current_user(request)
    is_admin = _is_admin(user)
    link_token = await _get_link_token()
    episode_override = request.query_params.get("e")

    # Folder share
    folder = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == True)
    if folder:
        files = await _collect_folder_files(str(folder.id))
        if not files:
            raise HTTPException(404, "No items found")
        collection = await SharedCollection.find_one(SharedCollection.token == token)
        ordered_items = _order_items(files, collection.item_ids if collection else None)
        if not ordered_items:
            raise HTTPException(404, "No items found")

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
            active_item = _select_default_item(ordered_items)
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
                    logger.warning(f"HLS prep failed for watch party item: {e}")

        if viewer_name:
            await _log_activity(
                action="watch_together_open",
                request=request,
                viewer_name=viewer_name,
                token=token,
                item=active_item,
                items=ordered_items,
                meta={"kind": "folder"},
            )

        return templates.TemplateResponse("watch_party.html", {
            "request": request,
            "episodes": ordered_items,
            "active_item": active_item,
            "item": active_item,
            "stream_url": f"/s/stream/file/{active_item.id}?t={link_token}&u={viewer_name}",
            "hls_url": active_hls,
            "bundle_name": folder.name or (collection.name if collection else "Shared Folder"),
            "viewer_name": viewer_name,
            "token": token,
            "is_admin": is_admin,
            "user": user,
            "link_token": link_token,
            "room_code": party.room_code if party else "",
            "host_name": party.host_name if party else ""
        })

    # Bundle or single file
    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if collection:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(collection.item_ids))).to_list()
        if not items:
            raise HTTPException(404, "Link expired")
        ordered_items = _order_items(items, collection.item_ids)
        item = _select_default_item(ordered_items) or ordered_items[0]
    else:
        item = await FileSystemItem.find_one(FileSystemItem.share_token == token)
        if not item:
            raise HTTPException(404, "Link expired")

    item.formatted_size = format_size(item.size)
    item.icon = get_icon_for_mime(item.mime_type)
    item.is_video = _is_video_item(item)

    active_hls = ""
    if item and item.is_video:
        storage_chat_id = normalize_chat_id(get_storage_chat_id() or "me")
        if storage_chat_id != "me":
            try:
                await ensure_hls(item, storage_chat_id)
                if is_hls_ready(str(item.id)):
                    active_hls = hls_url_for(str(item.id))
            except Exception as e:
                logger.warning(f"HLS prep failed for watch party item: {e}")

    if viewer_name:
        await _log_activity(
            action="watch_together_open",
            request=request,
            viewer_name=viewer_name,
            token=token,
            item=item,
            meta={"kind": "bundle_or_file"},
        )

    return templates.TemplateResponse("watch_party.html", {
        "request": request,
        "item": item,
        "stream_url": f"/s/stream/{token}?t={link_token}&u={viewer_name}",
        "hls_url": active_hls,
        "viewer_name": viewer_name,
        "token": token,
        "link_token": link_token,
        "room_code": party.room_code if party else "",
        "host_name": party.host_name if party else ""
    })

@router.get("/s/{token}/u={username}")
async def public_view_with_user(token: str, username: str):
    link_token = await _get_link_token()
    safe_user = urllib.parse.quote(username, safe="")
    safe_token = urllib.parse.quote(link_token, safe="")
    return RedirectResponse(url=f"/s/{token}?U={safe_user}&t={safe_token}")

@router.get("/s/stream/file/{item_id}")
async def public_stream_by_id(item_id: str, request: Request, range: str = Header(None), download: bool = False):
    await _validate_link_token(request)
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

    disposition = "attachment" if download else "inline"

    # Limit parallel clients based on env (download vs stream)
    max_workers, stripe_size = _parallel_conf(download=download)

    use_ephemeral = False
    parallel_clients: list[Client] = []
    release_parallel = None
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
            if bot_pool:
                candidate = get_pool_client()
                if candidate and await ensure_peer_access(candidate, chat_id):
                    client = candidate
        except Exception:
            client = None
        if not client:
            try:
                client = await pick_storage_client(chat_id)
            except Exception:
                client = None
        try:
            parallel_clients, release_parallel = await _get_parallel_clients(chat_id, max_workers=max_workers)
        except Exception:
            parallel_clients = []
            release_parallel = None
        if client and client not in parallel_clients:
            parallel_clients.append(client)
        storage_primary = client or (parallel_clients[0] if parallel_clients else None)

    msg_id = item.parts[0].message_id
    # Probe actual size to avoid OFFSET_INVALID on wrong DB sizes
    try:
        # Prefer telethon (works even if pyrogram session hits rate limits)
        msg = await tl_get_message(msg_id)
        actual_size = _extract_file_size(msg)
        if not actual_size and client and await ensure_peer_access(client, chat_id):
            msg_pyro = await client.get_messages(chat_id, message_ids=msg_id)
            actual_size = _extract_file_size(msg_pyro)
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
    align = _pick_align(file_size, for_download=download)

    if parallel_clients:
        parallel_clients = parallel_clients[:max_workers]

    async def cleanup():
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
                        async for chunk in parallel_stream_generator(parallel_clients, chat_id, msg_id, start, end, chunk_size=stripe_size):
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
    await _validate_link_token(request)
    item = await FileSystemItem.find_one(FileSystemItem.share_token == token)
    if not item: raise HTTPException(404)
    return await public_stream_by_id(str(item.id), request, range)


@router.get("/d/file/{item_id}")
async def public_download_by_id(item_id: str, request: Request, range: str = Header(None)):
    await _validate_link_token(request)
    return await public_stream_by_id(item_id, request, range, download=True)


@router.get("/d/{token}")
async def public_download_token(request: Request, token: str, range: str = Header(None)):
    viewer_name = await _require_token_and_username(request, "/login")
    if isinstance(viewer_name, RedirectResponse):
        return viewer_name
    kind, items = await _resolve_shared_items(token)
    if not kind or not items:
        raise HTTPException(404)

    await _log_activity(
        action="download_request",
        request=request,
        viewer_name=viewer_name,
        token=token,
        item=items[0] if items else None,
        items=items,
        meta={"kind": kind, "item_count": len(items)},
    )

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
async def telegram_redirect(token: str, request: Request):
    viewer_name = await _require_token_and_username(request, "/login")
    if isinstance(viewer_name, RedirectResponse):
        return viewer_name
    kind, items = await _resolve_shared_items(token)
    if not kind:
        raise HTTPException(404)
    await _log_activity(
        action="telegram_request",
        request=request,
        viewer_name=viewer_name,
        token=token,
        item=items[0] if items else None,
        items=items,
        meta={"kind": kind, "item_count": len(items)},
    )
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
  <title>Redirecting...</title>
</head>
<body>
  <p>Redirecting to Telegram...</p>
  <script>window.location.href = "{tg_url}";</script>
</body>
</html>
"""
    return HTMLResponse(html)


@router.post("/w/party/join")
async def watch_party_join(payload: dict = Body(...)):
    token = (payload.get("token") or "").strip()
    user_name = (payload.get("user_name") or "").strip()
    if not token or not user_name:
        return {"error": "Missing token or user"}
    party = await _get_or_create_party(token, user_name)
    # Upsert member presence
    member = await WatchPartyMember.find_one(
        WatchPartyMember.token == token,
        WatchPartyMember.user_name == user_name
    )
    if member:
        member.last_seen = datetime.now()
        await member.save()
    else:
        await WatchPartyMember(token=token, user_name=user_name).insert()
    role = "host" if party.host_name == user_name else "viewer"
    await _log_activity(
        action="watch_together_join",
        viewer_name=user_name,
        token=token,
        meta={"role": role},
    )
    return {"role": role, "host_name": party.host_name, "room_code": party.room_code}


@router.get("/w/party/state")
async def watch_party_state(token: str):
    token = (token or "").strip()
    if not token:
        return {"error": "Missing token"}
    party = await WatchParty.find_one(WatchParty.token == token)
    if not party:
        return {"error": "Not found"}
    return {
        "host_name": party.host_name,
        "position": party.position,
        "item_id": party.item_id,
        "is_playing": party.is_playing,
        "room_code": party.room_code,
        "updated_at": party.updated_at.isoformat() if party.updated_at else ""
    }


@router.get("/w/party/members")
async def watch_party_members(token: str):
    token = (token or "").strip()
    if not token:
        return {"members": []}
    cutoff = datetime.now().timestamp() - 40
    members = await WatchPartyMember.find(WatchPartyMember.token == token).to_list()
    active = []
    for m in members:
        ts = m.last_seen.timestamp() if m.last_seen else 0
        if ts >= cutoff:
            active.append(m.user_name)
    return {"members": sorted(set(active))}


@router.post("/w/party/ping")
async def watch_party_ping(payload: dict = Body(...)):
    token = (payload.get("token") or "").strip()
    user_name = (payload.get("user_name") or "").strip()
    if not token or not user_name:
        return {"status": "ignored"}
    member = await WatchPartyMember.find_one(
        WatchPartyMember.token == token,
        WatchPartyMember.user_name == user_name
    )
    if member:
        member.last_seen = datetime.now()
        await member.save()
    else:
        await WatchPartyMember(token=token, user_name=user_name).insert()
    return {"status": "ok"}


@router.get("/w/party/chat")
async def watch_party_chat(token: str):
    token = (token or "").strip()
    if not token:
        return {"messages": []}
    msgs = await WatchPartyMessage.find(WatchPartyMessage.token == token).sort("-created_at").limit(50).to_list()
    msgs.reverse()
    return {"messages": [{"user": m.user_name, "text": m.text, "ts": m.created_at.isoformat()} for m in msgs]}


@router.post("/w/party/chat")
async def watch_party_chat_post(payload: dict = Body(...)):
    token = (payload.get("token") or "").strip()
    user_name = (payload.get("user_name") or "").strip()
    text = (payload.get("text") or "").strip()
    if not token or not user_name or not text:
        return {"error": "Missing fields"}
    if len(text) > 400:
        return {"error": "Message too long"}
    msg = WatchPartyMessage(token=token, user_name=user_name, text=text)
    await msg.insert()
    return {"status": "ok"}


@router.post("/w/party/state")
async def watch_party_update(payload: dict = Body(...)):
    token = (payload.get("token") or "").strip()
    user_name = (payload.get("user_name") or "").strip()
    if not token or not user_name:
        return {"error": "Missing token or user"}
    position = float(payload.get("position") or 0)
    item_id = payload.get("item_id")
    is_playing = bool(payload.get("is_playing", True))

    now = datetime.now()
    party = await WatchParty.find_one(WatchParty.token == token)
    if not party:
        party = WatchParty(token=token, room_code=_generate_room_code(), host_name=user_name, host_last_seen=now, updated_at=now)
        await party.insert()
    else:
        if not getattr(party, "room_code", None):
            party.room_code = _generate_room_code()
        host_age = (now - party.host_last_seen).total_seconds() if party.host_last_seen else PARTY_HOST_TIMEOUT_SECONDS + 1
        if party.host_name != user_name and host_age <= PARTY_HOST_TIMEOUT_SECONDS:
            return {"status": "ignored", "reason": "not_host", "host_name": party.host_name}
        party.host_name = user_name

    party.position = position
    party.item_id = item_id
    party.is_playing = is_playing
    party.host_last_seen = now
    party.updated_at = now
    await party.save()
    return {"status": "ok", "host_name": party.host_name}

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
