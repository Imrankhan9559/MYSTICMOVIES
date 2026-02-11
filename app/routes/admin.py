from datetime import datetime
import re
import asyncio
import uuid
import json
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from beanie.operators import In, Or
from app.db.models import User, FileSystemItem, PlaybackProgress, TokenSetting, SiteSettings, ContentRequest
from app.routes.dashboard import get_current_user, _cast_ids, _clone_parts, _build_search_regex
from app.routes.content import refresh_tmdb_metadata, _parse_name, _tmdb_get
from app.core.config import settings
from app.core.telegram_bot import pool_status, reload_bot_pool, speed_test, _get_pool_tokens
from app.utils.file_utils import format_size

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

async def _site_settings() -> SiteSettings:
    row = await SiteSettings.find_one(SiteSettings.key == "main")
    if not row:
        row = SiteSettings(key="main")
        await row.insert()
    return row

async def _ensure_folder(owner_phone: str, name: str, parent_id: str | None, source: str = "catalog") -> FileSystemItem:
    existing = await FileSystemItem.find_one(
        FileSystemItem.is_folder == True,
        FileSystemItem.parent_id == parent_id,
        FileSystemItem.name == name,
        FileSystemItem.owner_phone == owner_phone
    )
    if existing:
        return existing
    folder = FileSystemItem(
        name=name,
        is_folder=True,
        parent_id=parent_id,
        owner_phone=owner_phone,
        source=source,
        catalog_status="published"
    )
    await folder.insert()
    return folder


async def _find_folder(owner_phone: str, name: str, parent_id: str | None) -> FileSystemItem | None:
    return await FileSystemItem.find_one(
        FileSystemItem.is_folder == True,
        FileSystemItem.parent_id == parent_id,
        FileSystemItem.name == name,
        FileSystemItem.owner_phone == owner_phone
    )


async def _cleanup_empty_tree(folder_id: str | None) -> None:
    if not folder_id:
        return
    folder = await FileSystemItem.get(folder_id)
    if not folder or not folder.is_folder:
        return
    # Clean children first
    children = await FileSystemItem.find(FileSystemItem.parent_id == str(folder.id)).to_list()
    for child in children:
        if child.is_folder:
            await _cleanup_empty_tree(str(child.id))
    # Re-check children after cleanup
    remaining = await FileSystemItem.find(FileSystemItem.parent_id == str(folder.id)).count()
    if remaining == 0:
        await folder.delete()


async def _cleanup_parents(folder_id: str | None) -> None:
    current_id = folder_id
    while current_id:
        folder = await FileSystemItem.get(current_id)
        if not folder or not folder.is_folder:
            return
        remaining = await FileSystemItem.find(FileSystemItem.parent_id == str(folder.id)).count()
        if remaining > 0:
            return
        parent_id = folder.parent_id
        await folder.delete()
        current_id = parent_id

def _quality_rank(q: str) -> int:
    order = {"2160P": 5, "1440P": 4, "1080P": 3, "720P": 2, "480P": 1, "380P": 0, "360P": 0, "HD": 0}
    return order.get((q or "").upper(), 0)

def _title_key(text: str) -> str:
    info = _parse_name(text or "")
    raw = info.get("title") or text or ""
    tokens = [t for t in (raw or "").split() if t]
    if len(tokens) > 1 and len(tokens[-1]) <= 2:
        tokens = tokens[:-1]
    key = " ".join(tokens) if tokens else raw
    return (key or "").strip().lower()

def _build_title_regex(title: str) -> str | None:
    tokens = [t for t in re.split(r"[^a-zA-Z0-9]+", (title or "").lower()) if t]
    if not tokens:
        return None
    return ".*".join(re.escape(token) for token in tokens)

def _clean_display_title(title: str) -> str:
    tokens = [t for t in (title or "").split() if t]
    if len(tokens) > 1 and len(tokens[-1]) <= 2:
        return " ".join(tokens[:-1])
    return title

def _summarize_group(group: dict) -> dict:
    items = group.get("items", [])
    total_size = sum(int(i.get("size") or 0) for i in items)
    qualities_set = {str(i.get("quality") or "").upper() for i in items if i.get("quality")}
    qualities = sorted(qualities_set, key=lambda q: (-_quality_rank(q), q))
    seasons_map: dict[int, dict] = {}
    if group.get("type") == "series":
        for item in items:
            season = int(item.get("season") or 1)
            episode = int(item.get("episode") or 0)
            entry = seasons_map.setdefault(season, {"episodes": set(), "qualities": set()})
            if episode:
                entry["episodes"].add(episode)
            quality = (item.get("quality") or "").upper()
            if quality:
                entry["qualities"].add(quality)
    seasons = []
    for season_num, entry in seasons_map.items():
        season_qualities = sorted(entry["qualities"], key=lambda q: (-_quality_rank(q), q))
        seasons.append({
            "season": season_num,
            "episode_count": len(entry["episodes"]),
            "qualities": season_qualities,
        })
    seasons.sort(key=lambda s: s["season"])

    group["file_count"] = len(items)
    group["total_size"] = total_size
    group["total_size_label"] = format_size(total_size)
    group["qualities"] = qualities
    group["seasons"] = seasons
    return group

async def _group_storage_suggestions() -> list[dict]:
    rows = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.source == "storage"
    ).sort("-created_at").to_list()
    groups: dict[tuple, dict] = {}
    for item in rows:
        status = (getattr(item, "catalog_status", "") or "").lower()
        if status in ("published", "used"):
            continue
        info = _parse_name(item.name or "")
        display_title = _clean_display_title((getattr(item, "title", "") or info["title"] or "").strip())
        if not display_title:
            continue
        ctype = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        year = (getattr(item, "year", "") or info["year"] or "").strip()
        key = (_title_key(getattr(item, "title", "") or item.name or ""), year, ctype)
        group = groups.setdefault(key, {
            "id": str(item.id),
            "title": display_title,
            "year": year,
            "type": ctype,
            "poster": getattr(item, "poster_url", "") or "",
            "backdrop": getattr(item, "backdrop_url", "") or "",
            "description": getattr(item, "description", "") or "",
            "genres": getattr(item, "genres", []) or [],
            "actors": getattr(item, "actors", []) or [],
            "director": getattr(item, "director", "") or "",
            "trailer_url": getattr(item, "trailer_url", "") or "",
            "trailer_key": getattr(item, "trailer_key", "") or "",
            "release_date": getattr(item, "release_date", "") or "",
            "items": []
        })
        quality = getattr(item, "quality", "") or info["quality"]
        season = getattr(item, "season", None) or info["season"]
        episode = getattr(item, "episode", None) or info["episode"]
        group["items"].append({
            "id": str(item.id),
            "name": item.name,
            "size": item.size or 0,
            "size_label": format_size(item.size or 0),
            "quality": quality,
            "season": season,
            "episode": episode,
            "episode_title": getattr(item, "episode_title", "") or ""
        })
    # sort items by quality/episode
    for g in groups.values():
        g["items"].sort(key=lambda x: (x.get("season") or 0, x.get("episode") or 0, x.get("quality") or ""))
        _summarize_group(g)
    return sorted(groups.values(), key=lambda g: g["title"].lower())

async def _group_published_catalog() -> list[dict]:
    rows = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published"
    ).sort("-created_at").to_list()
    groups: dict[tuple, dict] = {}
    for item in rows:
        info = _parse_name(item.name or "")
        display_title = _clean_display_title((getattr(item, "series_title", "") or getattr(item, "title", "") or info["title"] or "").strip())
        if not display_title:
            continue
        ctype = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        year = (getattr(item, "year", "") or info["year"] or "").strip()
        key = (_title_key(getattr(item, "title", "") or item.name or ""), year, ctype)
        group = groups.setdefault(key, {
            "title": display_title,
            "year": year,
            "type": ctype,
            "items": []
        })
        quality = getattr(item, "quality", "") or info["quality"]
        season = getattr(item, "season", None) or info["season"]
        episode = getattr(item, "episode", None) or info["episode"]
        group["items"].append({
            "id": str(item.id),
            "name": item.name,
            "size": item.size or 0,
            "size_label": format_size(item.size or 0),
            "quality": quality,
            "season": season,
            "episode": episode
        })
    for g in groups.values():
        g["items"].sort(key=lambda x: (x.get("season") or 0, x.get("episode") or 0, x.get("quality") or ""))
        _summarize_group(g)
    return sorted(groups.values(), key=lambda g: g["title"].lower())

@router.get("/admin")
async def admin_redirect(request: Request):
    return RedirectResponse("/dashboard")

@router.get("/main-control")
async def main_control_alias(request: Request):
    return RedirectResponse("/dashboard")

@router.get("/dashboard")
async def main_control(request: Request):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    total_users = await User.count()
    total_files = await FileSystemItem.find(FileSystemItem.is_folder == False).count()
    all_users = await User.find_all().to_list()
    pending_users = await User.find(User.status == "pending").sort("-requested_at").to_list()

    recent_progress = await PlaybackProgress.find_all().sort("-updated_at").limit(50).to_list()
    # Map item_id -> name for display
    item_ids = list({p.item_id for p in recent_progress if getattr(p, "item_id", None)})
    items_map = {}
    if item_ids:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
        items_map = {str(i.id): i.name for i in items}
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    link_token = token_doc.value if token_doc else ""
    bots = await pool_status()
    pool_tokens = ", ".join(_get_pool_tokens())
    site = await _site_settings()
    pending_requests = await ContentRequest.find(ContentRequest.status == "pending").sort("-created_at").limit(50).to_list()
    content_items = await FileSystemItem.find(FileSystemItem.is_folder == False).sort("-created_at").limit(120).to_list()

    tmdb_configured = bool(getattr(settings, "TMDB_API_KEY", ""))
    tmdb_status = (request.query_params.get("tmdb") or "").strip().lower()

    published_groups = await _group_published_catalog()
    published_movies = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "movie"
    ).count()
    published_series = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "series"
    ).count()
    pending_storage = await FileSystemItem.find({
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }).count()

    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": None, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status,
        "published_groups": published_groups,
        "published_movies": published_movies,
        "published_series": published_series,
        "pending_storage": pending_storage
    })

@router.get("/dashboard/add-content")
async def add_content(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    tmdb_configured = bool(getattr(settings, "TMDB_API_KEY", ""))
    pending_storage = await FileSystemItem.find({
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }).count()
    published_movies = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "movie"
    ).count()
    published_series = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "series"
    ).count()

    storage_items = await FileSystemItem.find({
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }).sort("-created_at").limit(500).to_list()

    files = []
    for item in storage_items:
        info = _parse_name(item.name or "")
        catalog_type = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        files.append({
            "id": str(item.id),
            "name": item.name,
            "size": item.size or 0,
            "size_label": format_size(item.size or 0),
            "quality": getattr(item, "quality", "") or info["quality"],
            "season": getattr(item, "season", None) or info["season"],
            "episode": getattr(item, "episode", None) or info["episode"],
            "type": catalog_type,
        })

    site = await _site_settings()
    return templates.TemplateResponse("admin_add_content.html", {
        "request": request,
        "user": user,
        "is_admin": True,
        "tmdb_configured": tmdb_configured,
        "pending_storage": pending_storage,
        "published_movies": published_movies,
        "published_series": published_series,
        "files": files,
        "site": site
    })

@router.get("/dashboard/publish-content")
async def publish_content(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    published_groups = await _group_published_catalog()
    site = await _site_settings()
    return templates.TemplateResponse("publish_content.html", {
        "request": request,
        "user": user,
        "is_admin": True,
        "published_groups": published_groups,
        "site": site
    })

@router.get("/dashboard/publish-content/edit/{group_id}")
async def publish_content_edit(request: Request, group_id: str):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_item = await FileSystemItem.get(group_id)
    if not base_item or base_item.catalog_status != "published":
        return RedirectResponse("/dashboard/publish-content", status_code=303)

    group_type = (getattr(base_item, "catalog_type", "") or "movie").strip().lower()
    group_title = (base_item.title or base_item.series_title or "").strip()
    if not group_title:
        return RedirectResponse("/dashboard/publish-content", status_code=303)

    items = await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        Or(FileSystemItem.title == group_title, FileSystemItem.series_title == group_title)
    ).to_list()

    group = {
        "id": str(base_item.id),
        "title": group_title,
        "year": base_item.year or "",
        "type": group_type,
        "poster": base_item.poster_url or "",
        "backdrop": base_item.backdrop_url or "",
        "description": base_item.description or "",
        "genres": base_item.genres or [],
        "actors": base_item.actors or [],
        "director": base_item.director or "",
        "trailer_url": base_item.trailer_url or "",
        "trailer_key": base_item.trailer_key or "",
        "release_date": base_item.release_date or "",
        "items": [],
    }
    for item in items:
        info = _parse_name(item.name or "")
        quality = getattr(item, "quality", "") or info["quality"]
        season = getattr(item, "season", None) or info["season"]
        episode = getattr(item, "episode", None) or info["episode"]
        group["items"].append({
            "id": str(item.id),
            "name": item.name,
            "size": item.size or 0,
            "size_label": format_size(item.size or 0),
            "quality": quality,
            "season": season,
            "episode": episode,
            "episode_title": getattr(item, "episode_title", "") or ""
        })
    _summarize_group(group)
    site = await _site_settings()
    return templates.TemplateResponse("publish_content_edit.html", {
        "request": request,
        "user": user,
        "is_admin": True,
        "group": group,
        "site": site,
        "return_to": f"/dashboard/publish-content/edit/{group_id}"
    })

@router.post("/dashboard/publish-content/save")
@router.post("/dashboard/publish-content/update")
async def publish_content_update(
    request: Request,
    group_id: str = Form(""),
    group_title: str = Form(""),
    group_year: str = Form(""),
    group_type: str = Form("movie"),
    title: str = Form(""),
    year: str = Form(""),
    description: str = Form(""),
    genres: str = Form(""),
    actors: str = Form(""),
    director: str = Form(""),
    trailer_url: str = Form(""),
    trailer_key: str = Form(""),
    poster_url: str = Form(""),
    backdrop_url: str = Form(""),
    release_date: str = Form(""),
    return_to: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    base_item = await FileSystemItem.get(group_id) if group_id else None
    if base_item:
        group_title = (base_item.title or base_item.series_title or group_title or "").strip()
        group_type = (getattr(base_item, "catalog_type", "") or group_type or "movie").strip().lower()
        group_year = (base_item.year or group_year or "").strip()
    group_title = (group_title or "").strip()
    group_type = (group_type or "movie").strip().lower()
    group_year = (group_year or "").strip()
    if not group_title:
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

    new_title = (title or "").strip() or group_title
    new_year = (year or "").strip() or group_year
    desc = (description or "").strip()
    genres_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
    actors_list = [a.strip() for a in (actors or "").split(",") if a.strip()]
    director = (director or "").strip()
    trailer_url = (trailer_url or "").strip()
    trailer_key = (trailer_key or "").strip()
    poster_url = (poster_url or "").strip()
    backdrop_url = (backdrop_url or "").strip()
    release_date = (release_date or "").strip()

    items = await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        Or(FileSystemItem.title == group_title, FileSystemItem.series_title == group_title)
    ).to_list()
    for item in items:
        item.title = new_title
        if group_type == "series":
            item.series_title = new_title
        item.year = new_year
        item.description = desc
        item.genres = genres_list
        item.actors = actors_list
        item.director = director
        item.trailer_url = trailer_url
        item.trailer_key = trailer_key
        item.poster_url = poster_url
        item.backdrop_url = backdrop_url
        item.release_date = release_date
        await item.save()

    # Rename catalog folder if title changed
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if admin_phone and new_title != group_title:
        root_name = "Movies" if group_type == "movie" else "Web Series"
        root = await _find_folder(admin_phone, root_name, None)
        if root:
            target_folder = await _find_folder(admin_phone, group_title, str(root.id))
            if target_folder:
                target_folder.name = new_title
                await target_folder.save()

    return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

@router.post("/dashboard/publish-content/add-files")
async def publish_content_add_files(
    request: Request,
    group_id: str = Form(""),
    item_ids: str = Form(""),
    overrides: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not group_id:
        return RedirectResponse("/dashboard/publish-content", status_code=303)
    base_item = await FileSystemItem.get(group_id)
    if not base_item or base_item.catalog_status != "published":
        return RedirectResponse("/dashboard/publish-content", status_code=303)

    raw_ids = [i.strip() for i in (item_ids or "").split(",") if i.strip()]
    if not raw_ids:
        return RedirectResponse(f"/dashboard/publish-content/edit/{group_id}", status_code=303)
    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(raw_ids))).to_list()
    if not items:
        return RedirectResponse(f"/dashboard/publish-content/edit/{group_id}", status_code=303)

    override_map = {}
    if overrides:
        try:
            override_map = json.loads(overrides)
        except Exception:
            override_map = {}

    catalog_type = (getattr(base_item, "catalog_type", "") or "movie").strip().lower()
    title = (base_item.title or base_item.series_title or "").strip()
    year = base_item.year or ""
    desc = base_item.description or ""
    genres_list = base_item.genres or []
    actors_list = base_item.actors or []
    director = base_item.director or ""
    trailer_url = base_item.trailer_url or ""
    poster_url = base_item.poster_url or ""
    backdrop_url = base_item.backdrop_url or ""
    trailer_key = base_item.trailer_key or ""
    release_date = base_item.release_date or ""
    cast_profiles_list = base_item.cast_profiles or []
    tmdb_id_val = getattr(base_item, "tmdb_id", None)

    await _publish_items(
        items=items,
        catalog_type=catalog_type,
        title=title,
        year=year,
        desc=desc,
        genres_list=genres_list,
        actors_list=actors_list,
        director=director,
        trailer_url=trailer_url,
        release_date=release_date,
        poster_url=poster_url,
        backdrop_url=backdrop_url,
        trailer_key=trailer_key,
        cast_profiles=cast_profiles_list,
        tmdb_id=tmdb_id_val,
        overrides=override_map
    )

    return RedirectResponse(f"/dashboard/publish-content/edit/{group_id}", status_code=303)

@router.post("/dashboard/publish-content/delete")
async def publish_content_delete(
    request: Request,
    group_id: str = Form(""),
    group_title: str = Form(""),
    group_type: str = Form("movie"),
    return_to: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    base_item = await FileSystemItem.get(group_id) if group_id else None
    if base_item:
        group_title = (base_item.title or base_item.series_title or group_title or "").strip()
        group_type = (getattr(base_item, "catalog_type", "") or group_type or "movie").strip().lower()
    group_title = (group_title or "").strip()
    group_type = (group_type or "movie").strip().lower()
    if not group_title:
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

    await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        Or(FileSystemItem.title == group_title, FileSystemItem.series_title == group_title)
    ).delete()

    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if admin_phone:
        root_name = "Movies" if group_type == "movie" else "Web Series"
        root = await _find_folder(admin_phone, root_name, None)
        if root:
            target_folder = await _find_folder(admin_phone, group_title, str(root.id))
            if target_folder:
                await _cleanup_empty_tree(str(target_folder.id))

    return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

@router.post("/dashboard/publish-content/delete-file")
async def publish_content_delete_file(
    request: Request,
    published_id: str = Form(""),
    return_to: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not published_id:
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)
    item = await FileSystemItem.get(published_id)
    if not item or item.catalog_status != "published":
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)
    parent_id = item.parent_id
    await item.delete()
    await _cleanup_parents(parent_id)
    return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

@router.post("/dashboard/publish-content/update-file")
async def publish_content_update_file(
    request: Request,
    published_id: str = Form(""),
    storage_id: str = Form(""),
    quality: str = Form(""),
    season: str = Form(""),
    episode: str = Form(""),
    episode_title: str = Form(""),
    return_to: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not published_id:
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)
    item = await FileSystemItem.get(published_id)
    if not item or item.catalog_status != "published":
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

    item.quality = (quality or "").strip()
    if (season or "").strip():
        try:
            item.season = int(season)
        except Exception:
            pass
    else:
        item.season = None
    if (episode or "").strip():
        try:
            item.episode = int(episode)
        except Exception:
            pass
    else:
        item.episode = None
    item.episode_title = (episode_title or "").strip()

    if storage_id:
        storage_item = await FileSystemItem.get(storage_id)
        if storage_item and storage_item.source == "storage":
            item.name = storage_item.name
            item.size = storage_item.size or 0
            item.mime_type = storage_item.mime_type
            item.parts = _clone_parts(storage_item.parts)
            storage_item.catalog_status = "used"
            await storage_item.save()

    # If series metadata changed, ensure the file is in the correct season/quality folder
    if item.catalog_type == "series":
        admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
        if admin_phone:
            root = await _find_folder(admin_phone, "Web Series", None)
            if root:
                series_title = item.series_title or item.title or ""
                series_folder = await _find_folder(admin_phone, series_title, str(root.id))
                if series_folder:
                    season_val = item.season or 1
                    quality_val = (item.quality or "HD").strip() or "HD"
                    season_folder = await _ensure_folder(admin_phone, f"Season {season_val}", str(series_folder.id))
                    quality_folder = await _ensure_folder(admin_phone, quality_val, str(season_folder.id))
                    item.parent_id = str(quality_folder.id)

    await item.save()
    return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

@router.get("/dashboard/storage/search")
async def admin_storage_search(request: Request, q: str = "", offset: int = 0, limit: int = 200):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    q = (q or "").strip()
    try:
        offset = max(int(offset), 0)
    except Exception:
        offset = 0
    try:
        limit = int(limit)
    except Exception:
        limit = 200
    limit = max(20, min(limit, 500))

    query: dict = {
        "is_folder": False
    }
    if q:
        search_regex = _build_search_regex(q)
        if not search_regex:
            return {"items": [], "has_more": False}
        query["name"] = {"$regex": search_regex, "$options": "i"}

    sort_field = "name" if q else "-created_at"
    items = await FileSystemItem.find(query).sort(sort_field).skip(offset).limit(limit + 1).to_list()
    has_more = len(items) > limit
    if has_more:
        items = items[:limit]
    payload = [{
        "id": str(item.id),
        "name": item.name,
        "size": item.size or 0,
        "size_label": format_size(item.size or 0)
    } for item in items]
    return {"items": payload, "has_more": has_more}

@router.get("/main-control/tmdb/lookup")
@router.get("/dashboard/tmdb/lookup")
async def main_control_tmdb_lookup(q: str = "", content_type: str = "movie"):
    if not settings.TMDB_API_KEY:
        return {"ok": False, "error": "TMDB_API_KEY missing"}
    q = (q or "").strip()
    if not q:
        return {"ok": False, "error": "Missing query"}
    content_type = (content_type or "movie").strip().lower()
    try:
        from app.routes.content import _tmdb_search, _tmdb_details
        search = await _tmdb_search(q, "", content_type == "series")
        results = (search or {}).get("results") or []
        if not results:
            return {"ok": False, "error": "No results"}
        pick = results[0]
        tmdb_id = pick.get("id")
        if not tmdb_id:
            return {"ok": False, "error": "Invalid TMDB result"}
        details = await _tmdb_details(tmdb_id, content_type == "series")
        if not details:
            return {"ok": False, "error": "TMDB details not found"}

        title = details.get("name") if content_type == "series" else details.get("title")
        overview = details.get("overview") or ""
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
        credits = details.get("credits") or {}
        cast = [c.get("name") for c in (credits.get("cast") or [])[:8] if c.get("name")]
        director = ""
        for crew in credits.get("crew") or []:
            if crew.get("job") == "Director":
                director = crew.get("name") or ""
                break
        trailer = ""
        for v in details.get("videos", {}).get("results", []):
            if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser"):
                trailer = f"https://www.youtube.com/watch?v={v.get('key')}"
                break

        return {
            "ok": True,
            "title": title or q,
            "year": year,
            "description": overview,
            "genres": genres,
            "actors": cast,
            "director": director,
            "trailer_url": trailer
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/dashboard/tmdb/search")
async def tmdb_search(q: str = "", content_type: str = "movie"):
    if not settings.TMDB_API_KEY:
        return {"ok": False, "error": "TMDB_API_KEY missing", "results": []}
    q = (q or "").strip()
    if not q:
        return {"ok": False, "error": "Missing query", "results": []}
    content_type = (content_type or "movie").strip().lower()
    try:
        from app.routes.content import _tmdb_search
        search = await _tmdb_search(q, "", content_type == "series")
        results = (search or {}).get("results") or []
        poster_base = "https://image.tmdb.org/t/p/w185"
        payload = []
        for row in results[:20]:
            title = row.get("name") if content_type == "series" else row.get("title")
            year = (row.get("first_air_date") or row.get("release_date") or "")[:4]
            poster_path = row.get("poster_path")
            payload.append({
                "id": row.get("id"),
                "title": title,
                "year": year,
                "overview": row.get("overview") or "",
                "poster": poster_base + poster_path if poster_path else ""
            })
        return {"ok": True, "results": payload}
    except Exception as e:
        return {"ok": False, "error": str(e), "results": []}

@router.get("/dashboard/tmdb/details")
async def tmdb_details(tmdb_id: int, content_type: str = "movie"):
    if not settings.TMDB_API_KEY:
        return {"ok": False, "error": "TMDB_API_KEY missing"}
    content_type = (content_type or "movie").strip().lower()
    try:
        from app.routes.content import _tmdb_details
        details = await _tmdb_details(tmdb_id, content_type == "series")
        if not details:
            return {"ok": False, "error": "TMDB details not found"}
        title = details.get("name") if content_type == "series" else details.get("title")
        overview = details.get("overview") or ""
        release_date = (details.get("release_date") or details.get("first_air_date") or "")
        year = release_date[:4] if release_date else ""
        genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
        credits = details.get("credits") or {}
        cast_rows = credits.get("cast") or []
        cast = [c.get("name") for c in cast_rows[:12] if c.get("name")]
        cast_profiles = []
        profile_base = "https://image.tmdb.org/t/p/w185"
        for c in cast_rows[:12]:
            name = c.get("name") or ""
            if not name:
                continue
            role = c.get("character") or ""
            profile_path = c.get("profile_path")
            image = profile_base + profile_path if profile_path else ""
            cast_profiles.append({"name": name, "role": role, "image": image})
        director = ""
        for crew in credits.get("crew") or []:
            if crew.get("job") == "Director":
                director = crew.get("name") or ""
                break
        trailer = ""
        trailer_key = ""
        for v in details.get("videos", {}).get("results", []):
            if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser"):
                trailer_key = v.get("key") or ""
                trailer = f"https://www.youtube.com/watch?v={trailer_key}" if trailer_key else ""
                break
        poster = details.get("poster_path") or ""
        backdrop = details.get("backdrop_path") or ""
        poster_url = f"https://image.tmdb.org/t/p/w780{poster}" if poster else ""
        backdrop_url = f"https://image.tmdb.org/t/p/w1280{backdrop}" if backdrop else ""
        return {
            "ok": True,
            "title": title,
            "year": year,
            "release_date": release_date,
            "description": overview,
            "genres": genres,
            "actors": cast,
            "cast_profiles": cast_profiles,
            "director": director,
            "trailer_url": trailer,
            "trailer_key": trailer_key,
            "poster": poster_url,
            "backdrop": backdrop_url,
            "tmdb_id": tmdb_id
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/dashboard/tmdb/seasons")
async def tmdb_seasons(tmdb_id: int):
    if not settings.TMDB_API_KEY:
        return {"ok": False, "error": "TMDB_API_KEY missing", "seasons": []}
    try:
        details = await _tmdb_get(f"/tv/{tmdb_id}", {})
        seasons = []
        for s in details.get("seasons", []) or []:
            season_no = s.get("season_number")
            if season_no is None:
                continue
            season_details = await _tmdb_get(f"/tv/{tmdb_id}/season/{season_no}", {})
            episodes = []
            for ep in season_details.get("episodes", []) or []:
                episodes.append({
                    "episode": ep.get("episode_number"),
                    "name": ep.get("name") or ""
                })
            seasons.append({
                "season": season_no,
                "name": s.get("name") or f"Season {season_no}",
                "episode_count": s.get("episode_count") or len(episodes),
                "episodes": episodes
            })
        return {"ok": True, "seasons": seasons}
    except Exception as e:
        return {"ok": False, "error": str(e), "seasons": []}

async def _publish_items(
    items: list[FileSystemItem],
    catalog_type: str,
    title: str,
    year: str,
    desc: str,
    genres_list: list[str],
    actors_list: list[str],
    director: str,
    trailer_url: str,
    poster_url: str = "",
    backdrop_url: str = "",
    trailer_key: str = "",
    cast_profiles: list | None = None,
    release_date: str = "",
    tmdb_id: int | None = None,
    overrides: dict | None = None
) -> None:
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if not admin_phone:
        return
    catalog_type = (catalog_type or "movie").strip().lower()

    overrides = overrides or {}
    cast_profiles = cast_profiles or []
    release_date = (release_date or "").strip()

    if catalog_type == "movie":
        root = await _ensure_folder(admin_phone, "Movies", None)
        movie_folder = await _ensure_folder(admin_phone, title, str(root.id))
        for item in items:
            existing = await FileSystemItem.find_one(
                FileSystemItem.parent_id == str(movie_folder.id),
                FileSystemItem.is_folder == False,
                FileSystemItem.name == item.name,
                FileSystemItem.size == item.size
            )
            if existing:
                continue
            info = _parse_name(item.name or "")
            override = overrides.get(str(item.id), {}) or {}
            quality = (override.get("quality") or getattr(item, "quality", "") or info["quality"] or "HD").strip()
            new_file = FileSystemItem(
                name=item.name,
                is_folder=False,
                parent_id=str(movie_folder.id),
                owner_phone=admin_phone,
                size=item.size,
                mime_type=item.mime_type,
                source="catalog",
                catalog_status="published",
                catalog_type="movie",
                title=title,
                year=year,
                quality=quality,
                description=desc,
                release_date=release_date,
                genres=genres_list,
                actors=actors_list,
                director=director,
                trailer_url=trailer_url,
                trailer_key=trailer_key,
                poster_url=poster_url,
                backdrop_url=backdrop_url,
                cast_profiles=cast_profiles,
                tmdb_id=tmdb_id,
                parts=_clone_parts(item.parts)
            )
            await new_file.insert()
    else:
        root = await _ensure_folder(admin_phone, "Web Series", None)
        series_folder = await _ensure_folder(admin_phone, title, str(root.id))
        for item in items:
            info = _parse_name(item.name or "")
            override = overrides.get(str(item.id), {}) or {}
            season_val = override.get("season") or getattr(item, "season", None) or info["season"]
            episode_val = override.get("episode") or getattr(item, "episode", None) or info["episode"]
            episode_title = (override.get("episode_title") or override.get("title") or getattr(item, "episode_title", "") or "").strip()
            try:
                season = int(season_val) if season_val else 1
            except Exception:
                season = 1
            try:
                episode = int(episode_val) if episode_val else 1
            except Exception:
                episode = 1
            quality = (override.get("quality") or getattr(item, "quality", "") or info["quality"] or "HD").strip()
            season_folder = await _ensure_folder(admin_phone, f"Season {season}", str(series_folder.id))
            quality_folder = await _ensure_folder(admin_phone, quality, str(season_folder.id))
            existing = await FileSystemItem.find_one(
                FileSystemItem.parent_id == str(quality_folder.id),
                FileSystemItem.is_folder == False,
                FileSystemItem.name == item.name,
                FileSystemItem.size == item.size
            )
            if existing:
                continue
            new_file = FileSystemItem(
                name=item.name,
                is_folder=False,
                parent_id=str(quality_folder.id),
                owner_phone=admin_phone,
                size=item.size,
                mime_type=item.mime_type,
                source="catalog",
                catalog_status="published",
                catalog_type="series",
                title=title,
                series_title=title,
                year=year,
                quality=quality,
                season=season,
                episode=episode,
                episode_title=episode_title,
                description=desc,
                release_date=release_date,
                genres=genres_list,
                actors=actors_list,
                director=director,
                trailer_url=trailer_url,
                trailer_key=trailer_key,
                poster_url=poster_url,
                backdrop_url=backdrop_url,
                cast_profiles=cast_profiles,
                tmdb_id=tmdb_id,
                parts=_clone_parts(item.parts)
            )
            await new_file.insert()

    for item in items:
        try:
            override = overrides.get(str(item.id), {}) or {}
            if override.get("quality"):
                item.quality = override.get("quality")
            if override.get("season"):
                try:
                    item.season = int(override.get("season"))
                except Exception:
                    pass
            if override.get("episode"):
                try:
                    item.episode = int(override.get("episode"))
                except Exception:
                    pass
            item.catalog_status = "used"
            if override.get("episode_title"):
                item.episode_title = override.get("episode_title")
            await item.save()
        except Exception:
            pass

@router.post("/main-control/publish")
@router.post("/dashboard/publish")
async def main_control_publish(
    request: Request,
    item_ids: str = Form(""),
    catalog_type: str = Form("movie"),
    title: str = Form(""),
    year: str = Form(""),
    description: str = Form(""),
    genres: str = Form(""),
    actors: str = Form(""),
    director: str = Form(""),
    trailer_url: str = Form(""),
    release_date: str = Form(""),
    tmdb_id: str = Form(""),
    cast_profiles: str = Form(""),
    poster_url: str = Form(""),
    backdrop_url: str = Form(""),
    trailer_key: str = Form(""),
    overrides: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    raw_ids = [i.strip() for i in (item_ids or "").split(",") if i.strip()]
    if not raw_ids:
        return RedirectResponse("/dashboard", status_code=303)
    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(raw_ids))).to_list()
    if not items:
        return RedirectResponse("/dashboard", status_code=303)

    catalog_type = (catalog_type or "movie").strip().lower()
    title = (title or "").strip()
    if not title:
        title = _parse_name(items[0].name or "").get("title") or "Untitled"
    year = (year or "").strip()
    desc = (description or "").strip()
    genres_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
    actors_list = [a.strip() for a in (actors or "").split(",") if a.strip()]
    director = (director or "").strip()
    trailer_url = (trailer_url or "").strip()

    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if not admin_phone:
        return RedirectResponse("/dashboard", status_code=303)
    override_map = {}
    if overrides:
        try:
            override_map = json.loads(overrides)
        except Exception:
            override_map = {}

    cast_profiles_list = []
    if cast_profiles:
        try:
            cast_profiles_list = json.loads(cast_profiles)
        except Exception:
            cast_profiles_list = []
    try:
        tmdb_id_val = int(tmdb_id) if tmdb_id else None
    except Exception:
        tmdb_id_val = None

    await _publish_items(
        items=items,
        catalog_type=catalog_type,
        title=title,
        year=year,
        desc=desc,
        genres_list=genres_list,
        actors_list=actors_list,
        director=director,
        trailer_url=trailer_url,
        release_date=release_date,
        poster_url=(poster_url or "").strip(),
        backdrop_url=(backdrop_url or "").strip(),
        trailer_key=(trailer_key or "").strip(),
        cast_profiles=cast_profiles_list,
        tmdb_id=tmdb_id_val,
        overrides=override_map
    )

    return RedirectResponse("/dashboard", status_code=303)

@router.post("/dashboard/publish_by_title")
async def publish_by_title(
    request: Request,
    title: str = Form(""),
    catalog_type: str = Form("movie"),
    year: str = Form(""),
    description: str = Form(""),
    genres: str = Form(""),
    actors: str = Form(""),
    director: str = Form(""),
    trailer_url: str = Form(""),
    release_date: str = Form(""),
    tmdb_id: str = Form(""),
    cast_profiles: str = Form(""),
    poster_url: str = Form(""),
    backdrop_url: str = Form(""),
    trailer_key: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    title = (title or "").strip()
    if not title:
        return RedirectResponse("/dashboard", status_code=303)

    catalog_type = (catalog_type or "movie").strip().lower()
    year = (year or "").strip()
    desc = (description or "").strip()
    genres_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
    actors_list = [a.strip() for a in (actors or "").split(",") if a.strip()]
    director = (director or "").strip()
    trailer_url = (trailer_url or "").strip()

    title_key = _title_key(title)
    pattern = _build_title_regex(title) or ""
    query: dict = {
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }
    if pattern:
        query["name"] = {"$regex": pattern, "$options": "i"}

    candidates = await FileSystemItem.find(query).to_list()
    matched: list[FileSystemItem] = []
    for item in candidates:
        info = _parse_name(item.name or "")
        if _title_key(item.name or "") != title_key:
            continue
        item_type = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        if item_type != catalog_type:
            continue
        if year:
            item_year = (getattr(item, "year", "") or info.get("year") or "").strip()
            if item_year and item_year != year:
                continue
        matched.append(item)

    if not matched:
        return RedirectResponse("/dashboard?publish=not_found", status_code=303)

    cast_profiles_list = []
    if cast_profiles:
        try:
            cast_profiles_list = json.loads(cast_profiles)
        except Exception:
            cast_profiles_list = []
    try:
        tmdb_id_val = int(tmdb_id) if tmdb_id else None
    except Exception:
        tmdb_id_val = None

    await _publish_items(
        items=matched,
        catalog_type=catalog_type,
        title=title,
        year=year,
        desc=desc,
        genres_list=genres_list,
        actors_list=actors_list,
        director=director,
        trailer_url=trailer_url,
        release_date=release_date,
        poster_url=(poster_url or "").strip(),
        backdrop_url=(backdrop_url or "").strip(),
        trailer_key=(trailer_key or "").strip(),
        cast_profiles=cast_profiles_list,
        tmdb_id=tmdb_id_val
    )
    return RedirectResponse("/dashboard?publish=ok", status_code=303)

@router.post("/main-control/update-metadata")
@router.post("/dashboard/update-metadata")
async def main_control_update_metadata(
    request: Request,
    group_title: str = Form(""),
    group_year: str = Form(""),
    group_type: str = Form("movie"),
    title: str = Form(""),
    year: str = Form(""),
    description: str = Form(""),
    genres: str = Form(""),
    actors: str = Form(""),
    director: str = Form(""),
    trailer_url: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    group_title = (group_title or "").strip()
    group_type = (group_type or "movie").strip().lower()
    group_year = (group_year or "").strip()
    if not group_title:
        return RedirectResponse("/dashboard", status_code=303)

    new_title = (title or "").strip() or group_title
    new_year = (year or "").strip() or group_year
    desc = (description or "").strip()
    genres_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
    actors_list = [a.strip() for a in (actors or "").split(",") if a.strip()]
    director = (director or "").strip()
    trailer_url = (trailer_url or "").strip()

    items = await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        FileSystemItem.title == group_title
    ).to_list()
    for item in items:
        item.title = new_title
        if group_type == "series":
            item.series_title = new_title
        item.year = new_year
        if desc:
            item.description = desc
        if genres_list:
            item.genres = genres_list
        if actors_list:
            item.actors = actors_list
        if director:
            item.director = director
        if trailer_url:
            item.trailer_url = trailer_url
        await item.save()

    return RedirectResponse("/dashboard", status_code=303)

@router.post("/main-control/delete")
@router.post("/dashboard/delete")
async def main_control_delete_group(
    request: Request,
    group_title: str = Form(""),
    group_type: str = Form("movie")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    group_title = (group_title or "").strip()
    group_type = (group_type or "movie").strip().lower()
    if not group_title:
        return RedirectResponse("/dashboard", status_code=303)
    await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        FileSystemItem.title == group_title
    ).delete()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/token/regenerate")
async def regenerate_link_token(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    new_val = str(uuid.uuid4())
    if token_doc:
        token_doc.value = new_val
        token_doc.updated_at = datetime.now()
        await token_doc.save()
    else:
        token_doc = TokenSetting(key="link_token", value=new_val)
        await token_doc.insert()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/bots/update_tokens")
async def admin_update_tokens(request: Request, bot_tokens: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    tokens = [t.strip() for t in bot_tokens.replace("\n", ",").split(",") if t.strip()]
    await reload_bot_pool(tokens)
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/bots/speedtest")
async def admin_speed_test(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    try:
        result = await speed_test()
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    # rebuild page with result
    total_users = await User.count()
    total_files = await FileSystemItem.find(FileSystemItem.is_folder == False).count()
    all_users = await User.find_all().to_list()
    pending_users = await User.find(User.status == "pending").sort("-requested_at").to_list()
    recent_progress = await PlaybackProgress.find_all().sort("-updated_at").limit(50).to_list()
    item_ids = list({p.item_id for p in recent_progress if getattr(p, "item_id", None)})
    items_map = {}
    if item_ids:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
        items_map = {str(i.id): i.name for i in items}
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    link_token = token_doc.value if token_doc else ""
    bots = await pool_status()
    pool_tokens = ", ".join(_get_pool_tokens())
    site = await _site_settings()
    pending_requests = await ContentRequest.find(ContentRequest.status == "pending").sort("-created_at").limit(50).to_list()
    content_items = await FileSystemItem.find(FileSystemItem.is_folder == False).sort("-created_at").limit(120).to_list()
    tmdb_configured = bool(getattr(settings, "TMDB_API_KEY", ""))
    tmdb_status = (request.query_params.get("tmdb") or "").strip().lower()
    published_groups = await _group_published_catalog()
    published_movies = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "movie"
    ).count()
    published_series = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == "series"
    ).count()
    pending_storage = await FileSystemItem.find({
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }).count()
    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": result, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status,
        "published_groups": published_groups,
        "published_movies": published_movies,
        "published_series": published_series,
        "pending_storage": pending_storage
    })

@router.post("/admin/tmdb/refresh")
async def admin_refresh_tmdb(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not getattr(settings, "TMDB_API_KEY", ""):
        return RedirectResponse("/dashboard?tmdb=missing", status_code=303)
    # Fire-and-forget; refresh can take time for large catalogs.
    asyncio.create_task(refresh_tmdb_metadata(limit=None))
    return RedirectResponse("/dashboard?tmdb=refresh_started", status_code=303)

@router.get("/settings")
async def admin_settings_redirect(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    return RedirectResponse("/dashboard")

@router.post("/admin/site/save")
async def save_site_settings(
    request: Request,
    site_name: str = Form("mysticmovies"),
    accent_color: str = Form("#facc15"),
    bg_color: str = Form("#070b12"),
    card_color: str = Form("#111827"),
    hero_title: str = Form("Watch Movies & Series"),
    hero_subtitle: str = Form("Stream, download, and send to Telegram in one place."),
    hero_cta_text: str = Form("Browse Content"),
    hero_cta_link: str = Form("/content"),
    footer_text: str = Form("MysticMovies")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    site = await _site_settings()
    site.site_name = (site_name or "mysticmovies").strip()
    site.accent_color = (accent_color or "#facc15").strip()
    site.bg_color = (bg_color or "#070b12").strip()
    site.card_color = (card_color or "#111827").strip()
    site.hero_title = (hero_title or "").strip()
    site.hero_subtitle = (hero_subtitle or "").strip()
    site.hero_cta_text = (hero_cta_text or "").strip()
    site.hero_cta_link = (hero_cta_link or "/content").strip()
    site.footer_text = (footer_text or "MysticMovies").strip()
    site.updated_at = datetime.now()
    await site.save()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/content/update")
async def update_content_metadata(
    request: Request,
    item_id: str = Form(...),
    catalog_type: str = Form("movie"),
    title: str = Form(""),
    description: str = Form(""),
    year: str = Form(""),
    genres: str = Form(""),
    actors: str = Form(""),
    director: str = Form(""),
    trailer_url: str = Form(""),
    series_title: str = Form(""),
    season: str = Form(""),
    episode: str = Form(""),
    quality: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    item = await FileSystemItem.get(item_id)
    if not item:
        raise HTTPException(404)
    item.catalog_type = (catalog_type or "movie").strip().lower()
    item.title = (title or "").strip()
    item.description = (description or "").strip()
    item.year = (year or "").strip()
    item.genres = [g.strip() for g in (genres or "").split(",") if g.strip()]
    item.actors = [a.strip() for a in (actors or "").split(",") if a.strip()]
    item.director = (director or "").strip()
    item.trailer_url = (trailer_url or "").strip()
    item.series_title = (series_title or "").strip()
    item.season = int(season) if (season or "").isdigit() else None
    item.episode = int(episode) if (episode or "").isdigit() else None
    item.quality = (quality or "").strip()
    await item.save()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/request/{request_id}/{action}")
async def update_content_request(request: Request, request_id: str, action: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await ContentRequest.get(request_id)
    if not row:
        raise HTTPException(404)
    action = (action or "").lower()
    if action not in ("fulfilled", "rejected", "pending"):
        raise HTTPException(400)
    row.status = action
    row.updated_at = datetime.now()
    await row.save()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/delete_user")
async def delete_user(request: Request, user_phone: str = Form(...)):
    """Deletes a user from the DB"""
    user = await get_current_user(request)
    # Re-verify admin
    if not _is_admin(user):
        raise HTTPException(403)
    
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        await target.delete()
        # Optional: Delete their files too
        await FileSystemItem.find(FileSystemItem.owner_phone == user_phone).delete()
    
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/approve_user")
async def approve_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "approved"
        target.approved_at = datetime.now()
        await target.save()
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/block_user")
async def block_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "blocked"
        await target.save()
    return RedirectResponse("/dashboard", status_code=303)
