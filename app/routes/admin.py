from datetime import datetime
import re
import asyncio
import uuid
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from beanie.operators import In
from app.db.models import User, FileSystemItem, PlaybackProgress, TokenSetting, SiteSettings, ContentRequest
from app.routes.dashboard import get_current_user, _cast_ids, _clone_parts
from app.routes.content import refresh_tmdb_metadata, _parse_name
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
        year = (details.get("release_date") or details.get("first_air_date") or "")[:4]
        genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
        credits = details.get("credits") or {}
        cast = [c.get("name") for c in (credits.get("cast") or [])[:12] if c.get("name")]
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
            "description": overview,
            "genres": genres,
            "actors": cast,
            "director": director,
            "trailer_url": trailer,
            "trailer_key": trailer_key,
            "poster": poster_url,
            "backdrop": backdrop_url
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

async def _publish_items(
    items: list[FileSystemItem],
    catalog_type: str,
    title: str,
    year: str,
    desc: str,
    genres_list: list[str],
    actors_list: list[str],
    director: str,
    trailer_url: str
) -> None:
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if not admin_phone:
        return
    catalog_type = (catalog_type or "movie").strip().lower()

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
            quality = getattr(item, "quality", "") or info["quality"] or "HD"
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
                genres=genres_list,
                actors=actors_list,
                director=director,
                trailer_url=trailer_url,
                parts=_clone_parts(item.parts)
            )
            await new_file.insert()
    else:
        root = await _ensure_folder(admin_phone, "Web Series", None)
        series_folder = await _ensure_folder(admin_phone, title, str(root.id))
        for item in items:
            info = _parse_name(item.name or "")
            season = getattr(item, "season", None) or info["season"]
            episode = getattr(item, "episode", None) or info["episode"]
            quality = getattr(item, "quality", "") or info["quality"] or "HD"
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
                description=desc,
                genres=genres_list,
                actors=actors_list,
                director=director,
                trailer_url=trailer_url,
                parts=_clone_parts(item.parts)
            )
            await new_file.insert()

    for item in items:
        try:
            item.catalog_status = "used"
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
    trailer_url: str = Form("")
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
    await _publish_items(
        items=items,
        catalog_type=catalog_type,
        title=title,
        year=year,
        desc=desc,
        genres_list=genres_list,
        actors_list=actors_list,
        director=director,
        trailer_url=trailer_url
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
    trailer_url: str = Form("")
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

    await _publish_items(
        items=matched,
        catalog_type=catalog_type,
        title=title,
        year=year,
        desc=desc,
        genres_list=genres_list,
        actors_list=actors_list,
        director=director,
        trailer_url=trailer_url
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
