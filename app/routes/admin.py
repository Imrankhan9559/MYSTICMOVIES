from datetime import datetime
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
        title = (getattr(item, "title", "") or info["title"] or "").strip()
        if not title:
            continue
        ctype = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        year = (getattr(item, "year", "") or info["year"] or "").strip()
        key = (title.lower(), year, ctype)
        group = groups.setdefault(key, {
            "title": title,
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
    return sorted(groups.values(), key=lambda g: g["title"].lower())

async def _group_published_catalog() -> list[dict]:
    rows = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published"
    ).sort("-created_at").to_list()
    groups: dict[tuple, dict] = {}
    for item in rows:
        info = _parse_name(item.name or "")
        title = (getattr(item, "series_title", "") or getattr(item, "title", "") or info["title"] or "").strip()
        if not title:
            continue
        ctype = (getattr(item, "catalog_type", "") or ("series" if info["is_series"] else "movie")).lower()
        year = (getattr(item, "year", "") or info["year"] or "").strip()
        key = (title.lower(), year, ctype)
        group = groups.setdefault(key, {
            "title": title,
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
    return sorted(groups.values(), key=lambda g: g["title"].lower())

@router.get("/admin")
async def admin_redirect(request: Request):
    return RedirectResponse("/main-control")

@router.get("/main-control")
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

    storage_suggestions = await _group_storage_suggestions()
    published_groups = await _group_published_catalog()

    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": None, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status,
        "storage_suggestions": storage_suggestions,
        "published_groups": published_groups
    })

@router.get("/main-control/tmdb/lookup")
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

@router.post("/main-control/publish")
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
        return RedirectResponse("/main-control", status_code=303)
    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(raw_ids))).to_list()
    if not items:
        return RedirectResponse("/main-control", status_code=303)

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
        return RedirectResponse("/main-control", status_code=303)

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

    # Mark storage items as used
    for item in items:
        try:
            item.catalog_status = "used"
            await item.save()
        except Exception:
            pass

    return RedirectResponse("/main-control", status_code=303)

@router.post("/main-control/update-metadata")
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
        return RedirectResponse("/main-control", status_code=303)

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

    return RedirectResponse("/main-control", status_code=303)

@router.post("/main-control/delete")
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
        return RedirectResponse("/main-control", status_code=303)
    await FileSystemItem.find(
        FileSystemItem.catalog_status == "published",
        FileSystemItem.catalog_type == group_type,
        FileSystemItem.title == group_title
    ).delete()
    return RedirectResponse("/main-control", status_code=303)

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
    return RedirectResponse("/main-control", status_code=303)

@router.post("/admin/bots/update_tokens")
async def admin_update_tokens(request: Request, bot_tokens: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    tokens = [t.strip() for t in bot_tokens.replace("\n", ",").split(",") if t.strip()]
    await reload_bot_pool(tokens)
    return RedirectResponse("/main-control", status_code=303)

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
    storage_suggestions = await _group_storage_suggestions()
    published_groups = await _group_published_catalog()
    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": result, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status,
        "storage_suggestions": storage_suggestions,
        "published_groups": published_groups
    })

@router.post("/admin/tmdb/refresh")
async def admin_refresh_tmdb(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not getattr(settings, "TMDB_API_KEY", ""):
        return RedirectResponse("/main-control?tmdb=missing", status_code=303)
    # Fire-and-forget; refresh can take time for large catalogs.
    asyncio.create_task(refresh_tmdb_metadata(limit=None))
    return RedirectResponse("/main-control?tmdb=refresh_started", status_code=303)

@router.get("/settings")
async def admin_settings_redirect(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    return RedirectResponse("/main-control")

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
    return RedirectResponse("/main-control", status_code=303)

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
    return RedirectResponse("/main-control", status_code=303)

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
    return RedirectResponse("/main-control", status_code=303)

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
    
    return RedirectResponse("/main-control", status_code=303)

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
    return RedirectResponse("/main-control", status_code=303)

@router.post("/admin/block_user")
async def block_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "blocked"
        await target.save()
    return RedirectResponse("/main-control", status_code=303)
