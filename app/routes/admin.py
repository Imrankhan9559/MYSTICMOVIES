from datetime import datetime
import asyncio
import uuid
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from beanie.operators import In
from app.db.models import User, FileSystemItem, PlaybackProgress, TokenSetting, SiteSettings, ContentRequest
from app.routes.dashboard import get_current_user, _cast_ids
from app.routes.content import refresh_tmdb_metadata
from app.core.config import settings
from app.core.telegram_bot import pool_status, reload_bot_pool, speed_test, _get_pool_tokens

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

@router.get("/admin")
async def admin_panel(request: Request):
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

    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": None, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status
    })

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
    return RedirectResponse("/admin", status_code=303)

@router.post("/admin/bots/update_tokens")
async def admin_update_tokens(request: Request, bot_tokens: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    tokens = [t.strip() for t in bot_tokens.replace("\n", ",").split(",") if t.strip()]
    await reload_bot_pool(tokens)
    return RedirectResponse("/admin", status_code=303)

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
    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map,
        "bots": bots, "pool_tokens": pool_tokens, "speed_result": result, "site": site,
        "pending_content_requests": pending_requests, "content_items": content_items,
        "tmdb_configured": tmdb_configured, "tmdb_status": tmdb_status
    })

@router.post("/admin/tmdb/refresh")
async def admin_refresh_tmdb(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not getattr(settings, "TMDB_API_KEY", ""):
        return RedirectResponse("/admin?tmdb=missing", status_code=303)
    # Fire-and-forget; refresh can take time for large catalogs.
    asyncio.create_task(refresh_tmdb_metadata(limit=None))
    return RedirectResponse("/admin?tmdb=refresh_started", status_code=303)

@router.get("/settings")
async def admin_settings_redirect(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    return RedirectResponse("/admin")

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
    return RedirectResponse("/admin", status_code=303)

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
    return RedirectResponse("/admin", status_code=303)

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
    return RedirectResponse("/admin", status_code=303)

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
    
    return RedirectResponse("/admin", status_code=303)

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
    return RedirectResponse("/admin", status_code=303)

@router.post("/admin/block_user")
async def block_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "blocked"
        await target.save()
    return RedirectResponse("/admin", status_code=303)
