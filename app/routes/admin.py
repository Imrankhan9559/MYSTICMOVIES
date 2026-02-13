from datetime import datetime
import re
import asyncio
import uuid
import json
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from beanie.operators import In, Or
from app.core.content_store import build_content_groups, sync_content_catalog
from app.db.models import (
    User,
    FileSystemItem,
    PlaybackProgress,
    TokenSetting,
    SiteSettings,
    ContentRequest,
    UserActivityEvent,
    HomeSlider,
    AppSettings,
    AppRelease,
    AppBroadcast,
    AppDeviceSession,
)
from app.routes.dashboard import get_current_user, _cast_ids, _clone_parts, _build_search_regex
from app.routes.content import refresh_tmdb_metadata, _parse_name, _tmdb_get, _ensure_group_assets
from app.core.config import settings
from app.core.telegram_bot import pool_status, reload_bot_pool, speed_test, _get_pool_tokens
from app.utils.file_utils import format_size

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    if str(getattr(user, "role", "") or "").strip().lower() == "admin":
        return True
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))


DEFAULT_HEADER_MENU = [
    {"label": "Home", "url": "/", "icon": "fas fa-house"},
    {"label": "Content", "url": "/content", "icon": "fas fa-film"},
    {"label": "Request Content", "url": "/request-content", "icon": "fas fa-inbox"},
]
DEFAULT_FOOTER_EXPLORE_LINKS = [
    {"label": "Movies Library", "url": "/content/f/movies"},
    {"label": "Web Series", "url": "/content/f/series"},
    {"label": "Latest Uploads", "url": "/content/f/all"},
]
DEFAULT_FOOTER_SUPPORT_LINKS = [
    {"label": "Request Content", "url": "/request-content"},
    {"label": "Report an Issue", "url": "/request-content"},
]


def _clean_link_rows(value, include_icon: bool = False) -> list[dict]:
    rows = value if isinstance(value, list) else []
    cleaned: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = (row.get("label") or "").strip()
        url = (row.get("url") or "").strip() or "#"
        icon = (row.get("icon") or "").strip()
        if not label:
            continue
        payload = {"label": label, "url": url}
        if include_icon:
            payload["icon"] = icon
        cleaned.append(payload)
    return cleaned


def _links_to_text(rows: list[dict], include_icon: bool = False) -> str:
    lines = []
    for row in rows or []:
        label = (row.get("label") or "").strip()
        url = (row.get("url") or "").strip()
        icon = (row.get("icon") or "").strip()
        if not label:
            continue
        if include_icon:
            lines.append(f"{label}|{url}|{icon}")
        else:
            lines.append(f"{label}|{url}")
    return "\n".join(lines)


def _parse_links_text(raw: str, include_icon: bool = False) -> list[dict]:
    out: list[dict] = []
    for line in (raw or "").splitlines():
        text = line.strip()
        if not text:
            continue
        parts = [part.strip() for part in text.split("|")]
        label = parts[0] if len(parts) > 0 else ""
        url = parts[1] if len(parts) > 1 and parts[1] else "#"
        icon = parts[2] if len(parts) > 2 else ""
        if not label:
            continue
        row = {"label": label, "url": url}
        if include_icon:
            row["icon"] = icon
        out.append(row)
    return out


def _apply_site_defaults(row: SiteSettings) -> bool:
    changed = False
    if not getattr(row, "topbar_text", "").strip():
        row.topbar_text = "Welcome to Mystic Movies"
        changed = True
    if getattr(row, "logo_path", None) is None:
        row.logo_path = ""
        changed = True
    if not getattr(row, "footer_about_text", "").strip():
        row.footer_about_text = "Mystic Movies provides high-quality content for free. If a movie is missing, let us know."
        changed = True
    if not getattr(row, "social_fb", "").strip():
        row.social_fb = "#"
        changed = True
    if not getattr(row, "social_ig", "").strip():
        row.social_ig = "#"
        changed = True
    if not getattr(row, "social_tg", "").strip():
        row.social_tg = "#"
        changed = True
    if not getattr(row, "donate_link", "").strip():
        row.donate_link = "/donate"
        changed = True
    if not getattr(row, "contact_name", "").strip():
        row.contact_name = "Mystic Movies Admin"
        changed = True
    if not getattr(row, "contact_email", "").strip():
        row.contact_email = "support@mysticmovies.site"
        changed = True

    header_menu = _clean_link_rows(getattr(row, "header_menu", None), include_icon=True)
    if not header_menu:
        row.header_menu = [x.copy() for x in DEFAULT_HEADER_MENU]
        changed = True
    else:
        row.header_menu = header_menu

    explore_links = _clean_link_rows(getattr(row, "footer_explore_links", None), include_icon=False)
    if not explore_links:
        row.footer_explore_links = [x.copy() for x in DEFAULT_FOOTER_EXPLORE_LINKS]
        changed = True
    else:
        row.footer_explore_links = explore_links

    support_links = _clean_link_rows(getattr(row, "footer_support_links", None), include_icon=False)
    if not support_links:
        row.footer_support_links = [x.copy() for x in DEFAULT_FOOTER_SUPPORT_LINKS]
        changed = True
    else:
        row.footer_support_links = support_links
    return changed

async def _site_settings() -> SiteSettings:
    row = await SiteSettings.find_one(SiteSettings.key == "main")
    if not row:
        row = SiteSettings(key="main")
        await row.insert()
    changed = _apply_site_defaults(row)
    if changed:
        row.updated_at = datetime.now()
        await row.save()
    return row


async def _catalog_counts(groups: list[dict] | None = None) -> dict:
    all_groups = groups if groups is not None else await _group_published_catalog()
    movie_groups = [g for g in all_groups if g.get("type") == "movie"]
    series_groups = [g for g in all_groups if g.get("type") == "series"]
    return {
        "published_groups": all_groups,
        "published_movies": len(movie_groups),
        "published_series": len(series_groups),
        "published_movie_files": sum(int(g.get("file_count") or 0) for g in movie_groups),
        "published_series_files": sum(int(g.get("file_count") or 0) for g in series_groups),
    }


async def _admin_badges(groups: list[dict] | None = None) -> dict:
    counts = await _catalog_counts(groups)
    counts["pending_storage"] = await FileSystemItem.find({
        "source": "storage",
        "is_folder": False,
        "catalog_status": {"$nin": ["published", "used"]}
    }).count()
    return counts


async def _admin_context_base(user: User, groups: list[dict] | None = None) -> dict:
    site = await _site_settings()
    badges = await _admin_badges(groups)
    return {
        "user": user,
        "is_admin": True,
        "site": site,
        **badges,
    }

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


def _slugify(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _content_path(title: str, year: str) -> str:
    title_part = _slugify(title)
    year_part = (year or "").strip()
    slug = f"{title_part}-{year_part}" if year_part else title_part
    return f"/content/details/{slug}" if slug else "/content"


def _format_release_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return dt.strftime("%d %b %Y")
    except Exception:
        return raw

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
    await sync_content_catalog(force=False)
    groups = await build_content_groups(None, True, limit=5000, ensure_sync=False)
    for g in groups:
        g["items"].sort(key=lambda x: (x.get("season") or 0, x.get("episode") or 0, x.get("quality") or ""))
        _summarize_group(g)
        g["release_date_label"] = _format_release_date(g.get("release_date", ""))
        g["content_path"] = _content_path(g.get("title", ""), g.get("year", ""))
    return sorted(groups, key=lambda g: (g.get("title") or "").lower())


async def _find_group_by_item_id(item_id: str) -> dict | None:
    groups = await _group_published_catalog()
    for g in groups:
        if g.get("id") == item_id:
            return g
        for itm in g.get("items", []):
            if itm.get("id") == item_id:
                return g
    return None


async def _find_group_identity(
    group_id: str = "",
    group_title: str = "",
    group_year: str = "",
    group_type: str = "",
) -> dict | None:
    group_id = (group_id or "").strip()
    if group_id:
        found = await _find_group_by_item_id(group_id)
        if found:
            return found

    title = (group_title or "").strip().lower()
    year = (group_year or "").strip()
    ctype = (group_type or "").strip().lower()
    if not title:
        return None
    groups = await _group_published_catalog()
    for g in groups:
        if (g.get("title", "") or "").strip().lower() != title:
            continue
        if year and (g.get("year", "") or "").strip() != year:
            continue
        if ctype and (g.get("type", "") or "").strip().lower() != ctype:
            continue
        return g
    return None


def _build_request_content_options(published_groups: list[dict]) -> list[dict]:
    options = []
    for g in published_groups:
        content_path = (g.get("content_path") or "").strip()
        if not content_path.startswith("/content/details/"):
            continue
        title = (g.get("title") or "").strip()
        year = (g.get("year") or "").strip()
        ctype = (g.get("type") or "").strip().lower()
        options.append({
            "id": g.get("id"),
            "title": title,
            "type": ctype,
            "year": year,
            "path": content_path,
            "label": f"{title} ({year or '-'}) [{(ctype or 'movie').upper()}]",
        })
    options.sort(key=lambda x: ((x.get("title") or "").lower(), x.get("year") or ""))
    return options


def _hydrate_request_links(rows: list[ContentRequest], published_groups: list[dict]) -> None:
    by_id = {str(g.get("id")): g for g in published_groups if g.get("id")}
    for row in rows:
        path = (getattr(row, "fulfilled_content_path", "") or "").strip()
        ref_id = (getattr(row, "fulfilled_content_id", "") or "").strip()
        if path:
            if not path.startswith("/") and not re.match(r"^https?://", path, re.I):
                path = "/" + path.lstrip("/")
            setattr(row, "fulfilled_content_path", path)
            continue
        if ref_id and ref_id in by_id:
            g = by_id[ref_id]
            resolved_path = (g.get("content_path") or "").strip()
            if resolved_path:
                setattr(row, "fulfilled_content_path", resolved_path)
            if not getattr(row, "fulfilled_content_title", ""):
                setattr(row, "fulfilled_content_title", g.get("title") or "")
            if not getattr(row, "fulfilled_content_type", ""):
                setattr(row, "fulfilled_content_type", g.get("type") or "")

@router.get("/admin")
async def admin_redirect(request: Request):
    return RedirectResponse("/dashboard")

@router.get("/main-control")
async def main_control_alias(request: Request):
    return RedirectResponse("/dashboard")

@router.get("/dashboard")
async def main_control(request: Request):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/admin-login")
    
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    published_groups = await _group_published_catalog()
    base_ctx = await _admin_context_base(user, published_groups)

    total_users = await User.count()
    total_files = await FileSystemItem.find(FileSystemItem.is_folder == False).count()
    pending_admin_requests_all = await User.find(User.status == "pending").sort("-requested_at").to_list()
    pending_requests_all = await ContentRequest.find(ContentRequest.status == "pending").sort("-created_at").to_list()
    fulfilled_requests_all = await ContentRequest.find(ContentRequest.status == "fulfilled").sort("-updated_at").to_list()
    _hydrate_request_links(fulfilled_requests_all, published_groups)
    request_content_options = _build_request_content_options(published_groups)

    total_titles = int(base_ctx.get("published_movies") or 0) + int(base_ctx.get("published_series") or 0)
    published_preview = published_groups[:15]
    pending_content_requests = pending_requests_all[:5]
    fulfilled_content_requests = fulfilled_requests_all[:5]
    pending_admin_requests = pending_admin_requests_all[:5]

    return templates.TemplateResponse("admin.html", {
        "request": request,
        **base_ctx,
        "total_users": total_users,
        "total_files": total_files,
        "total_titles": total_titles,
        "pending_users": pending_admin_requests,
        "pending_content_requests": pending_content_requests,
        "fulfilled_content_requests": fulfilled_content_requests,
        "pending_content_requests_total": len(pending_requests_all),
        "fulfilled_content_requests_total": len(fulfilled_requests_all),
        "pending_admin_requests_total": len(pending_admin_requests_all),
        "request_content_options": request_content_options,
        "published_preview": published_preview,
    })


async def _render_main_settings(request: Request, user: User, speed_result: dict | None = None):
    base_ctx = await _admin_context_base(user)
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    link_token = token_doc.value if token_doc else ""
    bots = await pool_status()
    pool_tokens = ", ".join(_get_pool_tokens())
    tmdb_configured = bool(getattr(settings, "TMDB_API_KEY", ""))
    tmdb_status = (request.query_params.get("tmdb") or "").strip().lower()
    return templates.TemplateResponse("main_settings.html", {
        "request": request,
        **base_ctx,
        "link_token": link_token,
        "bots": bots,
        "pool_tokens": pool_tokens,
        "tmdb_configured": tmdb_configured,
        "tmdb_status": tmdb_status,
        "speed_result": speed_result,
    })


@router.get("/main-settings")
@router.get("/dashboard/main-settings")
async def main_settings(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    return await _render_main_settings(request, user, speed_result=None)


@router.get("/header-footer-settings")
@router.get("/dashboard/header-footer-settings")
async def header_footer_settings(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    base_ctx = await _admin_context_base(user)
    site = base_ctx.get("site")
    return templates.TemplateResponse("header_footer_settings.html", {
        "request": request,
        **base_ctx,
        "saved": (request.query_params.get("saved") or "").strip() == "1",
        "header_menu_text": _links_to_text(getattr(site, "header_menu", []), include_icon=True),
        "footer_explore_text": _links_to_text(getattr(site, "footer_explore_links", []), include_icon=False),
        "footer_support_text": _links_to_text(getattr(site, "footer_support_links", []), include_icon=False),
    })


@router.post("/admin/header-footer/save")
async def save_header_footer_settings(
    request: Request,
    topbar_text: str = Form("Welcome to Mystic Movies"),
    logo_path: str = Form(""),
    header_menu_text: str = Form(""),
    footer_about_text: str = Form(""),
    footer_explore_text: str = Form(""),
    footer_support_text: str = Form(""),
    social_fb: str = Form("#"),
    social_ig: str = Form("#"),
    social_tg: str = Form("#"),
    donate_link: str = Form("/donate"),
    contact_name: str = Form("Mystic Movies Admin"),
    contact_email: str = Form("support@mysticmovies.site"),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    site = await _site_settings()

    header_menu = _parse_links_text(header_menu_text, include_icon=True)
    explore_links = _parse_links_text(footer_explore_text, include_icon=False)
    support_links = _parse_links_text(footer_support_text, include_icon=False)

    site.topbar_text = (topbar_text or "Welcome to Mystic Movies").strip()
    site.logo_path = (logo_path or "").strip()
    site.header_menu = header_menu if header_menu else [x.copy() for x in DEFAULT_HEADER_MENU]
    site.footer_about_text = (footer_about_text or "").strip()
    site.footer_explore_links = explore_links if explore_links else [x.copy() for x in DEFAULT_FOOTER_EXPLORE_LINKS]
    site.footer_support_links = support_links if support_links else [x.copy() for x in DEFAULT_FOOTER_SUPPORT_LINKS]
    site.social_fb = (social_fb or "#").strip()
    site.social_ig = (social_ig or "#").strip()
    site.social_tg = (social_tg or "#").strip()
    site.donate_link = (donate_link or "/donate").strip()
    site.contact_name = (contact_name or "Mystic Movies Admin").strip()
    site.contact_email = (contact_email or "support@mysticmovies.site").strip()
    site.updated_at = datetime.now()
    await site.save()
    return RedirectResponse("/header-footer-settings?saved=1", status_code=303)


def _is_truthy(value: str) -> bool:
    return (value or "").strip().lower() in {"1", "true", "on", "yes"}


def _safe_int(value: str, default: int = 0) -> int:
    try:
        return int((value or "").strip())
    except Exception:
        return default


def _normalize_update_mode(value: str) -> str:
    mode = (value or "").strip().lower()
    if mode in {"forced", "recommended", "none"}:
        return mode
    return "none"


async def _app_settings_row() -> AppSettings:
    row = await AppSettings.find_one(AppSettings.key == "main")
    if not row:
        row = AppSettings(
            key="main",
            telegram_bot_username=(getattr(settings, "BOT_USERNAME", "") or "").strip(),
            updated_at=datetime.now(),
            created_at=datetime.now(),
        )
        await row.insert()
    changed = False
    if not (row.telegram_bot_username or "").strip() and (getattr(settings, "BOT_USERNAME", "") or "").strip():
        row.telegram_bot_username = (getattr(settings, "BOT_USERNAME", "") or "").strip()
        changed = True
    if changed:
        row.updated_at = datetime.now()
        await row.save()
    return row


async def _ensure_apps_root(user: User) -> FileSystemItem:
    owner_phone = (_normalize_phone(getattr(settings, "ADMIN_PHONE", "") or "") or _normalize_phone(user.phone_number))
    return await _ensure_folder(owner_phone, "APPS", None, source="admin")


async def _prepare_apk_item(user: User, file_id: str) -> FileSystemItem | None:
    file_key = (file_id or "").strip()
    if not file_key:
        return None
    item = await FileSystemItem.get(file_key)
    if not item or item.is_folder:
        return None
    if not (item.name or "").lower().endswith(".apk"):
        return None
    apps_root = await _ensure_apps_root(user)
    dirty = False
    if str(item.parent_id or "") != str(apps_root.id):
        item.parent_id = str(apps_root.id)
        dirty = True
    if not item.share_token:
        item.share_token = str(uuid.uuid4())
        dirty = True
    if dirty:
        await item.save()
    return item


async def _apk_candidates(limit: int = 700) -> list[dict]:
    rows = await FileSystemItem.find(FileSystemItem.is_folder == False).sort("-created_at").limit(limit).to_list()
    out: list[dict] = []
    for row in rows:
        name = (row.name or "").strip()
        if not name.lower().endswith(".apk"):
            continue
        out.append({
            "id": str(row.id),
            "name": name,
            "size": int(row.size or 0),
            "size_label": format_size(int(row.size or 0)),
            "owner_phone": row.owner_phone,
            "created_at": row.created_at,
            "share_token": (row.share_token or "").strip(),
        })
    return out


@router.get("/app-management")
@router.get("/dashboard/app-management")
async def app_management(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    app_cfg = await _app_settings_row()
    release_rows = await AppRelease.find_all().sort([("build_number", -1), ("created_at", -1)]).limit(40).to_list()
    broadcasts = await AppBroadcast.find_all().sort("-created_at").limit(40).to_list()
    devices = await AppDeviceSession.find_all().sort("-last_ping_at").limit(120).to_list()
    apk_files = await _apk_candidates()
    content_options = await _slider_content_options(limit=1200)
    pending_requests = await ContentRequest.find(ContentRequest.status == "pending").sort("-updated_at").limit(12).to_list()
    pending_requests_total = await ContentRequest.find(ContentRequest.status == "pending").count()
    app_events = await UserActivityEvent.find({"action": {"$regex": "^app_"}}).sort("-created_at").limit(20).to_list()
    selected_apk = next((row for row in apk_files if row["id"] == (app_cfg.latest_apk_item_id or "")), None)
    now = datetime.now()
    online_cutoff = now.timestamp() - 600
    online_devices = 0
    for row in devices:
        ts = row.last_ping_at.timestamp() if row.last_ping_at else 0
        if ts >= online_cutoff:
            online_devices += 1

    return templates.TemplateResponse("app_management.html", {
        "request": request,
        **base_ctx,
        "saved": (request.query_params.get("saved") or "").strip() == "1",
        "release_saved": (request.query_params.get("release") or "").strip() == "1",
        "notify_saved": (request.query_params.get("notify") or "").strip() == "1",
        "app_cfg": app_cfg,
        "apk_files": apk_files,
        "selected_apk": selected_apk,
        "release_rows": release_rows,
        "broadcasts": broadcasts,
        "devices": devices,
        "online_devices": online_devices,
        "content_options": content_options,
        "pending_requests": pending_requests,
        "pending_requests_total": pending_requests_total,
        "app_events": app_events,
    })


@router.post("/admin/app-management/save")
async def app_management_save(
    request: Request,
    app_name: str = Form("MysticMovies Android"),
    package_name: str = Form("com.mysticmovies.app"),
    splash_image_url: str = Form(""),
    loading_icon_url: str = Form(""),
    onboarding_message: str = Form(""),
    ads_message: str = Form(""),
    update_popup_title: str = Form("Update Available"),
    update_popup_body: str = Form("A new app version is available."),
    latest_version: str = Form(""),
    latest_build: str = Form("0"),
    latest_release_notes: str = Form(""),
    min_supported_version: str = Form(""),
    recommended_update: str = Form(""),
    force_update: str = Form(""),
    maintenance_mode: str = Form(""),
    maintenance_message: str = Form(""),
    push_enabled: str = Form(""),
    keepalive_on_launch: str = Form(""),
    telegram_bot_username: str = Form(""),
    latest_apk_item_id: str = Form(""),
    clear_latest_apk: str = Form(""),
    request_login_required: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await _app_settings_row()
    row.app_name = (app_name or "MysticMovies Android").strip()
    row.package_name = (package_name or "com.mysticmovies.app").strip()
    row.splash_image_url = (splash_image_url or "").strip()
    row.loading_icon_url = (loading_icon_url or "").strip()
    row.onboarding_message = (onboarding_message or "").strip()
    row.ads_message = (ads_message or "").strip()
    row.update_popup_title = (update_popup_title or "Update Available").strip()
    row.update_popup_body = (update_popup_body or "A new app version is available.").strip()
    row.latest_version = (latest_version or "").strip()
    row.latest_build = _safe_int(latest_build, 0)
    row.latest_release_notes = (latest_release_notes or "").strip()
    row.min_supported_version = (min_supported_version or "").strip()
    row.recommended_update = _is_truthy(recommended_update)
    row.force_update = _is_truthy(force_update)
    row.maintenance_mode = _is_truthy(maintenance_mode)
    row.maintenance_message = (maintenance_message or "").strip()
    row.push_enabled = _is_truthy(push_enabled)
    row.keepalive_on_launch = _is_truthy(keepalive_on_launch)
    row.telegram_bot_username = (telegram_bot_username or "").strip()
    row.request_login_required = _is_truthy(request_login_required)

    if _is_truthy(clear_latest_apk):
        row.latest_apk_item_id = None
        row.latest_apk_share_token = None
        row.latest_apk_size = 0
    else:
        apk_item = await _prepare_apk_item(user, latest_apk_item_id)
        if apk_item:
            row.latest_apk_item_id = str(apk_item.id)
            row.latest_apk_share_token = (apk_item.share_token or "").strip() or None
            row.latest_apk_size = int(apk_item.size or 0)

    row.updated_at = datetime.now()
    await row.save()
    return RedirectResponse("/app-management?saved=1", status_code=303)


@router.post("/admin/app-management/release")
async def app_management_release(
    request: Request,
    version: str = Form(""),
    build_number: str = Form("0"),
    release_notes: str = Form(""),
    update_mode: str = Form("none"),
    is_active: str = Form(""),
    apk_item_id: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)

    apk_item = await _prepare_apk_item(user, apk_item_id)
    build_no = _safe_int(build_number, 0)
    mode = _normalize_update_mode(update_mode)
    row = AppRelease(
        version=(version or "").strip(),
        build_number=build_no,
        release_notes=(release_notes or "").strip(),
        apk_item_id=str(apk_item.id) if apk_item else None,
        apk_share_token=(apk_item.share_token or "").strip() if apk_item else None,
        apk_size=int(apk_item.size or 0) if apk_item else 0,
        update_mode=mode,
        is_active=_is_truthy(is_active),
        created_by=user.phone_number,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    await row.insert()

    # Sync latest release to app settings for bootstrap.
    cfg = await _app_settings_row()
    if row.version:
        cfg.latest_version = row.version
    if row.build_number:
        cfg.latest_build = row.build_number
    if row.release_notes:
        cfg.latest_release_notes = row.release_notes
    if row.apk_item_id:
        cfg.latest_apk_item_id = row.apk_item_id
    if row.apk_share_token:
        cfg.latest_apk_share_token = row.apk_share_token
        cfg.latest_apk_size = row.apk_size
    if mode == "forced":
        cfg.force_update = True
        cfg.recommended_update = True
    elif mode == "recommended":
        cfg.recommended_update = True
    cfg.updated_at = datetime.now()
    await cfg.save()

    return RedirectResponse("/app-management?release=1", status_code=303)


@router.post("/admin/app-management/notify")
async def app_management_notify(
    request: Request,
    title: str = Form(""),
    message: str = Form(""),
    notice_type: str = Form("news"),
    is_active: str = Form(""),
    link_url: str = Form(""),
    image_url: str = Form(""),
    audience: str = Form("all"),
    content_slug: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    notice_title = (title or "").strip()
    notice_message = (message or "").strip()
    if not notice_title or not notice_message:
        return RedirectResponse("/app-management", status_code=303)
    kind = (notice_type or "news").strip().lower()
    if kind not in {"news", "ad", "feature", "maintenance"}:
        kind = "news"
    audience_value = (audience or "all").strip().lower()
    if audience_value not in {"all", "logged_in"}:
        audience_value = "all"
    content_slug_clean = (content_slug or "").strip().strip("/")
    target_link = (link_url or "").strip()
    if content_slug_clean:
        target_link = f"/content/details/{content_slug_clean}"

    row = AppBroadcast(
        title=notice_title,
        message=notice_message,
        type=kind,
        is_active=_is_truthy(is_active),
        created_by=user.phone_number,
        created_at=datetime.now(),
    )
    row.link_url = target_link
    row.image_url = (image_url or "").strip()
    row.audience = audience_value
    await row.insert()
    return RedirectResponse("/app-management?notify=1", status_code=303)


@router.post("/admin/app-management/broadcast/{broadcast_id}/toggle")
async def app_management_toggle_broadcast(request: Request, broadcast_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await AppBroadcast.get(broadcast_id)
    if row:
        row.is_active = not bool(row.is_active)
        await row.save()
    return RedirectResponse("/app-management", status_code=303)


@router.post("/admin/app-management/broadcast/{broadcast_id}/delete")
async def app_management_delete_broadcast(request: Request, broadcast_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await AppBroadcast.get(broadcast_id)
    if row:
        await row.delete()
    return RedirectResponse("/app-management", status_code=303)


async def _slider_content_options(limit: int = 800) -> list[dict]:
    groups = await _group_published_catalog()
    options = []
    for group in groups:
        title = (group.get("title") or "").strip()
        if not title:
            continue
        slug = (group.get("slug") or "").strip() or _slugify(title)
        if not slug:
            continue
        options.append({
            "slug": slug,
            "title": title,
            "year": (group.get("year") or "").strip(),
            "type": (group.get("type") or "").strip().lower(),
            "poster": group.get("poster") or "",
            "backdrop": group.get("backdrop") or "",
            "link_url": f"/content/details/{slug}",
        })
        if len(options) >= limit:
            break
    return options


@router.get("/manage-slider")
@router.get("/dashboard/manage-slider")
async def manage_slider(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    base_ctx = await _admin_context_base(user)
    sliders = await HomeSlider.find_all().sort([("sort_order", 1), ("created_at", -1)]).to_list()
    options = await _slider_content_options()
    return templates.TemplateResponse("manage_slider.html", {
        "request": request,
        **base_ctx,
        "sliders": sliders,
        "content_options": options,
        "saved": (request.query_params.get("saved") or "").strip() == "1",
    })


@router.post("/manage-slider/create")
async def create_slider_item(
    request: Request,
    content_slug: str = Form(""),
    title: str = Form(""),
    subtitle: str = Form(""),
    button_text: str = Form("Watch Now"),
    link_url: str = Form(""),
    image_url: str = Form(""),
    sort_order: int = Form(0),
    is_active: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    slug = (content_slug or "").strip().lower()
    title = (title or "").strip()
    link = (link_url or "").strip()
    if slug and not link:
        link = f"/content/details/{slug}"
    row = HomeSlider(
        content_slug=slug or None,
        title=title,
        subtitle=(subtitle or "").strip(),
        button_text=(button_text or "Watch Now").strip(),
        link_url=link or "/content",
        image_url=(image_url or "").strip(),
        sort_order=int(sort_order or 0),
        is_active=_is_truthy(is_active),
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    await row.insert()
    return RedirectResponse("/manage-slider?saved=1", status_code=303)


@router.post("/manage-slider/update/{slider_id}")
async def update_slider_item(
    request: Request,
    slider_id: str,
    content_slug: str = Form(""),
    title: str = Form(""),
    subtitle: str = Form(""),
    button_text: str = Form("Watch Now"),
    link_url: str = Form(""),
    image_url: str = Form(""),
    sort_order: int = Form(0),
    is_active: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await HomeSlider.get(slider_id)
    if not row:
        raise HTTPException(404)
    slug = (content_slug or "").strip().lower()
    link = (link_url or "").strip()
    if slug and not link:
        link = f"/content/details/{slug}"
    row.content_slug = slug or None
    row.title = (title or "").strip()
    row.subtitle = (subtitle or "").strip()
    row.button_text = (button_text or "Watch Now").strip()
    row.link_url = link or "/content"
    row.image_url = (image_url or "").strip()
    row.sort_order = int(sort_order or 0)
    row.is_active = _is_truthy(is_active)
    row.updated_at = datetime.now()
    await row.save()
    return RedirectResponse("/manage-slider?saved=1", status_code=303)


@router.post("/manage-slider/delete/{slider_id}")
async def delete_slider_item(request: Request, slider_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await HomeSlider.get(slider_id)
    if row:
        await row.delete()
    return RedirectResponse("/manage-slider?saved=1", status_code=303)


@router.post("/manage-slider/reorder")
async def reorder_slider_items(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    ids = payload.get("ids") if isinstance(payload, dict) else []
    if not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="ids must be a list")

    ordered_ids: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        slider_id = str(raw or "").strip()
        if not slider_id or slider_id in seen:
            continue
        seen.add(slider_id)
        ordered_ids.append(slider_id)

    if not ordered_ids:
        return {"status": "ok", "updated": 0}

    rows = await HomeSlider.find(In(HomeSlider.id, _cast_ids(ordered_ids))).to_list()
    rows_by_id = {str(row.id): row for row in rows}
    now = datetime.now()
    updated = 0
    order_value = 10

    for slider_id in ordered_ids:
        row = rows_by_id.get(slider_id)
        if not row:
            continue
        row.sort_order = order_value
        row.updated_at = now
        await row.save()
        updated += 1
        order_value += 10

    return {"status": "ok", "updated": updated}


@router.get("/users")
@router.get("/dashboard/users")
async def users_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    all_users = await User.find_all().sort("-created_at").to_list()
    pending_admin_requests = await User.find(User.status == "pending").sort("-requested_at").to_list()
    return templates.TemplateResponse("admin_users.html", {
        "request": request,
        **base_ctx,
        "users": all_users,
        "pending_admin_requests": pending_admin_requests,
    })


@router.get("/content-requests")
@router.get("/dashboard/content-requests")
@router.get("/Content-Requests")
@router.get("/ContentRequests")
async def content_requests_page(request: Request, status: str = ""):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    status_filter = (status or "").strip().lower()
    all_rows = await ContentRequest.find_all().sort("-updated_at").limit(1200).to_list()
    if status_filter in {"pending", "fulfilled", "rejected"}:
        all_requests = [r for r in all_rows if (getattr(r, "status", "") or "") == status_filter]
    else:
        all_requests = all_rows
    published_groups = await _group_published_catalog()
    _hydrate_request_links(all_rows, published_groups)
    _hydrate_request_links(all_requests, published_groups)
    pending_content_requests = [r for r in all_rows if (getattr(r, "status", "") or "") == "pending"]
    fulfilled_content_requests = [r for r in all_rows if (getattr(r, "status", "") or "") == "fulfilled"]
    rejected_content_requests = [r for r in all_rows if (getattr(r, "status", "") or "") == "rejected"]

    base_url = str(request.base_url).rstrip("/")
    for row in all_requests:
        path = (getattr(row, "fulfilled_content_path", "") or "").strip()
        if path and not path.startswith("/"):
            path = "/" + path.lstrip("/")
        setattr(row, "content_path", path)
        setattr(row, "content_full_url", f"{base_url}{path}" if path else "")

    request_content_options = _build_request_content_options(published_groups)
    return templates.TemplateResponse("admin_content_requests.html", {
        "request": request,
        **base_ctx,
        "status_filter": status_filter,
        "all_requests": all_requests,
        "pending_content_requests": pending_content_requests,
        "fulfilled_content_requests": fulfilled_content_requests,
        "rejected_content_requests": rejected_content_requests,
        "request_content_options": request_content_options,
    })


@router.get("/user-playback-analytics")
async def user_playback_analytics(request: Request, q: str = "", user_key: str = ""):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    search = (q or "").strip()
    selected_user = (user_key or "").strip()

    event_query: dict | None = None
    if search:
        regex = _build_search_regex(search)
        if regex:
            event_query = {
                "$or": [
                    {"user_key": {"$regex": regex, "$options": "i"}},
                    {"user_name": {"$regex": regex, "$options": "i"}},
                    {"content_title": {"$regex": regex, "$options": "i"}},
                    {"action": {"$regex": regex, "$options": "i"}},
                ]
            }
    events = await UserActivityEvent.find(event_query or {}).sort("-created_at").limit(800).to_list()
    if selected_user:
        events = [e for e in events if (getattr(e, "user_key", "") or "") == selected_user]

    item_ids = [e.item_id for e in events if getattr(e, "item_id", None)]
    item_map = {}
    if item_ids:
        db_items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
        item_map = {str(row.id): (row.series_title or row.title or row.name) for row in db_items}

    user_stats: dict[str, dict] = {}
    for e in events:
        key = (getattr(e, "user_key", "") or "").strip() or "guest"
        entry = user_stats.setdefault(key, {
            "user_key": key,
            "user_name": getattr(e, "user_name", None) or key,
            "user_phone": getattr(e, "user_phone", None) or "",
            "downloads": 0,
            "telegram": 0,
            "watch_together": 0,
            "events": 0,
            "last_seen": getattr(e, "created_at", None),
        })
        action = (getattr(e, "action", "") or "").strip().lower()
        if action == "download_request":
            entry["downloads"] += 1
        elif action == "telegram_request":
            entry["telegram"] += 1
        elif action.startswith("watch_together"):
            entry["watch_together"] += 1
        entry["events"] += 1
        created = getattr(e, "created_at", None)
        if created and (not entry["last_seen"] or created > entry["last_seen"]):
            entry["last_seen"] = created

    progress_query = {"user_key": selected_user} if selected_user else {}
    progress_rows = await PlaybackProgress.find(progress_query).sort("-updated_at").limit(600).to_list()
    progress_stats: dict[str, dict] = {}
    for p in progress_rows:
        key = (getattr(p, "user_key", "") or "").strip() or "guest"
        entry = progress_stats.setdefault(key, {
            "user_key": key,
            "sessions": 0,
            "watch_minutes": 0,
            "last_seen": getattr(p, "updated_at", None),
        })
        entry["sessions"] += 1
        entry["watch_minutes"] += int((getattr(p, "position", 0.0) or 0.0) // 60)
        updated_at = getattr(p, "updated_at", None)
        if updated_at and (not entry["last_seen"] or updated_at > entry["last_seen"]):
            entry["last_seen"] = updated_at

    for key, prog in progress_stats.items():
        target = user_stats.setdefault(key, {
            "user_key": key,
            "user_name": key,
            "user_phone": "",
            "downloads": 0,
            "telegram": 0,
            "watch_together": 0,
            "events": 0,
            "last_seen": prog["last_seen"],
        })
        target["sessions"] = prog["sessions"]
        target["watch_minutes"] = prog["watch_minutes"]
        if prog["last_seen"] and (not target["last_seen"] or prog["last_seen"] > target["last_seen"]):
            target["last_seen"] = prog["last_seen"]

    users_analytics = sorted(
        user_stats.values(),
        key=lambda row: (-(row.get("events", 0) + row.get("sessions", 0)), row.get("user_key", "")),
    )

    events_view = []
    for e in events[:300]:
        item_id = getattr(e, "item_id", None) or ""
        events_view.append({
            "user_key": getattr(e, "user_key", "") or "",
            "user_name": getattr(e, "user_name", "") or "",
            "action": getattr(e, "action", "") or "",
            "content_title": getattr(e, "content_title", "") or item_map.get(item_id, ""),
            "created_at": getattr(e, "created_at", None),
            "meta": getattr(e, "meta", {}) or {},
        })

    progress_view = []
    for row in progress_rows[:300]:
        item_id = getattr(row, "item_id", "") or ""
        duration = float(getattr(row, "duration", 0.0) or 0.0)
        position = float(getattr(row, "position", 0.0) or 0.0)
        progress_view.append({
            "user_key": getattr(row, "user_key", "") or "",
            "item_id": item_id,
            "title": item_map.get(item_id, item_id),
            "progress_pct": round((position / duration) * 100, 1) if duration > 0 else 0.0,
            "position_min": round(position / 60.0, 1),
            "updated_at": getattr(row, "updated_at", None),
        })

    return templates.TemplateResponse("user_playback_analytics.html", {
        "request": request,
        **base_ctx,
        "q": search,
        "selected_user": selected_user,
        "users_analytics": users_analytics,
        "events_view": events_view,
        "progress_view": progress_view,
    })


@router.get("/publish-content")
async def publish_content_alias(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    return RedirectResponse("/dashboard/publish-content", status_code=302)

@router.get("/dashboard/add-content")
async def add_content(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    tmdb_configured = bool(getattr(settings, "TMDB_API_KEY", ""))

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

    return templates.TemplateResponse("admin_add_content.html", {
        "request": request,
        **base_ctx,
        "tmdb_configured": tmdb_configured,
        "files": files,
    })

@router.get("/dashboard/publish-content")
async def publish_content(request: Request, q: str = ""):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    all_groups = await _group_published_catalog()
    base_ctx = await _admin_context_base(user, all_groups)
    query = (q or "").strip()

    filtered_groups = all_groups
    if query:
        regex = _build_search_regex(query)
        if regex:
            needle = re.compile(regex, re.I)
            matched = []
            for group in all_groups:
                searchable = [
                    group.get("title", ""),
                    group.get("year", ""),
                    group.get("type", ""),
                ]
                searchable.extend([(row.get("name") or "") for row in group.get("items", [])])
                if any(needle.search(text or "") for text in searchable):
                    matched.append(group)
            filtered_groups = matched
        else:
            filtered_groups = []

    if settings.TMDB_API_KEY:
        for g in filtered_groups[:80]:
            try:
                if not g.get("poster") or not g.get("backdrop"):
                    await _ensure_group_assets(g)
            except Exception:
                pass
    base_url = str(request.base_url).rstrip("/")
    for g in filtered_groups:
        g["content_full_url"] = f"{base_url}{g.get('content_path', '')}"
    return templates.TemplateResponse("publish_content.html", {
        "request": request,
        **base_ctx,
        "published_groups": filtered_groups,
        "q": query,
    })


@router.get("/dashboard/publish-content/details/{group_id}")
async def publish_content_details(
    request: Request,
    group_id: str,
    group_title: str = "",
    group_year: str = "",
    group_type: str = "",
):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    group = await _find_group_identity(group_id, group_title, group_year, group_type)
    if not group:
        return RedirectResponse("/dashboard/publish-content", status_code=303)
    if settings.TMDB_API_KEY and (not group.get("poster") or not group.get("description")):
        try:
            await _ensure_group_assets(group)
        except Exception:
            pass
    base_url = str(request.base_url).rstrip("/")
    group["content_full_url"] = f"{base_url}{group.get('content_path', '')}"
    return templates.TemplateResponse("publish_content_details.html", {
        "request": request,
        **base_ctx,
        "group": group,
    })

@router.get("/dashboard/publish-content/edit/{group_id}")
async def publish_content_edit(
    request: Request,
    group_id: str,
    group_title: str = "",
    group_year: str = "",
    group_type: str = "",
):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    group = await _find_group_identity(group_id, group_title, group_year, group_type)
    if not group:
        return RedirectResponse("/dashboard/publish-content", status_code=303)
    if settings.TMDB_API_KEY:
        try:
            await _ensure_group_assets(group)
        except Exception:
            pass
    base_url = str(request.base_url).rstrip("/")
    group["content_full_url"] = f"{base_url}{group.get('content_path', '')}"
    return templates.TemplateResponse("publish_content_edit.html", {
        "request": request,
        **base_ctx,
        "group": group,
        "return_to": f"/dashboard/publish-content/edit/{group.get('id')}"
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
    group = await _find_group_by_item_id(group_id) if group_id else None
    if group:
        group_title = (group.get("title") or group_title or "").strip()
        group_type = (group.get("type") or group_type or "movie").strip().lower()
        group_year = (group.get("year") or group_year or "").strip()
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

    items = []
    if group and group.get("items"):
        ids = [i.get("id") for i in group["items"] if i.get("id")]
        if ids:
            items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(ids))).to_list()
    if not items:
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
    old_title = group_title
    if admin_phone and new_title != old_title:
        root_name = "Movies" if group_type == "movie" else "Web Series"
        root = await _find_folder(admin_phone, root_name, None)
        if root:
            target_folder = await _find_folder(admin_phone, old_title, str(root.id))
            if target_folder:
                target_folder.name = new_title
                await target_folder.save()

    await sync_content_catalog(force=True)
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
    group = await _find_group_by_item_id(group_id)
    if not group:
        return RedirectResponse("/dashboard/publish-content", status_code=303)

    raw_ids = [i.strip() for i in (item_ids or "").split(",") if i.strip()]
    if not raw_ids:
        return RedirectResponse(f"/dashboard/publish-content/edit/{group.get('id')}", status_code=303)
    items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(raw_ids))).to_list()
    if not items:
        return RedirectResponse(f"/dashboard/publish-content/edit/{group.get('id')}", status_code=303)

    override_map = {}
    if overrides:
        try:
            override_map = json.loads(overrides)
        except Exception:
            override_map = {}

    catalog_type = (group.get("type") or "movie").strip().lower()
    title = (group.get("title") or "").strip()
    year = (group.get("year") or "").strip()
    desc = (group.get("description") or "").strip()
    genres_list = group.get("genres") or []
    actors_list = group.get("actors") or []
    director = (group.get("director") or "").strip()
    trailer_url = (group.get("trailer_url") or "").strip()
    poster_url = (group.get("poster") or "").strip()
    backdrop_url = (group.get("backdrop") or "").strip()
    trailer_key = (group.get("trailer_key") or "").strip()
    release_date = (group.get("release_date") or "").strip()
    cast_profiles_list = group.get("cast_profiles") or []
    tmdb_id_val = group.get("tmdb_id")

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

    return RedirectResponse(f"/dashboard/publish-content/edit/{group.get('id')}", status_code=303)

@router.post("/dashboard/publish-content/delete")
async def publish_content_delete(
    request: Request,
    group_id: str = Form(""),
    group_title: str = Form(""),
    group_year: str = Form(""),
    group_type: str = Form("movie"),
    return_to: str = Form("")
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    group = await _find_group_by_item_id(group_id) if group_id else None
    if group:
        group_title = (group.get("title") or group_title or "").strip()
        group_year = (group.get("year") or group_year or "").strip()
        group_type = (group.get("type") or group_type or "movie").strip().lower()
    group_title = (group_title or "").strip()
    group_year = (group_year or "").strip()
    group_type = (group_type or "movie").strip().lower()
    if not group_title:
        return RedirectResponse(return_to or "/dashboard/publish-content", status_code=303)

    deleted_items: list[FileSystemItem] = []
    if group and group.get("items"):
        ids = [i.get("id") for i in group["items"] if i.get("id")]
        if ids:
            deleted_items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(ids))).to_list()
            if deleted_items:
                await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(ids))).delete()
    if not deleted_items:
        query_items = [
            FileSystemItem.catalog_status == "published",
            FileSystemItem.catalog_type == group_type,
            Or(FileSystemItem.title == group_title, FileSystemItem.series_title == group_title)
        ]
        if group_year:
            query_items.append(FileSystemItem.year == group_year)
        deleted_items = await FileSystemItem.find(*query_items).to_list()
        if deleted_items:
            await FileSystemItem.find(In(FileSystemItem.id, _cast_ids([str(x.id) for x in deleted_items]))).delete()

    parent_ids = {str(item.parent_id) for item in deleted_items if item.parent_id}
    for parent_id in parent_ids:
        try:
            await _cleanup_parents(parent_id)
        except Exception:
            pass

    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if admin_phone:
        root_name = "Movies" if group_type == "movie" else "Web Series"
        root = await _find_folder(admin_phone, root_name, None)
        if root:
            target_folder = await _find_folder(admin_phone, group_title, str(root.id))
            if target_folder:
                try:
                    await _cleanup_empty_tree(str(target_folder.id))
                except Exception:
                    pass

    await sync_content_catalog(force=True)
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
    await sync_content_catalog(force=True)
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
    old_parent_id = item.parent_id

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
    if old_parent_id and old_parent_id != item.parent_id:
        await _cleanup_parents(old_parent_id)
    await sync_content_catalog(force=True)
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
            cast_profiles.append({"id": c.get("id"), "name": name, "role": role, "image": image})
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

    await sync_content_catalog(force=True)

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

    await sync_content_catalog(force=True)
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
    await sync_content_catalog(force=True)
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
    return RedirectResponse("/main-settings", status_code=303)

@router.post("/admin/bots/update_tokens")
async def admin_update_tokens(request: Request, bot_tokens: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    tokens = [t.strip() for t in bot_tokens.replace("\n", ",").split(",") if t.strip()]
    await reload_bot_pool(tokens)
    return RedirectResponse("/main-settings", status_code=303)

@router.post("/admin/bots/speedtest")
async def admin_speed_test(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    try:
        result = await speed_test()
    except Exception as e:
        result = {"ok": False, "error": str(e)}
    return await _render_main_settings(request, user, speed_result=result)

@router.post("/admin/tmdb/refresh")
async def admin_refresh_tmdb(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    if not getattr(settings, "TMDB_API_KEY", ""):
        return RedirectResponse("/main-settings?tmdb=missing", status_code=303)
    # Fire-and-forget; refresh can take time for large catalogs.
    asyncio.create_task(refresh_tmdb_metadata(limit=None))
    return RedirectResponse("/main-settings?tmdb=refresh_started", status_code=303)

@router.get("/settings")
async def admin_settings_redirect(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    return RedirectResponse("/main-settings")

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
    return RedirectResponse("/main-settings", status_code=303)

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
    await sync_content_catalog(force=True)
    return RedirectResponse("/dashboard", status_code=303)

@router.post("/admin/request/{request_id}/{action}")
async def update_content_request(
    request: Request,
    request_id: str,
    action: str,
    selected_content_id: str = Form(""),
    selected_content_title: str = Form(""),
    selected_content_type: str = Form(""),
    selected_content_path: str = Form(""),
    return_to: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    row = await ContentRequest.get(request_id)
    if not row:
        raise HTTPException(404)
    action = (action or "").lower()
    if action not in ("fulfilled", "rejected", "pending"):
        raise HTTPException(400)

    selected_content_id = (selected_content_id or "").strip()
    selected_content_title = (selected_content_title or "").strip()
    selected_content_type = (selected_content_type or "").strip().lower()
    selected_content_path = (selected_content_path or "").strip()

    if action == "fulfilled":
        if selected_content_id and (not selected_content_path or not selected_content_title):
            picked = await _find_group_by_item_id(selected_content_id)
            if picked:
                selected_content_title = selected_content_title or (picked.get("title") or "")
                selected_content_type = selected_content_type or (picked.get("type") or "")
                selected_content_path = selected_content_path or (picked.get("content_path") or "")
        if not selected_content_path.startswith("/content/details/"):
            target = (return_to or "/dashboard").strip()
            if not target.startswith("/"):
                target = "/dashboard"
            sep = "&" if "?" in target else "?"
            return RedirectResponse(f"{target}{sep}request=missing_content", status_code=303)
        row.fulfilled_content_id = selected_content_id or None
        row.fulfilled_content_title = selected_content_title or None
        row.fulfilled_content_type = selected_content_type or None
        row.fulfilled_content_path = selected_content_path
    else:
        row.fulfilled_content_id = None
        row.fulfilled_content_title = None
        row.fulfilled_content_type = None
        row.fulfilled_content_path = None

    row.status = action
    row.updated_at = datetime.now()
    await row.save()
    target = (return_to or "/dashboard").strip()
    if not target.startswith("/"):
        target = "/dashboard"
    return RedirectResponse(target, status_code=303)

@router.post("/admin/delete_user")
async def delete_user(request: Request, user_phone: str = Form(...), return_to: str = Form("")):
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
    
    target = (return_to or "/dashboard").strip()
    if not target.startswith("/"):
        target = "/dashboard"
    return RedirectResponse(target, status_code=303)


@router.post("/admin/user/set-role")
async def set_user_role(
    request: Request,
    user_phone: str = Form(...),
    role: str = Form("user"),
    return_to: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    role_value = (role or "user").strip().lower()
    if role_value not in ("user", "admin"):
        role_value = "user"
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.role = role_value
        target.role_requested = role_value
        await target.save()
    return RedirectResponse(return_to or "/dashboard", status_code=303)

@router.post("/admin/approve_user")
async def approve_user(request: Request, user_phone: str = Form(...), return_to: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "approved"
        target.approved_at = datetime.now()
        requested_role = str(getattr(target, "role_requested", "") or "").strip().lower()
        if requested_role == "admin":
            target.role = "admin"
        elif not getattr(target, "role", ""):
            target.role = "user"
        await target.save()
    target = (return_to or "/dashboard").strip()
    if not target.startswith("/"):
        target = "/dashboard"
    return RedirectResponse(target, status_code=303)

@router.post("/admin/block_user")
async def block_user(request: Request, user_phone: str = Form(...), return_to: str = Form("")):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "blocked"
        await target.save()
    target = (return_to or "/dashboard").strip()
    if not target.startswith("/"):
        target = "/dashboard"
    return RedirectResponse(target, status_code=303)
