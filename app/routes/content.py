import re
import uuid
from datetime import datetime
from typing import Optional

from beanie.operators import Or
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from app.core.config import settings
from app.db.models import FileSystemItem, User, TokenSetting, WatchlistEntry, ContentRequest, SiteSettings
from app.routes.dashboard import get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg")
QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|480p|380p|360p)", re.I)
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")


def _normalize_phone(phone: str) -> str:
    return (phone or "").replace(" ", "")


def _is_admin(user: User | None) -> bool:
    if not user:
        return False
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))


def _is_video(item: FileSystemItem) -> bool:
    if item.is_folder:
        return False
    mime = (item.mime_type or "").lower()
    if mime.startswith("video"):
        return True
    return (item.name or "").lower().endswith(VIDEO_EXTS)


def _infer_type(item: FileSystemItem) -> str:
    catalog_type = (getattr(item, "catalog_type", "") or "").lower().strip()
    if catalog_type in ("movie", "series"):
        return catalog_type
    if SE_RE.search(item.name or ""):
        return "series"
    return "movie"


def _infer_quality(name: str) -> str:
    m = QUALITY_RE.search(name or "")
    return m.group(1).upper() if m else "HD"


def _clean_title(name: str) -> str:
    base = re.sub(r"\.[^.]+$", "", name or "")
    base = re.sub(r"[._]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def _series_key(name: str) -> str:
    t = _clean_title(name)
    t = SE_RE.sub("", t)
    t = re.sub(QUALITY_RE, "", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def _season_episode(name: str) -> tuple[int, int]:
    m = SE_RE.search(name or "")
    if not m:
        return 1, 1
    return int(m.group(1)), int(m.group(2))


def _item_card(item: FileSystemItem) -> dict:
    name = item.name or ""
    item_type = _infer_type(item)
    season, episode = _season_episode(name)
    return {
        "id": str(item.id),
        "name": name,
        "title": _clean_title(name),
        "type": item_type,
        "quality": _infer_quality(name),
        "season": season,
        "episode": episode,
        "series_key": _series_key(name),
        "poster": getattr(item, "poster_url", "") or "",
        "backdrop": getattr(item, "backdrop_url", "") or "",
        "description": getattr(item, "description", "") or "",
        "year": getattr(item, "year", "") or "",
        "genres": getattr(item, "genres", []) or [],
        "actors": getattr(item, "actors", []) or [],
        "director": getattr(item, "director", "") or "",
        "trailer_url": getattr(item, "trailer_url", "") or "",
        "size": item.size or 0,
    }


async def _get_link_token() -> str:
    token = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if not token:
        token = TokenSetting(key="link_token", value=str(uuid.uuid4()))
        await token.insert()
    return token.value


async def _site_settings() -> SiteSettings:
    row = await SiteSettings.find_one(SiteSettings.key == "main")
    if not row:
        row = SiteSettings(key="main")
        await row.insert()
    return row


def _content_query(user: User | None, is_admin: bool):
    if is_admin:
        return {}
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if user:
        return {
            "$or": [
                {"owner_phone": admin_phone},
                {"owner_phone": user.phone_number},
                {"collaborators": user.phone_number},
            ]
        }
    return {"owner_phone": admin_phone}


async def _fetch_cards(user: User | None, is_admin: bool, limit: int = 300) -> list[dict]:
    query = _content_query(user, is_admin)
    items = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        query
    ).sort("-created_at").limit(limit).to_list()
    cards = [_item_card(i) for i in items if _is_video(i)]
    return cards


@router.get("/")
async def home_page(request: Request):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    cards = await _fetch_cards(user, is_admin, limit=220)
    movies = [c for c in cards if c["type"] == "movie"][:24]
    series = [c for c in cards if c["type"] == "series"][:24]
    trending = cards[:18]
    return templates.TemplateResponse("home.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "trending": trending,
        "movies": movies,
        "series": series,
    })


@router.get("/content")
async def content_all(request: Request, q: str = ""):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    cards = await _fetch_cards(user, is_admin, limit=500)
    q = (q or "").strip().lower()
    if q:
        cards = [c for c in cards if q in c["title"].lower()]
    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "title": "All Content",
        "cards": cards,
        "active_tab": "all",
        "query": q,
    })


@router.get("/content/movies")
async def content_movies(request: Request, q: str = ""):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    cards = await _fetch_cards(user, is_admin, limit=500)
    cards = [c for c in cards if c["type"] == "movie"]
    q = (q or "").strip().lower()
    if q:
        cards = [c for c in cards if q in c["title"].lower()]
    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "title": "Movies",
        "cards": cards,
        "active_tab": "movies",
        "query": q,
    })


@router.get("/content/web-series")
async def content_series(request: Request, q: str = ""):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    cards = await _fetch_cards(user, is_admin, limit=800)
    cards = [c for c in cards if c["type"] == "series"]
    q = (q or "").strip().lower()
    if q:
        cards = [c for c in cards if q in c["title"].lower()]
    # Collapse duplicates by series key for catalog page.
    dedup = {}
    for c in cards:
        k = c["series_key"] or c["title"].lower()
        if k not in dedup:
            dedup[k] = c
    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "title": "Web Series",
        "cards": list(dedup.values()),
        "active_tab": "series",
        "query": q,
    })


@router.get("/content/details/{item_id}")
async def content_details(request: Request, item_id: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    item = await FileSystemItem.get(item_id)
    if not item or not _is_video(item):
        raise HTTPException(status_code=404, detail="Content not found")

    query = _content_query(user, is_admin)
    if query:
        allowed = False
        if "$or" in query:
            for cond in query["$or"]:
                if cond.get("owner_phone") and item.owner_phone == cond["owner_phone"]:
                    allowed = True
                if cond.get("collaborators") and cond["collaborators"] in (item.collaborators or []):
                    allowed = True
        else:
            allowed = item.owner_phone == query.get("owner_phone")
        if not allowed and not is_admin:
            return RedirectResponse("/login")

    site = await _site_settings()
    card = _item_card(item)
    cards = await _fetch_cards(user, is_admin, limit=250)
    related = []
    if card["type"] == "series":
        key = card["series_key"]
        eps = [c for c in cards if c["type"] == "series" and c["series_key"] == key]
        eps.sort(key=lambda x: (x["season"], x["episode"]))
        related = eps
    else:
        related = [c for c in cards if c["type"] == "movie" and c["id"] != card["id"]][:14]

    if not item.share_token:
        item.share_token = str(uuid.uuid4())
        await item.save()
    token = item.share_token
    link_token = await _get_link_token()
    watch_url = f"/w/{token}?t={link_token}&U="
    telegram_url = f"/t/{token}?t={link_token}&U="
    download_url = f"/d/{token}?t={link_token}&U="
    stream_url = f"/player/{item_id}"

    watchlisted = False
    if user:
        found = await WatchlistEntry.find_one(WatchlistEntry.user_phone == user.phone_number, WatchlistEntry.item_id == str(item.id))
        watchlisted = found is not None

    return templates.TemplateResponse("content_details.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "item": card,
        "episodes": related if card["type"] == "series" else [],
        "related": related if card["type"] == "movie" else [],
        "stream_url": stream_url,
        "download_url": download_url,
        "telegram_url": telegram_url,
        "watch_url": watch_url,
        "watchlisted": watchlisted,
    })


@router.get("/content/search")
async def content_search(request: Request, q: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    q = (q or "").strip().lower()
    if not q:
        return {"items": []}
    cards = await _fetch_cards(user, is_admin, limit=600)
    items = [c for c in cards if q in c["title"].lower()]
    return {"items": items[:25]}


@router.post("/content/watchlist/toggle/{item_id}")
async def toggle_watchlist(request: Request, item_id: str):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Login required"}, status_code=401)
    row = await WatchlistEntry.find_one(
        WatchlistEntry.user_phone == user.phone_number,
        WatchlistEntry.item_id == item_id,
    )
    if row:
        await row.delete()
        return {"status": "removed"}
    row = WatchlistEntry(user_phone=user.phone_number, item_id=item_id)
    await row.insert()
    return {"status": "added"}


@router.get("/content/watchlist")
async def content_watchlist(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    is_admin = _is_admin(user)
    site = await _site_settings()
    rows = await WatchlistEntry.find(WatchlistEntry.user_phone == user.phone_number).sort("-created_at").to_list()
    ids = [r.item_id for r in rows]
    cards = []
    for item_id in ids:
        item = await FileSystemItem.get(item_id)
        if item and _is_video(item):
            cards.append(_item_card(item))
    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "title": "My Watchlist",
        "cards": cards,
        "active_tab": "watchlist",
        "query": "",
    })


@router.post("/content/request")
async def request_content(request: Request, title: str = Form(...), request_type: str = Form("movie"), note: str = Form("")):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    row = ContentRequest(
        user_phone=user.phone_number,
        user_name=user.first_name or user.phone_number,
        title=(title or "").strip(),
        request_type=(request_type or "movie").strip().lower(),
        note=(note or "").strip(),
        status="pending",
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    await row.insert()
    return RedirectResponse("/content?requested=1", status_code=303)


@router.get("/content/season-download/{series_key}/{season_no}", response_class=HTMLResponse)
async def season_download_page(request: Request, series_key: str, season_no: int):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    is_admin = _is_admin(user)
    cards = await _fetch_cards(user, is_admin, limit=1200)
    season_eps = [
        c for c in cards
        if c["type"] == "series" and c["series_key"] == (series_key or "").lower() and c["season"] == season_no
    ]
    season_eps.sort(key=lambda x: x["episode"])
    return templates.TemplateResponse("season_download.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "series_key": series_key,
        "season_no": season_no,
        "episodes": season_eps,
    })
