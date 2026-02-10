import re
import uuid
import json
import asyncio
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional

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
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
TRASH_RE = re.compile(
    r"(x264|x265|h\.?264|h\.?265|hevc|aac|dts|hdrip|webrip|webdl|bluray|brrip|dvdrip|hdts|hdtc|cam|line|"
    r"dual|multi|hindi|english|telugu|tamil|malayalam|punjabi|subbed|subs|proper|repack|uncut|"
    r"yts|rarbg|evo|mkv|mp4|avi)",
    re.I,
)
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


def _parse_name(name: str) -> dict:
    raw = name or ""
    cleaned = _clean_title(raw)
    year = ""
    m_year = YEAR_RE.search(cleaned)
    if m_year:
        year = m_year.group(1)
        cleaned = cleaned.replace(year, "")
    quality = _infer_quality(raw)
    cleaned = QUALITY_RE.sub("", cleaned)
    cleaned = TRASH_RE.sub("", cleaned)
    cleaned = re.sub(r"[\[\]\(\)\-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    season, episode = _season_episode(raw)
    is_series = bool(SE_RE.search(raw))
    title = cleaned.title() if cleaned else _clean_title(raw)
    return {
        "title": title,
        "title_key": title.lower(),
        "year": year,
        "quality": quality,
        "is_series": is_series,
        "season": season,
        "episode": episode,
    }


async def _tmdb_get(path: str, params: dict) -> dict:
    if not settings.TMDB_API_KEY:
        return {}
    params = params.copy()
    params["api_key"] = settings.TMDB_API_KEY
    url = "https://api.themoviedb.org/3" + path + "?" + urllib.parse.urlencode(params)
    def _fetch():
        with urllib.request.urlopen(url) as resp:
            return json.loads(resp.read().decode("utf-8"))
    return await asyncio.to_thread(_fetch)


async def _tmdb_search(title: str, year: str, is_series: bool) -> dict:
    path = "/search/tv" if is_series else "/search/movie"
    params = {"query": title}
    if year and not is_series:
        params["year"] = year
    if year and is_series:
        params["first_air_date_year"] = year
    return await _tmdb_get(path, params)


async def _tmdb_details(tmdb_id: int, is_series: bool) -> dict:
    path = f"/tv/{tmdb_id}" if is_series else f"/movie/{tmdb_id}"
    return await _tmdb_get(path, {"append_to_response": "videos,credits"})


async def _enrich_group(group: dict) -> dict:
    if not settings.TMDB_API_KEY:
        return group
    if group.get("description") or group.get("poster") or group.get("backdrop"):
        return group
    search = await _tmdb_search(group["title"], group.get("year", ""), group["type"] == "series")
    results = (search or {}).get("results") or []
    if not results:
        return group
    pick = results[0]
    tmdb_id = pick.get("id")
    if not tmdb_id:
        return group
    details = await _tmdb_details(tmdb_id, group["type"] == "series")
    if not details:
        return group

    poster = details.get("poster_path")
    backdrop = details.get("backdrop_path")
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

    base = "https://image.tmdb.org/t/p/w780"
    group["poster"] = base + poster if poster else group.get("poster", "")
    group["backdrop"] = base + backdrop if backdrop else group.get("backdrop", "")
    group["description"] = overview or group.get("description", "")
    group["year"] = year or group.get("year", "")
    group["genres"] = genres or group.get("genres", [])
    group["actors"] = cast or group.get("actors", [])
    group["director"] = director or group.get("director", "")
    group["trailer_url"] = trailer or group.get("trailer_url", "")
    return group


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
    info = _parse_name(name)
    item_type = "series" if info["is_series"] else "movie"
    season, episode = info["season"], info["episode"]
    return {
        "id": str(item.id),
        "name": name,
        "title": info["title"],
        "type": item_type,
        "quality": info["quality"],
        "season": season,
        "episode": episode,
        "series_key": _series_key(name),
        "poster": getattr(item, "poster_url", "") or "",
        "backdrop": getattr(item, "backdrop_url", "") or "",
        "description": getattr(item, "description", "") or "",
        "year": info["year"] or getattr(item, "year", "") or "",
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


async def _ensure_share_token(file_id: str) -> str:
    item = await FileSystemItem.get(file_id)
    if not item:
        return ""
    if not item.share_token:
        item.share_token = str(uuid.uuid4())
        await item.save()
    return item.share_token


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


async def _build_catalog(user: User | None, is_admin: bool, limit: int = 1200) -> list[dict]:
    cards = await _fetch_cards(user, is_admin, limit=limit)
    groups = {}
    for c in cards:
        key = (c["title"].lower(), c["year"], c["type"])
        if key not in groups:
            groups[key] = {
                "id": c["id"],
                "title": c["title"],
                "year": c["year"],
                "type": c["type"],
                "poster": c["poster"],
                "backdrop": c["backdrop"],
                "description": c["description"],
                "genres": c["genres"],
                "actors": c["actors"],
                "director": c["director"],
                "trailer_url": c["trailer_url"],
                "qualities": {},
                "seasons": {},
                "items": [],
            }
        groups[key]["items"].append(c)
        if c["type"] == "movie":
            groups[key]["qualities"][c["quality"]] = {"file_id": c["id"], "size": c["size"]}
        else:
            season = c["season"]
            episode = c["episode"]
            season_bucket = groups[key]["seasons"].setdefault(season, {})
            ep_bucket = season_bucket.setdefault(episode, {})
            ep_bucket[c["quality"]] = {"file_id": c["id"], "size": c["size"]}
    def _quality_rank(q: str) -> int:
        order = {"2160P": 5, "1440P": 4, "1080P": 3, "720P": 2, "480P": 1, "380P": 0, "360P": 0}
        return order.get((q or "").upper(), 0)

    result = []
    for g in groups.values():
        if g["type"] == "movie":
            qualities = sorted(g["qualities"].keys(), key=_quality_rank, reverse=True)
            g["primary_quality"] = qualities[0] if qualities else "HD"
        else:
            g["season_count"] = len(g["seasons"])
            g["primary_quality"] = "S" + str(min(g["seasons"].keys())) if g["seasons"] else "Series"
        result.append(g)
    return result


@router.get("/")
async def home_page(request: Request):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    catalog = await _build_catalog(user, is_admin, limit=400)
    movies = [c for c in catalog if c["type"] == "movie"][:24]
    series = [c for c in catalog if c["type"] == "series"][:24]
    trending = catalog[:18]
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
    cards = await _build_catalog(user, is_admin, limit=800)
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
    cards = await _build_catalog(user, is_admin, limit=800)
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
    cards = await _build_catalog(user, is_admin, limit=1200)
    cards = [c for c in cards if c["type"] == "series"]
    q = (q or "").strip().lower()
    if q:
        cards = [c for c in cards if q in c["title"].lower()]
    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "title": "Web Series",
        "cards": cards,
        "active_tab": "series",
        "query": q,
    })


@router.get("/content/details/{item_id}")
async def content_details(request: Request, item_id: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    catalog = await _build_catalog(user, is_admin, limit=1500)
    group = None
    for g in catalog:
        if g["id"] == item_id:
            group = g
            break
        for itm in g["items"]:
            if itm["id"] == item_id:
                group = g
                break
        if group:
            break
    if not group:
        raise HTTPException(status_code=404, detail="Content not found")

    site = await _site_settings()
    group = await _enrich_group(group)
    link_token = await _get_link_token()

    viewer_name = ""
    if user:
        viewer_name = user.first_name or user.phone_number

    qualities = []
    if group["type"] == "movie":
        for q, v in group["qualities"].items():
            token = await _ensure_share_token(v["file_id"])
            qualities.append({
                "label": q,
                "size": v["size"],
                "stream_url": f"/player/{v['file_id']}",
                "download_url": f"/s/stream/file/{v['file_id']}?download=true",
                "telegram_url": f"/t/{token}?t={link_token}" if token else "",
                "watch_url": f"/w/{token}?t={link_token}" if token else "",
            })

    seasons = []
    if group["type"] == "series":
        for s_no, eps in sorted(group["seasons"].items(), key=lambda x: x[0]):
            # aggregate sizes per quality
            quality_totals = {}
            for ep_no, variants in eps.items():
                for q, v in variants.items():
                    quality_totals[q] = quality_totals.get(q, 0) + (v["size"] or 0)
                    if "token" not in v:
                        v["token"] = await _ensure_share_token(v["file_id"])
            seasons.append({
                "season": s_no,
                "qualities": [
                    {"label": q, "size": size}
                    for q, size in sorted(quality_totals.items(), key=lambda x: x[0], reverse=True)
                ],
                "episodes": eps,
            })

    watchlisted = False
    if user:
        found = await WatchlistEntry.find_one(WatchlistEntry.user_phone == user.phone_number, WatchlistEntry.item_id == group["id"])
        watchlisted = found is not None

    return templates.TemplateResponse("content_details.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "item": group,
        "qualities": qualities,
        "seasons": seasons,
        "watchlisted": watchlisted,
        "link_token": link_token,
        "viewer_name": viewer_name,
    })


@router.get("/content/search")
async def content_search(request: Request, q: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    q = (q or "").strip().lower()
    if not q:
        return {"items": []}
    cards = await _build_catalog(user, is_admin, limit=1200)
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
    ids = {r.item_id for r in rows}
    cards = await _build_catalog(user, is_admin, limit=1200)
    cards = [c for c in cards if c["id"] in ids]
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
    catalog = await _build_catalog(user, is_admin, limit=1500)
    season_eps = []
    for g in catalog:
        if g["type"] != "series":
            continue
        key = (g["title"] or "").lower()
        if key != (series_key or "").lower():
            continue
        for ep_no, variants in g["seasons"].get(season_no, {}).items():
            for q, v in variants.items():
                season_eps.append({
                    "episode": ep_no,
                    "title": g["title"],
                    "quality": q,
                    "id": v["file_id"],
                    "size": v["size"],
                })
    season_eps.sort(key=lambda x: (x["episode"], x["quality"]))
    return templates.TemplateResponse("season_download.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "series_key": series_key,
        "season_no": season_no,
        "episodes": season_eps,
    })
