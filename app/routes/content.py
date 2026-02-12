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
from beanie.operators import In

from app.core.config import settings
from app.db.models import FileSystemItem, User, TokenSetting, WatchlistEntry, ContentRequest, SiteSettings
from app.routes.dashboard import get_current_user
from app.utils.file_utils import format_size

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".mov", ".avi", ".mpeg", ".mpg")
QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|480p|380p|360p)", re.I)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
TRASH_RE = re.compile(
    r"(x264|x265|h\.?264|h\.?265|hevc|aac|dts|hdrip|webrip|webdl|bluray|brrip|dvdrip|hdts|hdtc|cam|line|"
    r"dual|multi|hindi|english|telugu|tamil|malayalam|punjabi|subbed|subs|proper|repack|uncut|"
    r"yts|rarbg|evo|hdhub4u|hdhub|v1|v2|v3|mkv|mp4|avi)",
    re.I,
)
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")
SEASON_TAG_RE = re.compile(r"\bS\d{1,2}E\d{1,3}\b|\bS\d{1,2}\b|\bE\d{1,3}\b|\bSeason\s?\d{1,2}\b|\bEpisode\s?\d{1,3}\b", re.I)

def _quality_rank(q: str) -> int:
    order = {"2160P": 5, "1440P": 4, "1080P": 3, "720P": 2, "480P": 1, "380P": 0, "360P": 0}
    return order.get((q or "").upper(), 0)

def _viewer_name(user: User | None) -> str:
    if not user:
        return ""
    return (user.requested_name or user.first_name or user.phone_number or "").strip()

def _share_params(link_token: str, viewer_name: str) -> str:
    params = f"t={link_token}" if link_token else ""
    if viewer_name:
        safe_name = urllib.parse.quote(viewer_name)
        params = f"{params}&U={safe_name}" if params else f"U={safe_name}"
    return params


def _normalize_phone(phone: str) -> str:
    return (phone or "").replace(" ", "")


def _is_admin(user: User | None) -> bool:
    if not user:
        return False
    if str(getattr(user, "role", "") or "").strip().lower() == "admin":
        return True
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


def _tokenize_search(text: str) -> list[str]:
    return [t for t in re.split(r"[^a-zA-Z0-9]+", (text or "").lower()) if t]


def _build_search_regex(text: str) -> str:
    tokens = _tokenize_search(text)
    if not tokens:
        return ""
    return ".*".join(re.escape(token) for token in tokens)


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
    cleaned = SEASON_TAG_RE.sub("", cleaned)
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
    release_date = (details.get("release_date") or details.get("first_air_date") or "")
    year = release_date[:4] if release_date else ""
    genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
    credits = details.get("credits") or {}
    cast_rows = credits.get("cast") or []
    cast = [c.get("name") for c in cast_rows[:8] if c.get("name")]
    profile_base = "https://image.tmdb.org/t/p/w185"
    cast_profiles = []
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
    if not director and group["type"] == "series":
        created_by = details.get("created_by") or []
        if created_by:
            director = created_by[0].get("name") or ""
        if not director:
            for crew in credits.get("crew") or []:
                if crew.get("job") in ("Creator", "Executive Producer"):
                    director = crew.get("name") or ""
                    break

    trailer = ""
    trailer_key = ""
    for v in details.get("videos", {}).get("results", []):
        if v.get("site") == "YouTube" and v.get("type") in ("Trailer", "Teaser"):
            trailer_key = v.get("key") or ""
            if trailer_key:
                trailer = f"https://www.youtube.com/watch?v={trailer_key}"
            break

    tmdb_title = details.get("name") if group["type"] == "series" else details.get("title")
    if tmdb_title:
        group["title"] = tmdb_title

    base = "https://image.tmdb.org/t/p/w780"
    group["poster"] = base + poster if poster else group.get("poster", "")
    group["backdrop"] = base + backdrop if backdrop else group.get("backdrop", "")
    group["description"] = overview or group.get("description", "")
    group["year"] = year or group.get("year", "")
    group["release_date"] = release_date or group.get("release_date", "")
    group["genres"] = genres or group.get("genres", [])
    group["actors"] = cast or group.get("actors", [])
    group["director"] = director or group.get("director", "")
    group["trailer_url"] = trailer or group.get("trailer_url", "")
    if trailer_key:
        group["trailer_key"] = trailer_key
    group["cast_profiles"] = cast_profiles or group.get("cast_profiles", [])
    return group

async def _persist_group_metadata(group: dict):
    update = {}
    if group.get("poster"):
        update["poster_url"] = group["poster"]
    if group.get("backdrop"):
        update["backdrop_url"] = group["backdrop"]
    if group.get("description"):
        update["description"] = group["description"]
    if group.get("year"):
        update["year"] = group["year"]
    if group.get("release_date"):
        update["release_date"] = group["release_date"]
    if group.get("genres"):
        update["genres"] = group["genres"]
    if group.get("actors"):
        update["actors"] = group["actors"]
    if group.get("director"):
        update["director"] = group["director"]
    if group.get("trailer_url"):
        update["trailer_url"] = group["trailer_url"]
    if group.get("trailer_key"):
        update["trailer_key"] = group["trailer_key"]
    if group.get("cast_profiles"):
        update["cast_profiles"] = group["cast_profiles"]
    title_value = (group.get("title") or "").strip()
    group_type = (group.get("type") or "").strip().lower()
    if not update and not title_value and not group_type:
        return

    for item in group.get("items", []):
        db_item = await FileSystemItem.get(item["id"])
        if not db_item:
            continue
        changed = False
        if group_type in ("movie", "series") and not getattr(db_item, "catalog_type", ""):
            db_item.catalog_type = group_type
            changed = True
        if title_value:
            if group_type == "series":
                if not getattr(db_item, "series_title", ""):
                    db_item.series_title = title_value
                    changed = True
                current_title = getattr(db_item, "title", "") or ""
                parsed_title = _parse_name(db_item.name or "").get("title", "")
                if not current_title or current_title.strip().lower() == parsed_title.strip().lower():
                    db_item.title = title_value
                    changed = True
            else:
                current_title = getattr(db_item, "title", "") or ""
                parsed_title = _parse_name(db_item.name or "").get("title", "")
                if not current_title or current_title.strip().lower() == parsed_title.strip().lower():
                    db_item.title = title_value
                    changed = True
        if update.get("poster_url") and not getattr(db_item, "poster_url", ""):
            db_item.poster_url = update["poster_url"]
            changed = True
        if update.get("backdrop_url") and not getattr(db_item, "backdrop_url", ""):
            db_item.backdrop_url = update["backdrop_url"]
            changed = True
        if update.get("description") and not getattr(db_item, "description", ""):
            db_item.description = update["description"]
            changed = True
        if update.get("year") and not getattr(db_item, "year", ""):
            db_item.year = update["year"]
            changed = True
        if update.get("release_date") and not getattr(db_item, "release_date", ""):
            db_item.release_date = update["release_date"]
            changed = True
        if update.get("genres") and not getattr(db_item, "genres", []):
            db_item.genres = update["genres"]
            changed = True
        if update.get("actors") and not getattr(db_item, "actors", []):
            db_item.actors = update["actors"]
            changed = True
        if update.get("director") and not getattr(db_item, "director", ""):
            db_item.director = update["director"]
            changed = True
        if update.get("trailer_url") and not getattr(db_item, "trailer_url", ""):
            db_item.trailer_url = update["trailer_url"]
            changed = True
        if update.get("trailer_key") and not getattr(db_item, "trailer_key", ""):
            db_item.trailer_key = update["trailer_key"]
            changed = True
        if update.get("cast_profiles") and not getattr(db_item, "cast_profiles", []):
            db_item.cast_profiles = update["cast_profiles"]
            changed = True
        if changed:
            await db_item.save()

async def _ensure_group_assets(group: dict) -> dict:
    if not settings.TMDB_API_KEY:
        return group
    missing = (
        not group.get("description")
        or not group.get("poster")
        or not group.get("backdrop")
        or not group.get("genres")
        or not group.get("actors")
        or not group.get("director")
        or not group.get("trailer_url")
        or not group.get("cast_profiles")
    )
    if not missing:
        return group
    await _enrich_group(group)
    await _persist_group_metadata(group)
    return group

async def refresh_tmdb_metadata(limit: int | None = None) -> dict:
    if not settings.TMDB_API_KEY:
        return {"ok": False, "error": "TMDB_API_KEY not configured"}
    try:
        max_limit = limit if limit is not None else 2000
        catalog = await _build_catalog(None, True, limit=max_limit)
        updated = 0
        total = 0
        for group in catalog:
            total += 1
            before = bool(group.get("poster") or group.get("backdrop") or group.get("description"))
            await _ensure_group_assets(group)
            after = bool(group.get("poster") or group.get("backdrop") or group.get("description"))
            if after and not before:
                updated += 1
        return {"ok": True, "updated": updated, "total": total}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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


def _slugify(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _group_slug(title: str, year: str) -> str:
    base = _slugify(title)
    year = (year or "").strip()
    if year:
        return f"{base}-{year}"
    return base


def _youtube_key(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""

    # Already a plain YouTube video key
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw):
        return raw

    # Embedded iframe or HTML snippets: extract src first
    src_match = re.search(r"""src=["']([^"']+)["']""", raw, re.I)
    if src_match:
        raw = src_match.group(1).strip()

    if raw.startswith("//"):
        raw = "https:" + raw
    if raw.startswith("www."):
        raw = "https://" + raw

    try:
        parsed = urllib.parse.urlparse(raw)
        host = (parsed.netloc or "").lower()
        if "youtu.be" in host:
            key = parsed.path.lstrip("/").split("?")[0].strip()
            return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
        if "youtube" in host:
            if parsed.path.startswith("/embed/"):
                key = parsed.path.split("/embed/")[-1].split("?")[0].strip()
                return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
            if parsed.path.startswith("/shorts/"):
                key = parsed.path.split("/shorts/")[-1].split("?")[0].strip()
                return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
            params = urllib.parse.parse_qs(parsed.query)
            if "v" in params and params["v"]:
                key = (params["v"][0] or "").strip()
                return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
    except Exception:
        pass
    # Fallback for raw URL/text that still contains a YouTube key
    match = re.search(r"(?:v=|\/embed\/|youtu\.be\/|\/shorts\/)([A-Za-z0-9_-]{11})", raw, re.I)
    if match:
        return match.group(1)
    return ""


def _item_card(item: FileSystemItem) -> dict:
    name = item.name or ""
    info = _parse_name(name)
    catalog_type = (getattr(item, "catalog_type", "") or "").lower().strip()
    item_type = catalog_type if catalog_type in ("movie", "series") else ("series" if info["is_series"] else "movie")
    season = getattr(item, "season", None) or info["season"]
    episode = getattr(item, "episode", None) or info["episode"]
    quality = getattr(item, "quality", "") or info["quality"]
    title_override = getattr(item, "title", "") or ""
    series_title = getattr(item, "series_title", "") or ""
    display_title = series_title or title_override or info["title"]
    return {
        "id": str(item.id),
        "name": name,
        "title": display_title,
        "type": item_type,
        "quality": quality,
        "season": season,
        "episode": episode,
        "episode_title": getattr(item, "episode_title", "") or "",
        "series_key": _series_key(series_title or display_title or name),
        "poster": getattr(item, "poster_url", "") or "",
        "backdrop": getattr(item, "backdrop_url", "") or "",
        "description": getattr(item, "description", "") or "",
        "year": (getattr(item, "year", "") or info["year"] or ""),
        "release_date": getattr(item, "release_date", "") or "",
        "genres": getattr(item, "genres", []) or [],
        "actors": getattr(item, "actors", []) or [],
        "director": getattr(item, "director", "") or "",
        "trailer_url": getattr(item, "trailer_url", "") or "",
        "trailer_key": getattr(item, "trailer_key", "") or "",
        "cast_profiles": getattr(item, "cast_profiles", []) or [],
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
    base = {"catalog_status": "published"}
    if is_admin:
        return base
    admin_phone = getattr(settings, "ADMIN_PHONE", "") or ""
    if user:
        base["$or"] = [
            {"owner_phone": admin_phone},
            {"owner_phone": user.phone_number},
            {"collaborators": user.phone_number},
        ]
        return base
    base["owner_phone"] = admin_phone
    return base


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
                "slug": _group_slug(c["title"], c["year"]),
                "release_date": c.get("release_date", ""),
                "type": c["type"],
                "poster": c["poster"],
                "backdrop": c["backdrop"],
                "description": c["description"],
                "genres": c["genres"],
                "actors": c["actors"],
                "director": c["director"],
                "trailer_url": c["trailer_url"],
                "trailer_key": c.get("trailer_key", ""),
                "cast_profiles": c.get("cast_profiles", []),
                "qualities": {},
                "seasons": {},
                "episode_titles": {},
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
            title_map = groups[key]["episode_titles"].setdefault(season, {})
            if c.get("episode_title"):
                title_map[episode] = c.get("episode_title")
    result = []
    for g in groups.values():
        if g["type"] == "movie":
            qualities = sorted(g["qualities"].keys(), key=_quality_rank, reverse=True)
            g["primary_quality"] = qualities[0] if qualities else "HD"
            g["quality"] = g["primary_quality"]
            g["card_labels"] = qualities[:3] if qualities else ["HD"]
        else:
            season_numbers = sorted(int(s) for s in g["seasons"].keys())
            g["season_count"] = len(season_numbers)
            g["primary_quality"] = f"S{season_numbers[0]:02d}" if season_numbers else "Series"
            g["quality"] = g["primary_quality"]
            labels = [f"Season {s}" for s in season_numbers[:3]]
            if g["season_count"] > 3:
                labels.append(f"+{g['season_count'] - 3} more")
            g["card_labels"] = labels or ["Series"]
        result.append(g)
    return result

async def _build_file_links(items: list[dict], link_token: str, viewer_name: str, limit: int = 3) -> list[dict]:
    if not items:
        return []
    viewer_name = (viewer_name or "").strip()
    params = _share_params(link_token, viewer_name) if viewer_name else ""
    needs_links = bool(viewer_name)

    def _sort_key(item: dict):
        if item.get("type") == "series":
            return (
                int(item.get("season") or 0),
                int(item.get("episode") or 0),
                -_quality_rank(item.get("quality")),
            )
        return (-_quality_rank(item.get("quality")), (item.get("name") or "").lower())

    ordered = sorted(items, key=_sort_key)
    links: list[dict] = []
    for item in ordered:
        if len(links) >= limit:
            break
        quality = (item.get("quality") or "").upper()
        label = quality or ""
        if item.get("type") == "series":
            season = int(item.get("season") or 0)
            episode = int(item.get("episode") or 0)
            if season and episode:
                label = f"S{season:02d}E{episode:02d} {quality or 'HD'}"
            else:
                label = quality or item.get("name") or "Episode"
        if not label:
            label = item.get("name") or "File"
        token = ""
        query = ""
        if needs_links:
            token = await _ensure_share_token(item["id"])
            if not token:
                continue
            query = f"?{params}" if params else ""
        links.append({
            "name": item.get("name") or item.get("title") or "File",
            "label": label,
            "view_url": f"/s/{token}{query}" if needs_links else "",
            "download_url": f"/d/{token}{query}" if needs_links else "",
            "telegram_url": f"/t/{token}{query}" if needs_links else "",
            "watch_url": f"/w/{token}{query}" if needs_links else "",
        })
    return links

async def _warm_group_assets(groups: list[dict], limit: int = 16):
    if not settings.TMDB_API_KEY:
        return
    warmed = 0
    for g in groups:
        if warmed >= limit:
            break
        if g.get("poster") or g.get("backdrop") or g.get("description"):
            continue
        await _ensure_group_assets(g)
        warmed += 1


@router.get("/")
async def home_page(request: Request):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    catalog = await _build_catalog(user, is_admin, limit=200)
    movies = [c for c in catalog if c["type"] == "movie"][:24]
    series = [c for c in catalog if c["type"] == "series"][:24]
    trending = catalog[:18]
    display_groups = trending + movies + series
    # Warm TMDB in background to keep homepage fast
    asyncio.create_task(_warm_group_assets(display_groups, limit=12))
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
    asyncio.create_task(_warm_group_assets(cards[:24], limit=8))
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
    asyncio.create_task(_warm_group_assets(cards[:24], limit=8))
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
    cards = await _build_catalog(user, is_admin, limit=3000)
    cards = [c for c in cards if c["type"] == "series"]
    q = (q or "").strip().lower()
    if q:
        cards = [c for c in cards if q in c["title"].lower()]
    asyncio.create_task(_warm_group_assets(cards[:24], limit=8))
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


@router.get("/content/details/{content_key}")
async def content_details(request: Request, content_key: str):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    catalog = await _build_catalog(user, is_admin, limit=1500)
    group = None
    content_key = (content_key or "").strip()
    slug_key = content_key.lower()
    is_object_id = bool(re.fullmatch(r"[0-9a-fA-F]{24}", content_key))

    if not is_object_id:
        for g in catalog:
            if (g.get("slug") or "") == slug_key:
                group = g
                break
        if not group:
            base = slug_key
            year = ""
            parts = slug_key.rsplit("-", 1)
            if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
                base, year = parts[0], parts[1]
            for g in catalog:
                if _slugify(g.get("title", "")) == base and (not year or (g.get("year") or "") == year):
                    group = g
                    break

    if not group:
        for g in catalog:
            if g["id"] == content_key:
                group = g
                break
            for itm in g["items"]:
                if itm["id"] == content_key:
                    group = g
                    break
            if group:
                break
    if not group:
        raise HTTPException(status_code=404, detail="Content not found")

    site = await _site_settings()
    group = await _ensure_group_assets(group)
    raw_trailer_key = _youtube_key(group.get("trailer_key") or "")
    trailer_embed = raw_trailer_key or _youtube_key(group.get("trailer_url") or "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", trailer_embed or ""):
        trailer_embed = ""
    group["trailer_embed"] = trailer_embed
    link_token = await _get_link_token()

    viewer_name = _viewer_name(user)
    share_params = _share_params(link_token, viewer_name) if viewer_name else ""

    qualities = []
    if group["type"] == "movie":
        movie_qualities = sorted(
            group["qualities"].items(),
            key=lambda x: (-_quality_rank(x[0]), x[0]),
        )
        for q, v in movie_qualities:
            token = await _ensure_share_token(v["file_id"])
            query = f"?{share_params}" if share_params else ""
            size_bytes = int(v.get("size") or 0)
            qualities.append({
                "label": q,
                "size": size_bytes,
                "size_label": format_size(size_bytes),
                "view_url": f"/s/{token}{query}" if token and share_params else "",
                "download_url": f"/d/{token}{query}" if token and share_params else "",
                "telegram_url": f"/t/{token}{query}" if token and share_params else "",
                "watch_url": f"/w/{token}{query}" if token and share_params else "",
                "admin_url": f"/player/{v['file_id']}",
                "file_id": v["file_id"],
            })

    seasons = []
    if group["type"] == "series":
        for s_no, eps in sorted(group["seasons"].items(), key=lambda x: int(x[0])):
            season_titles = (group.get("episode_titles", {}) or {}).get(s_no, {})
            season_qualities = set()
            episodes_payload = []
            for ep_no, variants in sorted(eps.items(), key=lambda x: int(x[0])):
                quality_rows = []
                max_size = 0
                for q, v in sorted(variants.items(), key=lambda x: (-_quality_rank(x[0]), x[0])):
                    token = await _ensure_share_token(v["file_id"])
                    query = f"?{share_params}" if share_params else ""
                    size_bytes = int(v.get("size") or 0)
                    max_size = max(max_size, size_bytes)
                    season_qualities.add(q)
                    quality_rows.append({
                        "label": q,
                        "size": size_bytes,
                        "size_label": format_size(size_bytes),
                        "view_url": f"/s/{token}{query}" if token and share_params else "",
                        "download_url": f"/d/{token}{query}" if token and share_params else "",
                        "telegram_url": f"/t/{token}{query}" if token and share_params else "",
                        "watch_url": f"/w/{token}{query}" if token and share_params else "",
                        "admin_url": f"/player/{v['file_id']}",
                        "file_id": v["file_id"],
                    })
                episode_num = int(ep_no)
                episodes_payload.append({
                    "episode": episode_num,
                    "title": season_titles.get(episode_num, "") or season_titles.get(str(episode_num), ""),
                    "qualities": quality_rows,
                    "quality_count": len(quality_rows),
                    "display_size": max_size,
                    "display_size_label": format_size(max_size),
                })
            seasons.append({
                "season": s_no,
                "qualities": sorted(season_qualities, key=lambda q: (-_quality_rank(q), q)),
                "episode_count": len(episodes_payload),
                "episodes": episodes_payload,
            })

    watchlisted = False
    if user:
        slug_key = (group.get("slug") or "").strip()
        keys = [group.get("id", "")]
        if slug_key:
            keys.append(f"slug:{slug_key}")
        keys = [k for k in keys if k]
        found = await WatchlistEntry.find_one(
            WatchlistEntry.user_phone == user.phone_number,
            In(WatchlistEntry.item_id, keys)
        )
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


@router.get("/content/search/suggestions")
async def content_search_suggestions(request: Request, q: str = ""):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    q = (q or "").strip()
    if len(q) < 2:
        return {"items": []}

    search_regex = _build_search_regex(q)
    if not search_regex:
        return {"items": []}

    base_query = _content_query(user, is_admin)
    query = {
        "$and": [
            base_query,
            {"is_folder": False},
            {
                "$or": [
                    {"title": {"$regex": search_regex, "$options": "i"}},
                    {"series_title": {"$regex": search_regex, "$options": "i"}},
                    {"name": {"$regex": search_regex, "$options": "i"}},
                ]
            },
        ]
    }
    rows = await FileSystemItem.find(query).sort("-created_at").limit(220).to_list()
    grouped: dict[tuple[str, str, str], dict] = {}
    for row in rows:
        if not _is_video(row):
            continue
        card = _item_card(row)
        key = (card["title"].lower(), card["year"], card["type"])
        if key in grouped:
            continue
        grouped[key] = {
            "title": card["title"],
            "year": card["year"],
            "type": card["type"],
            "poster": card["poster"],
            "slug": _group_slug(card["title"], card["year"]),
        }
        if len(grouped) >= 12:
            break
    items = sorted(grouped.values(), key=lambda x: (x["title"] or "").lower())
    return {"items": items[:10]}


@router.post("/content/watchlist/toggle/{item_id}")
async def toggle_watchlist(request: Request, item_id: str):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Login required"}, status_code=401)

    raw_key = (item_id or "").strip()
    if not raw_key:
        return JSONResponse({"error": "Content not found"}, status_code=404)

    primary_key = ""
    legacy_id = ""
    if re.fullmatch(r"[0-9a-fA-F]{24}", raw_key):
        legacy_id = raw_key
        primary_key = raw_key
        # Prefer stable slug key when possible.
        is_admin = _is_admin(user)
        catalog = await _build_catalog(user, is_admin, limit=3000)
        for group in catalog:
            if group.get("id") == raw_key or any((itm.get("id") == raw_key) for itm in (group.get("items") or [])):
                slug_val = (group.get("slug") or "").strip()
                if slug_val:
                    primary_key = f"slug:{slug_val}"
                break
    else:
        slug = raw_key.lower()
        primary_key = f"slug:{slug}"
        # Resolve legacy object id so old entries can still be removed cleanly.
        is_admin = _is_admin(user)
        catalog = await _build_catalog(user, is_admin, limit=3000)
        for group in catalog:
            if (group.get("slug") or "") == slug:
                legacy_id = group.get("id") or ""
                break
        if not legacy_id:
            return JSONResponse({"error": "Content not found"}, status_code=404)

    lookup_keys = [primary_key]
    if legacy_id and legacy_id not in lookup_keys:
        lookup_keys.append(legacy_id)

    existing_rows = await WatchlistEntry.find(
        WatchlistEntry.user_phone == user.phone_number,
        In(WatchlistEntry.item_id, lookup_keys),
    ).to_list()
    if existing_rows:
        for row in existing_rows:
            await row.delete()
        return {"status": "removed"}

    row = WatchlistEntry(user_phone=user.phone_number, item_id=primary_key)
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
    object_ids = set()
    slug_ids = set()
    for row in rows:
        key = (row.item_id or "").strip()
        if key.startswith("slug:"):
            slug_ids.add(key.split("slug:", 1)[-1])
        elif re.fullmatch(r"[0-9a-fA-F]{24}", key):
            object_ids.add(key)
    cards = await _build_catalog(user, is_admin, limit=1200)
    cards = [c for c in cards if c["id"] in object_ids or (c.get("slug") or "") in slug_ids]
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
async def request_content(
    request: Request,
    title: str = Form(...),
    request_type: str = Form("movie"),
    note: str = Form(""),
    return_to: str = Form("")
):
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
    target = (return_to or "").strip()
    if not target.startswith("/"):
        target = "/content"
    sep = "&" if "?" in target else "?"
    return RedirectResponse(f"{target}{sep}requested=1", status_code=303)


@router.get("/request-content")
async def request_content_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    is_admin = _is_admin(user)
    site = await _site_settings()
    my_requests = await ContentRequest.find(ContentRequest.user_phone == user.phone_number).sort("-created_at").limit(40).to_list()
    catalog = await _build_catalog(user, is_admin, limit=3000)
    by_id = {str(c.get("id")): c for c in catalog}
    for row in my_requests:
        raw_path = str(getattr(row, "fulfilled_content_path", "") or "").strip()
        if not raw_path:
            ref_id = str(getattr(row, "fulfilled_content_id", "") or "").strip()
            match = by_id.get(ref_id)
            if match:
                raw_path = f"/content/details/{match.get('slug') or match.get('id')}"
                if not getattr(row, "fulfilled_content_title", ""):
                    setattr(row, "fulfilled_content_title", match.get("title") or "")
        if raw_path and not raw_path.startswith("/") and not re.match(r"^https?://", raw_path, re.I):
            raw_path = "/" + raw_path.lstrip("/")
        setattr(row, "view_path", raw_path)
    return templates.TemplateResponse("request_content.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "requests": my_requests,
    })


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
