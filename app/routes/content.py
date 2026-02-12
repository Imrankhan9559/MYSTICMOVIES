import re
import uuid
import json
import asyncio
import html
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from beanie.operators import In

from app.core.config import settings
from app.core.content_store import build_content_groups, sync_content_catalog
from app.db.models import ContentItem, FileSystemItem, User, TokenSetting, WatchlistEntry, ContentRequest, SiteSettings
from app.routes.dashboard import get_current_user
from app.utils.file_utils import format_size

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
CATALOG_ITEMS_PER_PAGE = 24

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
        cast_profiles.append({"id": c.get("id"), "name": name, "role": role, "image": image})

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
    doc = None
    raw_id = (group.get("id") or "").strip()
    if raw_id and re.fullmatch(r"[0-9a-fA-F]{24}", raw_id):
        try:
            doc = await ContentItem.get(raw_id)
        except Exception:
            doc = None
    if not doc:
        slug = (group.get("slug") or _group_slug(group.get("title", ""), group.get("year", ""))).strip()
        ctype = (group.get("type") or "movie").strip().lower()
        if slug:
            doc = await ContentItem.find_one(ContentItem.slug == slug, ContentItem.content_type == ctype)
    if not doc:
        # Keep the dedicated content collection in sync before a final lookup.
        await sync_content_catalog(force=True)
        raw_id = (group.get("id") or "").strip()
        if raw_id and re.fullmatch(r"[0-9a-fA-F]{24}", raw_id):
            try:
                doc = await ContentItem.get(raw_id)
            except Exception:
                doc = None
    if not doc:
        return

    changed = False
    title = (group.get("title") or "").strip()
    year = (group.get("year") or "").strip()
    slug = (group.get("slug") or _group_slug(title, year)).strip()
    ctype = (group.get("type") or doc.content_type or "movie").strip().lower()

    if title and title != (doc.title or ""):
        doc.title = title
        changed = True
    if year and year != (doc.year or ""):
        doc.year = year
        changed = True
    if slug and slug != (doc.slug or ""):
        doc.slug = slug
        changed = True
    if ctype in ("movie", "series") and ctype != (doc.content_type or ""):
        doc.content_type = ctype
        changed = True

    str_updates = {
        "poster_url": group.get("poster", ""),
        "backdrop_url": group.get("backdrop", ""),
        "description": group.get("description", ""),
        "release_date": group.get("release_date", ""),
        "director": group.get("director", ""),
        "trailer_url": group.get("trailer_url", ""),
        "trailer_key": group.get("trailer_key", ""),
    }
    for field_name, value in str_updates.items():
        clean = (value or "").strip()
        if clean and clean != (getattr(doc, field_name, "") or ""):
            setattr(doc, field_name, clean)
            changed = True

    list_updates = {
        "genres": group.get("genres", []),
        "actors": group.get("actors", []),
        "cast_profiles": group.get("cast_profiles", []),
    }
    for field_name, value in list_updates.items():
        normalized = list(value or [])
        if normalized and normalized != (getattr(doc, field_name, []) or []):
            setattr(doc, field_name, normalized)
            changed = True

    if group.get("tmdb_id") and not getattr(doc, "tmdb_id", None):
        try:
            doc.tmdb_id = int(group.get("tmdb_id"))
            changed = True
        except Exception:
            pass

    if changed:
        doc.search_title = (doc.title or "").strip().lower()
        doc.updated_at = datetime.now()
        await doc.save()

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
    raw = html.unescape((url or "").strip())
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
            if parsed.path.startswith("/live/"):
                key = parsed.path.split("/live/")[-1].split("?")[0].strip()
                return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
            params = urllib.parse.parse_qs(parsed.query)
            if "v" in params and params["v"]:
                key = (params["v"][0] or "").strip()
                return key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else ""
    except Exception:
        pass
    # Fallback for raw URL/text that still contains a YouTube key
    match = re.search(r"(?:v=|\/embed\/|youtu\.be\/|\/shorts\/|\/live\/)([A-Za-z0-9_-]{11})", raw, re.I)
    if match:
        return match.group(1)
    return ""


def _normalize_person_name(name: str) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", (name or "").strip().lower())
    return re.sub(r"\s+", " ", value).strip()


def _same_person_name(left: str, right: str) -> bool:
    lhs = _normalize_person_name(left)
    rhs = _normalize_person_name(right)
    return bool(lhs and rhs and lhs == rhs)


def _google_profile_url(name: str) -> str:
    query = (name or "").strip()
    if not query:
        return "https://www.google.com"
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(f"{query} actor profile")


async def _tmdb_person_profile(name: str, tmdb_person_id: int | None = None) -> dict:
    if not settings.TMDB_API_KEY:
        return {}
    person_id = tmdb_person_id
    picked: dict = {}
    if not person_id:
        search = await _tmdb_get("/search/person", {"query": name, "include_adult": "false"})
        results = (search or {}).get("results") or []
        if not results:
            return {}
        wanted = _normalize_person_name(name)
        for row in results:
            if _normalize_person_name(row.get("name") or "") == wanted:
                picked = row
                break
        if not picked:
            picked = results[0]
        person_id = picked.get("id")
    if not person_id:
        return {}

    details = await _tmdb_get(
        f"/person/{person_id}",
        {"append_to_response": "combined_credits,external_ids"},
    )
    if not details:
        details = picked
    if not details:
        return {}

    profile_path = details.get("profile_path") or picked.get("profile_path") or ""
    profile_image = f"https://image.tmdb.org/t/p/w500{profile_path}" if profile_path else ""

    known_for_rows = (details.get("combined_credits", {}) or {}).get("cast") or []
    if not known_for_rows:
        known_for_rows = picked.get("known_for") or []
    def _popularity(value) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0

    known_for_rows = sorted(
        known_for_rows,
        key=lambda row: (
            (row.get("release_date") or row.get("first_air_date") or ""),
            _popularity(row.get("popularity")),
        ),
        reverse=True,
    )
    known_for = []
    seen = set()
    for row in known_for_rows:
        title = (row.get("title") or row.get("name") or "").strip()
        if not title:
            continue
        year = (row.get("release_date") or row.get("first_air_date") or "")[:4]
        media_type = (row.get("media_type") or "").lower()
        content_type = "series" if media_type == "tv" else "movie"
        key = (title.lower(), year, content_type)
        if key in seen:
            continue
        seen.add(key)
        known_for.append({
            "title": title,
            "year": year,
            "type": content_type,
        })
        if len(known_for) >= 12:
            break

    external_ids = details.get("external_ids") or {}
    return {
        "tmdb_id": int(person_id),
        "name": details.get("name") or picked.get("name") or name,
        "known_for_department": details.get("known_for_department") or "",
        "birthday": details.get("birthday") or "",
        "deathday": details.get("deathday") or "",
        "place_of_birth": details.get("place_of_birth") or "",
        "biography": details.get("biography") or "",
        "profile_image": profile_image,
        "homepage": details.get("homepage") or "",
        "imdb_id": details.get("imdb_id") or external_ids.get("imdb_id") or "",
        "popularity": details.get("popularity"),
        "known_for": known_for,
    }


async def _google_person_fallback(name: str) -> dict:
    query = (name or "").strip()
    if not query:
        return {}
    url = (
        "https://www.google.com/search?hl=en&q="
        + urllib.parse.quote_plus(f"{query} actor biography")
    )

    def _fetch():
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        title_match = re.search(r"<title>(.*?)</title>", body, re.I | re.S)
        desc_match = re.search(
            r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
            body,
            re.I,
        )
        title = html.unescape(title_match.group(1).strip()) if title_match else ""
        description = html.unescape(desc_match.group(1).strip()) if desc_match else ""
        return {"title": title, "description": description}

    try:
        return await asyncio.to_thread(_fetch)
    except Exception:
        return {}


_INDUSTRY_HINTS = {
    "bollywood": {
        "bollywood", "hindi", "indian", "india", "tollywood", "kollywood", "mollywood",
        "telugu", "tamil", "malayalam", "kannada", "marathi", "punjabi", "bengali",
    },
    "hollywood": {
        "hollywood", "english", "american", "usa", "u.s.", "uk", "british",
    },
}
_TITLE_STOPWORDS = {
    "the", "a", "an", "of", "and", "for", "with", "to", "in", "on", "from", "at",
}


def _norm_label(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _clean_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    cleaned = []
    for value in values:
        text = (value or "").strip()
        if text:
            cleaned.append(text)
    return cleaned


def _group_cast_names(group: dict) -> list[str]:
    names: list[str] = []
    for name in _clean_values(group.get("actors") or []):
        names.append(name)
    for row in group.get("cast_profiles") or []:
        name = (row.get("name") or "").strip()
        if name:
            names.append(name)
    return names


def _group_text_blob(group: dict) -> str:
    parts = [
        group.get("title") or "",
        group.get("description") or "",
        group.get("director") or "",
        " ".join(_clean_values(group.get("genres") or [])),
        " ".join(_group_cast_names(group)),
    ]
    for item in group.get("items") or []:
        parts.append(item.get("name") or "")
    text = " ".join(parts).lower()
    return re.sub(r"\s+", " ", text).strip()


def _infer_industry(group: dict) -> str:
    text = _group_text_blob(group)
    if not text:
        return "other"
    scores = {name: 0 for name in _INDUSTRY_HINTS}
    for industry, hints in _INDUSTRY_HINTS.items():
        for hint in hints:
            if hint in text:
                scores[industry] += 1
    if scores["bollywood"] > scores["hollywood"] and scores["bollywood"] > 0:
        return "bollywood"
    if scores["hollywood"] > scores["bollywood"] and scores["hollywood"] > 0:
        return "hollywood"
    return "other"


def _industry_label(industry: str) -> str:
    mapping = {
        "bollywood": "Bollywood",
        "hollywood": "Hollywood",
        "other": "Other",
    }
    return mapping.get((industry or "other").strip().lower(), "Other")


def _year_int(value: str) -> int | None:
    try:
        year = int((value or "").strip())
        if 1800 <= year <= 2100:
            return year
    except Exception:
        pass
    return None


def _title_tokens(value: str) -> set[str]:
    tokens = _tokenize_search(value)
    return {t for t in tokens if t not in _TITLE_STOPWORDS and len(t) > 2}


def _related_content_cards(target: dict, catalog: list[dict], limit: int = 12) -> list[dict]:
    target_id = (target.get("id") or "").strip()
    target_slug = (target.get("slug") or "").strip()
    target_type = (target.get("type") or "").strip().lower()
    target_industry = _infer_industry(target)
    target_industry_label = _industry_label(target_industry)
    target["industry_label"] = target_industry_label

    target_genres = _clean_values(target.get("genres") or [])
    target_genres_map = {_norm_label(x): x for x in target_genres}
    target_cast = _group_cast_names(target)
    target_cast_map = {_norm_label(x): x for x in target_cast}
    target_director_norm = _norm_label(target.get("director") or "")
    target_year = _year_int(target.get("year") or "")
    target_title_tokens = _title_tokens(target.get("title") or "")

    scored: list[dict] = []
    for row in catalog:
        row_id = (row.get("id") or "").strip()
        row_slug = (row.get("slug") or "").strip()
        if row_id and row_id == target_id:
            continue
        if target_slug and row_slug and row_slug == target_slug:
            continue

        reasons: list[str] = []
        score = 0.0

        row_genres = _clean_values(row.get("genres") or [])
        row_genres_map = {_norm_label(x): x for x in row_genres}
        shared_genres = [target_genres_map[key] for key in target_genres_map.keys() & row_genres_map.keys()]
        if shared_genres:
            score += min(12, len(shared_genres) * 3.0)
            for genre_name in shared_genres[:2]:
                reasons.append(f"Genre: {genre_name}")

        row_cast = _group_cast_names(row)
        row_cast_map = {_norm_label(x): x for x in row_cast}
        shared_cast = [target_cast_map[key] for key in target_cast_map.keys() & row_cast_map.keys()]
        if shared_cast:
            score += min(9, len(shared_cast) * 2.5)
            for cast_name in shared_cast[:2]:
                reasons.append(f"Cast: {cast_name}")

        row_director_norm = _norm_label(row.get("director") or "")
        if target_director_norm and row_director_norm and row_director_norm == target_director_norm:
            score += 3.0
            reasons.append("Same director")

        row_industry = _infer_industry(row)
        if target_industry == row_industry and target_industry != "other":
            score += 2.0
            reasons.append(target_industry_label)
        elif target_industry == row_industry and target_industry == "other":
            score += 0.5

        row_type = (row.get("type") or "").strip().lower()
        if target_type and row_type == target_type:
            score += 1.0

        row_year = _year_int(row.get("year") or "")
        if target_year and row_year:
            year_gap = abs(target_year - row_year)
            if year_gap == 0:
                score += 1.2
            elif year_gap <= 2:
                score += 0.7
            elif year_gap <= 5:
                score += 0.3

        row_title_tokens = _title_tokens(row.get("title") or "")
        shared_title_tokens = target_title_tokens & row_title_tokens
        if shared_title_tokens:
            score += min(1.0, len(shared_title_tokens) * 0.35)

        if score < 1.8:
            continue

        scored.append({
            "id": row_id,
            "slug": row_slug,
            "title": row.get("title") or "",
            "year": row.get("year") or "",
            "type": row.get("type") or "",
            "poster": row.get("poster") or "",
            "quality": row.get("quality") or row.get("primary_quality") or "HD",
            "industry_label": _industry_label(row_industry),
            "score": score,
            "match_tags": reasons[:3],
        })

    scored.sort(key=lambda item: (-item.get("score", 0.0), (item.get("title") or "").lower()))
    return scored[:limit]


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
    groups = await build_content_groups(user, is_admin, limit=max(limit, 200), ensure_sync=True)
    cards = []
    for group in groups:
        cards.extend(group.get("items") or [])
        if len(cards) >= limit:
            break
    cards = cards[:limit]
    return cards


async def _build_catalog(user: User | None, is_admin: bool, limit: int = 1200) -> list[dict]:
    return await build_content_groups(user, is_admin, limit=limit, ensure_sync=True)

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


def _normalize_filter_type(value: str) -> str:
    raw = (value or "").strip().lower()
    if raw in {"movies", "movie", "films"}:
        return "movies"
    if raw in {"series", "web-series", "webseries", "tv", "shows"}:
        return "series"
    return "all"


def _release_sort_score(card: dict) -> int:
    release_raw = (card.get("release_date") or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
        try:
            dt = datetime.strptime(release_raw, fmt)
            return int(dt.strftime("%Y%m%d"))
        except Exception:
            pass
    year = (card.get("year") or "").strip()
    if re.fullmatch(r"\d{4}", year):
        return int(f"{year}0101")
    return 0


def _card_matches_query(card: dict, query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return True
    haystack = " ".join([
        card.get("title") or "",
        card.get("year") or "",
        card.get("type") or "",
        card.get("description") or "",
        " ".join(card.get("genres") or []),
        " ".join(card.get("actors") or []),
    ]).lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", q) if token]
    return all(token in haystack for token in tokens)


def _decorate_catalog_cards(cards: list[dict]) -> list[dict]:
    for card in cards:
        content_type = (card.get("type") or "").strip().lower()
        if content_type == "movie":
            quality_set: set[str] = set()
            for quality in (card.get("qualities") or {}).keys():
                q = (quality or "").strip().upper()
                if q:
                    quality_set.add(q)
            for row in card.get("items") or []:
                q = (row.get("quality") or "").strip().upper()
                if q:
                    quality_set.add(q)
            qualities = sorted(quality_set, key=_quality_rank, reverse=True)
            card["quality_row"] = qualities[:4]
            card["season_text"] = ""
        else:
            season_numbers = []
            for season_no in (card.get("seasons") or {}).keys():
                try:
                    season_numbers.append(int(season_no))
                except Exception:
                    continue
            season_numbers = sorted(set(season_numbers))
            card["quality_row"] = []
            if not season_numbers:
                card["season_text"] = "Season: 1"
            elif len(season_numbers) > 10:
                card["season_text"] = "Season: 1 to 10"
            else:
                card["season_text"] = "Season: " + " - ".join(str(num) for num in season_numbers)
    return cards


def _paginate_cards(cards: list[dict], page: int, per_page: int = CATALOG_ITEMS_PER_PAGE) -> tuple[list[dict], int, int, int]:
    total_items = len(cards)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    current_page = max(1, min(int(page or 1), total_pages))
    start = (current_page - 1) * per_page
    end = start + per_page
    return cards[start:end], total_items, total_pages, current_page


async def _render_catalog_page(
    request: Request,
    filter_type: str,
    q: str = "",
    page: int = 1,
    title_override: str = "",
    cards_override: list[dict] | None = None,
    active_tab: str = "all",
    page_base_url: str = "",
):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    settings_row = await _site_settings()
    normalized_filter = _normalize_filter_type(filter_type)
    search_query = (q or "").strip()
    search_mode = bool(search_query)

    cards = cards_override[:] if cards_override is not None else await _build_catalog(user, is_admin, limit=4000)
    if normalized_filter == "movies":
        cards = [c for c in cards if (c.get("type") or "").lower() == "movie"]
    elif normalized_filter == "series":
        cards = [c for c in cards if (c.get("type") or "").lower() == "series"]
    if search_mode:
        cards = [c for c in cards if _card_matches_query(c, search_query)]

    cards = sorted(
        cards,
        key=lambda row: (_release_sort_score(row), (row.get("title") or "").lower()),
        reverse=True,
    )
    cards = _decorate_catalog_cards(cards)
    asyncio.create_task(_warm_group_assets(cards[:24], limit=8))

    paged_cards, total_items, total_pages, current_page = _paginate_cards(cards, page)
    page_start = max(1, current_page - 2)
    page_end = min(total_pages, current_page + 2)

    if not page_base_url:
        if search_mode:
            page_base_url = f"/content/f/{normalized_filter}/search_content/{urllib.parse.quote(search_query)}"
        else:
            page_base_url = f"/content/f/{normalized_filter}"

    if title_override:
        title_value = title_override
    elif search_mode:
        title_value = f'Search Results: "{search_query}"'
    elif normalized_filter == "movies":
        title_value = "Latest Movies"
    elif normalized_filter == "series":
        title_value = "Latest Web Series"
    else:
        title_value = "Latest Uploads"

    return templates.TemplateResponse("content_list.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": settings_row,
        "title": title_value,
        "cards": paged_cards,
        "all_cards_count": len(cards),
        "active_tab": active_tab,
        "filter_type": normalized_filter,
        "search_mode": search_mode,
        "search_query": search_query,
        "total_items": total_items,
        "total_pages": total_pages,
        "current_page": current_page,
        "page_start": page_start,
        "page_end": page_end,
        "page_base_url": page_base_url,
        "query": search_query,
    })


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
async def content_all(
    request: Request,
    q: str = "",
    page: int = 1,
    filter: str = "",
    search_content: str = "",
):
    # Backward compatibility with old query style URLs:
    # /content?filter=movies&search_content=abc&page=2 -> /content/f/movies/search_content/abc?page=2
    legacy_filter = (filter or "").strip()
    legacy_search = (search_content or "").strip()
    if legacy_filter or legacy_search:
        normalized_filter = _normalize_filter_type(legacy_filter or "all")
        final_query = legacy_search or (q or "").strip()
        if final_query:
            clean_url = f"/content/f/{normalized_filter}/search_content/{urllib.parse.quote(final_query)}"
        else:
            clean_url = f"/content/f/{normalized_filter}"
        if page > 1:
            clean_url = f"{clean_url}?page={page}"
        return RedirectResponse(clean_url, status_code=307)
    if (q or "").strip():
        clean_url = f"/content/f/all/search_content/{urllib.parse.quote((q or '').strip())}"
        if page > 1:
            clean_url = f"{clean_url}?page={page}"
        return RedirectResponse(clean_url, status_code=307)
    return await _render_catalog_page(
        request=request,
        filter_type="all",
        q=q,
        page=page,
        active_tab="all",
    )


@router.get("/content/movies")
async def content_movies(request: Request, q: str = "", page: int = 1):
    if (q or "").strip():
        clean_url = f"/content/f/movies/search_content/{urllib.parse.quote((q or '').strip())}"
        if page > 1:
            clean_url = f"{clean_url}?page={page}"
        return RedirectResponse(clean_url, status_code=307)
    return await _render_catalog_page(
        request=request,
        filter_type="movies",
        q=q,
        page=page,
        active_tab="movies",
    )


@router.get("/content/web-series")
async def content_series(request: Request, q: str = "", page: int = 1):
    if (q or "").strip():
        clean_url = f"/content/f/series/search_content/{urllib.parse.quote((q or '').strip())}"
        if page > 1:
            clean_url = f"{clean_url}?page={page}"
        return RedirectResponse(clean_url, status_code=307)
    return await _render_catalog_page(
        request=request,
        filter_type="series",
        q=q,
        page=page,
        active_tab="series",
    )


@router.get("/content/f/{filter_type}")
async def content_by_filter(request: Request, filter_type: str, page: int = 1):
    normalized_filter = _normalize_filter_type(filter_type)
    return await _render_catalog_page(
        request=request,
        filter_type=normalized_filter,
        q="",
        page=page,
        active_tab=normalized_filter,
        page_base_url=f"/content/f/{normalized_filter}",
    )


@router.get("/content/f/{filter_type}/search_content/{search_query:path}")
async def content_by_filter_search(
    request: Request,
    filter_type: str,
    search_query: str,
    page: int = 1,
):
    normalized_filter = _normalize_filter_type(filter_type)
    decoded_query = urllib.parse.unquote(search_query or "")
    return await _render_catalog_page(
        request=request,
        filter_type=normalized_filter,
        q=decoded_query,
        page=page,
        active_tab=normalized_filter,
        page_base_url=f"/content/f/{normalized_filter}/search_content/{urllib.parse.quote(decoded_query)}",
    )


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
        for item in group.get("items", []) or []:
            item_id = (item.get("id") or "").strip()
            if item_id:
                keys.append(item_id)
        keys = [k for k in keys if k]
        found = await WatchlistEntry.find_one(
            WatchlistEntry.user_phone == user.phone_number,
            In(WatchlistEntry.item_id, keys)
        )
        watchlisted = found is not None
    related_cards = _related_content_cards(group, catalog, limit=12)

    return templates.TemplateResponse("content_details.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "item": group,
        "qualities": qualities,
        "seasons": seasons,
        "watchlisted": watchlisted,
        "related_cards": related_cards,
        "link_token": link_token,
        "viewer_name": viewer_name,
    })


@router.get("/content/cast")
async def content_cast_profile(
    request: Request,
    name: str = "",
    tmdb_id: Optional[int] = None,
    back: str = "",
):
    user = await get_current_user(request)
    is_admin = _is_admin(user)
    site = await _site_settings()

    cast_name = (name or "").strip()
    if not cast_name:
        raise HTTPException(status_code=400, detail="Cast name is required")

    catalog = await _build_catalog(user, is_admin, limit=3500)
    matched_groups = []
    for group in catalog:
        names = _group_cast_names(group)
        if any(_same_person_name(candidate, cast_name) for candidate in names):
            matched_groups.append(group)

    if not matched_groups:
        fuzzy_regex = _build_search_regex(cast_name)
        if fuzzy_regex:
            fuzzy_pattern = re.compile(fuzzy_regex, re.I)
            for group in catalog:
                names_blob = " ".join(_group_cast_names(group))
                if names_blob and fuzzy_pattern.search(names_blob):
                    matched_groups.append(group)

    cards_by_key: dict[tuple[str, str, str], dict] = {}
    local_image = ""
    local_roles: set[str] = set()
    local_tmdb_id: int | None = None

    for group in matched_groups:
        for cast_row in group.get("cast_profiles", []) or []:
            cast_row_name = (cast_row.get("name") or "").strip()
            if not cast_row_name or not _same_person_name(cast_row_name, cast_name):
                continue
            if not local_image and cast_row.get("image"):
                local_image = cast_row.get("image")
            role = (cast_row.get("role") or "").strip()
            if role:
                local_roles.add(role)
            if local_tmdb_id is None:
                raw_tmdb = cast_row.get("id") or cast_row.get("tmdb_id") or cast_row.get("person_id")
                try:
                    if raw_tmdb is not None and str(raw_tmdb).strip():
                        local_tmdb_id = int(str(raw_tmdb).strip())
                except Exception:
                    pass

        key = (
            (group.get("title") or "").lower(),
            group.get("year", ""),
            group.get("type", ""),
        )
        if key in cards_by_key:
            continue
        cards_by_key[key] = {
            "id": group.get("id", ""),
            "title": group.get("title", ""),
            "year": group.get("year", ""),
            "type": group.get("type", ""),
            "poster": group.get("poster", ""),
            "slug": group.get("slug", "") or _group_slug(group.get("title", ""), group.get("year", "")),
            "quality": group.get("quality", "") or group.get("primary_quality", "HD"),
        }
        if len(cards_by_key) >= 84:
            break

    resolved_tmdb_id = tmdb_id if tmdb_id and tmdb_id > 0 else local_tmdb_id
    tmdb_profile = await _tmdb_person_profile(cast_name, resolved_tmdb_id)
    google_profile = {}
    if not tmdb_profile or not tmdb_profile.get("biography"):
        google_profile = await _google_person_fallback(cast_name)

    profile = {
        "tmdb_id": resolved_tmdb_id,
        "name": cast_name,
        "known_for_department": "",
        "birthday": "",
        "deathday": "",
        "place_of_birth": "",
        "biography": "",
        "profile_image": local_image,
        "homepage": "",
        "imdb_id": "",
        "popularity": None,
        "known_for": [],
        "roles": sorted(local_roles),
        "source": "local",
        "google_url": _google_profile_url(cast_name),
    }
    if tmdb_profile:
        profile.update(tmdb_profile)
        profile["source"] = "tmdb"
    if not profile.get("profile_image") and local_image:
        profile["profile_image"] = local_image
    if google_profile.get("description") and not profile.get("biography"):
        profile["biography"] = google_profile.get("description")
    if google_profile and profile.get("source") != "tmdb":
        profile["source"] = "google"
    if not profile.get("roles"):
        profile["roles"] = sorted(local_roles)
    if profile.get("tmdb_id"):
        profile["tmdb_url"] = f"https://www.themoviedb.org/person/{profile['tmdb_id']}"
    if profile.get("imdb_id"):
        profile["imdb_url"] = f"https://www.imdb.com/name/{profile['imdb_id']}/"

    cards = sorted(cards_by_key.values(), key=lambda item: (item.get("title") or "").lower())
    back_path = (back or "").strip()
    if back_path and not back_path.startswith("/"):
        back_path = ""

    return templates.TemplateResponse("cast_profile.html", {
        "request": request,
        "user": user,
        "is_admin": is_admin,
        "site": site,
        "cast": profile,
        "cards": cards,
        "content_count": len(cards),
        "back_path": back_path,
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

    pattern = re.compile(search_regex, re.I)
    catalog = await _build_catalog(user, is_admin, limit=1600)
    grouped: dict[tuple[str, str, str], dict] = {}
    for group in catalog:
        searchable = [
            group.get("title", ""),
            group.get("year", ""),
            group.get("type", ""),
            " ".join((itm.get("name") or "") for itm in (group.get("items") or [])[:12]),
        ]
        if not any(pattern.search(text or "") for text in searchable):
            continue
        key = (
            (group.get("title") or "").lower(),
            group.get("year", ""),
            group.get("type", ""),
        )
        if key in grouped:
            continue
        grouped[key] = {
            "title": group.get("title", ""),
            "year": group.get("year", ""),
            "type": group.get("type", ""),
            "poster": group.get("poster", ""),
            "slug": group.get("slug", "") or _group_slug(group.get("title", ""), group.get("year", "")),
        }
        if len(grouped) >= 12:
            break
    items = sorted(grouped.values(), key=lambda x: (x.get("title") or "").lower())
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
        db_item = await ContentItem.get(raw_key)
        if db_item:
            slug_val = (getattr(db_item, "slug", "") or "").strip()
            if not slug_val:
                slug_val = _group_slug(getattr(db_item, "title", ""), getattr(db_item, "year", ""))
            if slug_val:
                primary_key = f"slug:{slug_val}"
    else:
        slug = raw_key.lower()
        primary_key = f"slug:{slug}"

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
async def content_watchlist(request: Request, page: int = 1):
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
    filtered_cards = []
    for card in cards:
        if card.get("id") in object_ids:
            filtered_cards.append(card)
            continue
        if (card.get("slug") or "") in slug_ids:
            filtered_cards.append(card)
            continue
        if any((itm.get("id") or "") in object_ids for itm in (card.get("items") or [])):
            filtered_cards.append(card)
    return await _render_catalog_page(
        request=request,
        filter_type="all",
        q="",
        page=page,
        title_override="My Watchlist",
        cards_override=filtered_cards,
        active_tab="watchlist",
        page_base_url="/content/watchlist",
    )


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
    by_file_id: dict[str, dict] = {}
    for card in catalog:
        for item in card.get("items", []) or []:
            file_id = str(item.get("id") or "").strip()
            if file_id and file_id not in by_file_id:
                by_file_id[file_id] = card
    for row in my_requests:
        raw_path = str(getattr(row, "fulfilled_content_path", "") or "").strip()
        if not raw_path:
            ref_id = str(getattr(row, "fulfilled_content_id", "") or "").strip()
            match = by_id.get(ref_id) or by_file_id.get(ref_id)
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
