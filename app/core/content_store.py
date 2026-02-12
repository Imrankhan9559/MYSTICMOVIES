import asyncio
import re
from datetime import datetime
from typing import Optional

from beanie import PydanticObjectId
from beanie.operators import In

from app.core.config import settings
from app.db.models import ContentFileRef, ContentItem, FileSystemItem, User

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
SEASON_TAG_RE = re.compile(
    r"\bS\d{1,2}E\d{1,3}\b|\bS\d{1,2}\b|\bE\d{1,3}\b|\bSeason\s?\d{1,2}\b|\bEpisode\s?\d{1,3}\b",
    re.I,
)

_sync_lock = asyncio.Lock()
_last_sync_ts = 0.0
_sync_interval_sec = 30.0


def _normalize_phone(phone: str) -> str:
    return re.sub(r"\D+", "", (phone or ""))


def _slugify(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower())
    return value.strip("-")


def group_slug(title: str, year: str = "") -> str:
    base = _slugify(title)
    y = (year or "").strip()
    return f"{base}-{y}" if base and y else base


def _infer_quality(name: str) -> str:
    match = QUALITY_RE.search(name or "")
    return (match.group(1) if match else "HD").upper()


def _quality_rank(q: str) -> int:
    order = {"2160P": 5, "1440P": 4, "1080P": 3, "720P": 2, "480P": 1, "380P": 0, "360P": 0, "HD": 0}
    return order.get((q or "").upper(), 0)


def _season_episode(name: str) -> tuple[Optional[int], Optional[int]]:
    match = SE_RE.search(name or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _clean_title(name: str) -> str:
    base = re.sub(r"\.[^.]+$", "", name or "")
    base = re.sub(r"[._]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def parse_file_name(name: str) -> dict:
    raw = name or ""
    cleaned = _clean_title(raw)
    year = ""
    year_match = YEAR_RE.search(cleaned)
    if year_match:
        year = year_match.group(1)
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
        "year": year,
        "quality": quality,
        "is_series": is_series,
        "season": season,
        "episode": episode,
    }


def is_video_file(item: FileSystemItem) -> bool:
    if item.is_folder:
        return False
    mime = (item.mime_type or "").lower()
    if mime.startswith("video"):
        return True
    return (item.name or "").lower().endswith(VIDEO_EXTS)


def _content_type_from_item(item: FileSystemItem) -> str:
    catalog_type = (getattr(item, "catalog_type", "") or "").strip().lower()
    if catalog_type in ("movie", "series"):
        return catalog_type
    parsed = parse_file_name(item.name or "")
    return "series" if parsed.get("is_series") else "movie"


def _content_title_from_item(item: FileSystemItem) -> str:
    parsed = parse_file_name(item.name or "")
    return (
        (getattr(item, "series_title", "") or "").strip()
        or (getattr(item, "title", "") or "").strip()
        or (parsed.get("title") or "").strip()
        or (item.name or "").strip()
    )


def _content_year_from_item(item: FileSystemItem) -> str:
    parsed = parse_file_name(item.name or "")
    return (
        (getattr(item, "year", "") or "").strip()
        or (parsed.get("year") or "").strip()
    )


def _content_doc_query(user: User | None, is_admin: bool) -> dict:
    base: dict = {"status": "published"}
    if is_admin:
        return base
    admin_phone = _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))
    if user:
        phone = _normalize_phone(getattr(user, "phone_number", ""))
        or_filters = [
            {"owner_phone": phone},
            {"collaborators": phone},
            {"owner_phone": ""},
        ]
        if admin_phone:
            or_filters.insert(0, {"owner_phone": admin_phone})
        base["$or"] = or_filters
        return base
    if admin_phone:
        base["$or"] = [{"owner_phone": admin_phone}, {"owner_phone": ""}]
    return base


def _cast_object_ids(raw_ids: list[str]) -> list[PydanticObjectId]:
    out: list[PydanticObjectId] = []
    for raw in raw_ids:
        try:
            out.append(PydanticObjectId(raw))
        except Exception:
            continue
    return out


def _doc_file_ids(doc: ContentItem) -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for raw in getattr(doc, "file_ids", []) or []:
        val = str(raw).strip()
        if val and val not in seen:
            seen.add(val)
            ids.append(val)
    for row in getattr(doc, "files", []) or []:
        val = str(getattr(row, "file_id", "") or "").strip()
        if val and val not in seen:
            seen.add(val)
            ids.append(val)
    return ids


def _pick_str(primary: str, fallback: str) -> str:
    if (primary or "").strip():
        return (primary or "").strip()
    return (fallback or "").strip()


def _pick_list(primary: list, fallback: list) -> list:
    if primary:
        return list(primary)
    return list(fallback or [])


async def sync_content_catalog(force: bool = False, limit: int = 10000) -> int:
    global _last_sync_ts
    now = asyncio.get_event_loop().time()
    if not force and (now - _last_sync_ts) <= _sync_interval_sec:
        return await ContentItem.find(ContentItem.status == "published").count()

    async with _sync_lock:
        now = asyncio.get_event_loop().time()
        if not force and (now - _last_sync_ts) <= _sync_interval_sec:
            return await ContentItem.find(ContentItem.status == "published").count()

        existing_docs = await ContentItem.find_all().to_list()
        existing_by_id = {str(doc.id): doc for doc in existing_docs}
        existing_by_slug_type: dict[tuple[str, str], ContentItem] = {}
        file_to_doc: dict[str, ContentItem] = {}
        for doc in existing_docs:
            key = ((getattr(doc, "slug", "") or "").strip(), (getattr(doc, "content_type", "") or "").strip())
            if key[0] and key[1]:
                existing_by_slug_type[key] = doc
            for file_id in _doc_file_ids(doc):
                file_to_doc[file_id] = doc

        rows = await FileSystemItem.find(
            FileSystemItem.is_folder == False,
            FileSystemItem.catalog_status == "published",
        ).sort("-created_at").limit(limit).to_list()

        grouped: dict[str, dict] = {}
        for item in rows:
            if not is_video_file(item):
                continue
            file_id = str(item.id)
            linked_doc = file_to_doc.get(file_id)
            parsed = parse_file_name(item.name or "")

            if linked_doc:
                key = f"doc:{linked_doc.id}"
                group = grouped.setdefault(
                    key,
                    {
                        "doc_id": str(linked_doc.id),
                        "slug": (linked_doc.slug or "").strip(),
                        "title": (linked_doc.title or "").strip(),
                        "content_type": (linked_doc.content_type or "movie").strip().lower(),
                        "year": (linked_doc.year or "").strip(),
                        "release_date": (linked_doc.release_date or "").strip(),
                        "poster_url": (linked_doc.poster_url or "").strip(),
                        "backdrop_url": (linked_doc.backdrop_url or "").strip(),
                        "description": (linked_doc.description or "").strip(),
                        "genres": list(linked_doc.genres or []),
                        "actors": list(linked_doc.actors or []),
                        "director": (linked_doc.director or "").strip(),
                        "trailer_url": (linked_doc.trailer_url or "").strip(),
                        "trailer_key": (linked_doc.trailer_key or "").strip(),
                        "cast_profiles": list(linked_doc.cast_profiles or []),
                        "tmdb_id": getattr(linked_doc, "tmdb_id", None),
                        "owner_phone": _normalize_phone(getattr(linked_doc, "owner_phone", "")),
                        "collaborators": set(_normalize_phone(p) for p in (linked_doc.collaborators or [])),
                        "file_ids": [],
                        "file_seen": set(),
                        "files": [],
                    },
                )
            else:
                title = _content_title_from_item(item)
                if not title:
                    continue
                content_type = _content_type_from_item(item)
                year = _content_year_from_item(item)
                slug = group_slug(title, year)
                key = f"auto:{content_type}:{slug}"
                group = grouped.setdefault(
                    key,
                    {
                        "doc_id": "",
                        "slug": slug,
                        "title": title,
                        "content_type": content_type,
                        "year": year,
                        "release_date": (getattr(item, "release_date", "") or "").strip(),
                        "poster_url": (getattr(item, "poster_url", "") or "").strip(),
                        "backdrop_url": (getattr(item, "backdrop_url", "") or "").strip(),
                        "description": (getattr(item, "description", "") or "").strip(),
                        "genres": list(getattr(item, "genres", []) or []),
                        "actors": list(getattr(item, "actors", []) or []),
                        "director": (getattr(item, "director", "") or "").strip(),
                        "trailer_url": (getattr(item, "trailer_url", "") or "").strip(),
                        "trailer_key": (getattr(item, "trailer_key", "") or "").strip(),
                        "cast_profiles": list(getattr(item, "cast_profiles", []) or []),
                        "tmdb_id": getattr(item, "tmdb_id", None),
                        "owner_phone": _normalize_phone(getattr(item, "owner_phone", "")),
                        "collaborators": set(),
                        "file_ids": [],
                        "file_seen": set(),
                        "files": [],
                    },
                )

            if not group.get("title"):
                group["title"] = _content_title_from_item(item)
            if not group.get("year"):
                group["year"] = _content_year_from_item(item)
            if not group.get("slug"):
                group["slug"] = group_slug(group.get("title", ""), group.get("year", ""))
            if not group.get("content_type"):
                group["content_type"] = _content_type_from_item(item)
            if not group.get("release_date") and getattr(item, "release_date", ""):
                group["release_date"] = (getattr(item, "release_date", "") or "").strip()
            if not group.get("poster_url") and getattr(item, "poster_url", ""):
                group["poster_url"] = (getattr(item, "poster_url", "") or "").strip()
            if not group.get("backdrop_url") and getattr(item, "backdrop_url", ""):
                group["backdrop_url"] = (getattr(item, "backdrop_url", "") or "").strip()
            if not group.get("description") and getattr(item, "description", ""):
                group["description"] = (getattr(item, "description", "") or "").strip()
            if not group.get("genres") and getattr(item, "genres", []):
                group["genres"] = list(getattr(item, "genres", []) or [])
            if not group.get("actors") and getattr(item, "actors", []):
                group["actors"] = list(getattr(item, "actors", []) or [])
            if not group.get("director") and getattr(item, "director", ""):
                group["director"] = (getattr(item, "director", "") or "").strip()
            if not group.get("trailer_url") and getattr(item, "trailer_url", ""):
                group["trailer_url"] = (getattr(item, "trailer_url", "") or "").strip()
            if not group.get("trailer_key") and getattr(item, "trailer_key", ""):
                group["trailer_key"] = (getattr(item, "trailer_key", "") or "").strip()
            if not group.get("cast_profiles") and getattr(item, "cast_profiles", []):
                group["cast_profiles"] = list(getattr(item, "cast_profiles", []) or [])
            if not group.get("tmdb_id") and getattr(item, "tmdb_id", None):
                group["tmdb_id"] = getattr(item, "tmdb_id", None)

            owner = _normalize_phone(getattr(item, "owner_phone", ""))
            if owner and not group.get("owner_phone"):
                group["owner_phone"] = owner
            for collaborator in getattr(item, "collaborators", []) or []:
                normalized = _normalize_phone(collaborator)
                if normalized:
                    group["collaborators"].add(normalized)

            if file_id in group["file_seen"]:
                continue
            group["file_seen"].add(file_id)
            group["file_ids"].append(file_id)

            quality = (getattr(item, "quality", "") or parsed.get("quality") or "HD").upper()
            season = getattr(item, "season", None)
            episode = getattr(item, "episode", None)
            if season is None:
                season = parsed.get("season")
            if episode is None:
                episode = parsed.get("episode")

            if group["content_type"] != "series":
                season = None
                episode = None
            else:
                season = int(season) if season else 1
                episode = int(episode) if episode else 1

            group["files"].append(
                {
                    "file_id": file_id,
                    "name": item.name or "",
                    "quality": quality,
                    "season": season,
                    "episode": episode,
                    "episode_title": (getattr(item, "episode_title", "") or "").strip() or None,
                    "size": int(getattr(item, "size", 0) or 0),
                    "mime_type": (getattr(item, "mime_type", "") or "").strip() or None,
                }
            )

        now_dt = datetime.now()
        seen_doc_ids: set[str] = set()

        for group in grouped.values():
            group["title"] = (group.get("title") or "").strip()
            group["year"] = (group.get("year") or "").strip()
            group["content_type"] = (group.get("content_type") or "movie").strip().lower()
            group["slug"] = (group.get("slug") or group_slug(group["title"], group["year"])).strip()
            if not group["title"] or not group["slug"]:
                continue

            group["files"].sort(
                key=lambda row: (
                    int(row.get("season") or 0),
                    int(row.get("episode") or 0),
                    -_quality_rank(row.get("quality", "")),
                    (row.get("name") or "").lower(),
                )
            )

            doc = None
            if group.get("doc_id"):
                doc = existing_by_id.get(group["doc_id"])
            if not doc:
                doc = existing_by_slug_type.get((group["slug"], group["content_type"]))

            if doc:
                doc.title = _pick_str(group["title"], getattr(doc, "title", "")) or group["title"]
                doc.year = _pick_str(group["year"], getattr(doc, "year", "")) or group["year"]
                doc.slug = _pick_str(group["slug"], getattr(doc, "slug", "")) or group["slug"]
                doc.search_title = ((doc.title or group["title"]).strip().lower())
                doc.content_type = group["content_type"]
                doc.release_date = _pick_str(group.get("release_date", ""), getattr(doc, "release_date", ""))
                doc.poster_url = _pick_str(group.get("poster_url", ""), getattr(doc, "poster_url", ""))
                doc.backdrop_url = _pick_str(group.get("backdrop_url", ""), getattr(doc, "backdrop_url", ""))
                doc.description = _pick_str(group.get("description", ""), getattr(doc, "description", ""))
                doc.genres = _pick_list(group.get("genres", []), getattr(doc, "genres", []))
                doc.actors = _pick_list(group.get("actors", []), getattr(doc, "actors", []))
                doc.director = _pick_str(group.get("director", ""), getattr(doc, "director", ""))
                doc.trailer_url = _pick_str(group.get("trailer_url", ""), getattr(doc, "trailer_url", ""))
                doc.trailer_key = _pick_str(group.get("trailer_key", ""), getattr(doc, "trailer_key", ""))
                doc.cast_profiles = _pick_list(group.get("cast_profiles", []), getattr(doc, "cast_profiles", []))
                if not getattr(doc, "tmdb_id", None) and group.get("tmdb_id"):
                    doc.tmdb_id = group.get("tmdb_id")
                doc.owner_phone = _pick_str(group.get("owner_phone", ""), getattr(doc, "owner_phone", ""))
                merged_collabs = set(_normalize_phone(x) for x in (getattr(doc, "collaborators", []) or []))
                merged_collabs.update(group.get("collaborators", set()))
                doc.collaborators = sorted(x for x in merged_collabs if x)
                doc.file_ids = list(group["file_ids"])
                doc.files = [ContentFileRef(**row) for row in group["files"]]
                doc.status = "published"
                doc.updated_at = now_dt
                await doc.save()
            else:
                title = group["title"]
                year = group["year"]
                new_doc = ContentItem(
                    slug=group["slug"] or group_slug(title, year),
                    title=title,
                    search_title=title.lower(),
                    content_type=group["content_type"],
                    status="published",
                    year=year,
                    release_date=group.get("release_date", ""),
                    poster_url=group.get("poster_url", ""),
                    backdrop_url=group.get("backdrop_url", ""),
                    description=group.get("description", ""),
                    genres=group.get("genres", []) or [],
                    actors=group.get("actors", []) or [],
                    director=group.get("director", ""),
                    trailer_url=group.get("trailer_url", ""),
                    trailer_key=group.get("trailer_key", ""),
                    cast_profiles=group.get("cast_profiles", []) or [],
                    tmdb_id=group.get("tmdb_id"),
                    owner_phone=group.get("owner_phone", ""),
                    collaborators=sorted(x for x in group.get("collaborators", set()) if x),
                    file_ids=list(group["file_ids"]),
                    files=[ContentFileRef(**row) for row in group["files"]],
                    created_at=now_dt,
                    updated_at=now_dt,
                )
                await new_doc.insert()
                doc = new_doc

            seen_doc_ids.add(str(doc.id))

        # Avoid accidental archiving when there may be more rows than the sync limit.
        if len(rows) < limit:
            for doc in existing_docs:
                doc_id = str(doc.id)
                if doc_id in seen_doc_ids:
                    continue
                if (getattr(doc, "status", "") or "").strip().lower() != "published":
                    continue
                doc.status = "archived"
                doc.file_ids = []
                doc.files = []
                doc.updated_at = now_dt
                await doc.save()

        _last_sync_ts = asyncio.get_event_loop().time()
        return len(seen_doc_ids)


async def fetch_content_docs(
    user: User | None,
    is_admin: bool,
    limit: int = 1200,
    ensure_sync: bool = True,
) -> list[ContentItem]:
    if ensure_sync:
        await sync_content_catalog(force=False, limit=max(limit * 4, 2000))
    query = _content_doc_query(user, is_admin)
    return await ContentItem.find(query).sort("-updated_at").limit(limit).to_list()


async def build_content_groups(
    user: User | None,
    is_admin: bool,
    limit: int = 1200,
    ensure_sync: bool = True,
) -> list[dict]:
    docs = await fetch_content_docs(user, is_admin, limit=limit, ensure_sync=ensure_sync)
    file_ids: list[str] = []
    seen_file_ids: set[str] = set()
    for doc in docs:
        for file_id in _doc_file_ids(doc):
            if file_id and file_id not in seen_file_ids:
                seen_file_ids.add(file_id)
                file_ids.append(file_id)

    file_map: dict[str, FileSystemItem] = {}
    cast_ids = _cast_object_ids(file_ids)
    if cast_ids:
        rows = await FileSystemItem.find(In(FileSystemItem.id, cast_ids)).to_list()
        file_map = {str(row.id): row for row in rows}

    groups: list[dict] = []
    for doc in docs:
        group = {
            "id": str(doc.id),
            "title": (doc.title or "").strip(),
            "year": (doc.year or "").strip(),
            "slug": (doc.slug or group_slug(doc.title or "", doc.year or "")).strip(),
            "release_date": (doc.release_date or "").strip(),
            "type": (doc.content_type or "movie").strip().lower(),
            "poster": (doc.poster_url or "").strip(),
            "backdrop": (doc.backdrop_url or "").strip(),
            "description": (doc.description or "").strip(),
            "genres": list(doc.genres or []),
            "actors": list(doc.actors or []),
            "director": (doc.director or "").strip(),
            "trailer_url": (doc.trailer_url or "").strip(),
            "trailer_key": (doc.trailer_key or "").strip(),
            "cast_profiles": list(doc.cast_profiles or []),
            "tmdb_id": getattr(doc, "tmdb_id", None),
            "qualities": {},
            "seasons": {},
            "episode_titles": {},
            "items": [],
            "updated_at": getattr(doc, "updated_at", None),
        }
        refs = list(getattr(doc, "files", []) or [])
        if not refs:
            for raw in getattr(doc, "file_ids", []) or []:
                refs.append(ContentFileRef(file_id=str(raw)))

        for ref in refs:
            file_id = str(getattr(ref, "file_id", "") or "").strip()
            if not file_id:
                continue

            row = file_map.get(file_id)
            if row and not is_video_file(row):
                continue

            name = (row.name if row else getattr(ref, "name", "")) or ""
            mime_type = (row.mime_type if row else getattr(ref, "mime_type", "")) or ""
            if not row:
                mime_ok = str(mime_type).lower().startswith("video")
                ext_ok = name.lower().endswith(VIDEO_EXTS)
                if not (mime_ok or ext_ok):
                    continue

            parsed = parse_file_name(name)
            quality = (
                (getattr(row, "quality", "") or "").strip()
                or (getattr(ref, "quality", "") or "").strip()
                or (parsed.get("quality") or "HD")
            ).upper()

            season = getattr(row, "season", None) if row else None
            episode = getattr(row, "episode", None) if row else None
            if season is None:
                season = getattr(ref, "season", None)
            if episode is None:
                episode = getattr(ref, "episode", None)
            if season is None:
                season = parsed.get("season")
            if episode is None:
                episode = parsed.get("episode")

            if group["type"] == "series":
                season = int(season) if season else 1
                episode = int(episode) if episode else 1
            else:
                season = None
                episode = None

            size = int((row.size if row else getattr(ref, "size", 0)) or 0)
            episode_title = (
                (getattr(row, "episode_title", "") or "").strip()
                or (getattr(ref, "episode_title", "") or "").strip()
            )
            card = {
                "id": file_id,
                "name": name,
                "title": group["title"],
                "type": group["type"],
                "quality": quality,
                "season": season,
                "episode": episode,
                "episode_title": episode_title,
                "series_key": (group["title"] or "").strip().lower(),
                "poster": group["poster"],
                "backdrop": group["backdrop"],
                "description": group["description"],
                "year": group["year"],
                "release_date": group["release_date"],
                "genres": group["genres"],
                "actors": group["actors"],
                "director": group["director"],
                "trailer_url": group["trailer_url"],
                "trailer_key": group["trailer_key"],
                "cast_profiles": group["cast_profiles"],
                "size": size,
            }
            group["items"].append(card)

            if group["type"] == "movie":
                prev = group["qualities"].get(quality)
                if not prev or int(prev.get("size") or 0) <= size:
                    group["qualities"][quality] = {"file_id": file_id, "size": size}
            else:
                season_bucket = group["seasons"].setdefault(season, {})
                episode_bucket = season_bucket.setdefault(episode, {})
                episode_bucket[quality] = {"file_id": file_id, "size": size}
                if episode_title:
                    title_map = group["episode_titles"].setdefault(season, {})
                    title_map[episode] = episode_title

        if not group["items"]:
            continue

        total_size = sum(int(row.get("size") or 0) for row in group["items"])
        group["file_count"] = len(group["items"])
        group["total_size"] = total_size

        if group["type"] == "movie":
            movie_qualities = sorted(group["qualities"].keys(), key=_quality_rank, reverse=True)
            group["primary_quality"] = movie_qualities[0] if movie_qualities else "HD"
            group["quality"] = group["primary_quality"]
            group["card_labels"] = movie_qualities[:3] if movie_qualities else ["HD"]
        else:
            season_numbers = sorted(int(s) for s in group["seasons"].keys())
            group["season_count"] = len(season_numbers)
            group["primary_quality"] = f"S{season_numbers[0]:02d}" if season_numbers else "Series"
            group["quality"] = group["primary_quality"]
            labels = [f"Season {s}" for s in season_numbers[:3]]
            if group["season_count"] > 3:
                labels.append(f"+{group['season_count'] - 3} more")
            group["card_labels"] = labels or ["Series"]

        groups.append(group)

    return groups
