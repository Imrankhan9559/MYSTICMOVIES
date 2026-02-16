import asyncio
import csv
import io
import re
from difflib import SequenceMatcher
from datetime import datetime
from typing import Any

from beanie.operators import In
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.core.config import settings
from app.db.models import FileSystemItem, MassContentState, User
from app.routes.admin import _admin_context_base, _build_title_regex, _is_admin, _publish_items
from app.routes.content import _parse_name, _tmdb_details, _tmdb_get, _tmdb_search
from app.routes.dashboard import _cast_ids, get_current_user
from app.utils.file_utils import format_size

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_MASS_WS_CLIENTS: set[WebSocket] = set()
_MASS_TASKS: dict[str, asyncio.Task] = {}
_MASS_UPLOAD_TASKS: dict[str, asyncio.Task] = {}
_MASS_LOCKS: dict[str, asyncio.Lock] = {}
_MASS_BROADCAST_LOCK = asyncio.Lock()
_MASS_PROCESS_SEMAPHORE = asyncio.Semaphore(3)
_STORAGE_POOL_CACHE: dict[str, Any] = {"rows": [], "expires_at": datetime.min}
_STORAGE_POOL_TTL_SECONDS = 45

_SERIES_SE_RE = re.compile(r"[Ss](\d{1,2})\s*[Ee](\d{1,3})")
_SERIES_SE_ALT_RE = re.compile(r"\b(\d{1,2})x(\d{1,3})\b", re.I)
_SERIES_WORD_RE = re.compile(r"\bSeason\s*(\d{1,2})\b.*?\bEpisode\s*(\d{1,3})\b", re.I)


def _norm_title(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _tokens(text: str) -> list[str]:
    return [x for x in re.split(r"[^a-z0-9]+", (text or "").lower()) if x]


def _to_title_marker(title: str) -> str:
    clean = " ".join((title or "").split()).strip()
    return f"title:{clean}" if clean else ""


def _to_source_marker(source_note: str) -> str:
    clean = (source_note or "").strip().lower()
    return f"source:{clean}" if clean else ""


def _extract_title_markers(values: list[str] | None) -> list[str]:
    out: list[str] = []
    for raw in values or []:
        text = str(raw or "").strip()
        if not text:
            continue
        lower = text.lower()
        if lower.startswith("title:"):
            t = text.split(":", 1)[1].strip()
            if t:
                out.append(t)
            continue
        if lower.startswith("source:"):
            continue
        if lower in {"manual", "csv_excel"}:
            continue
        # Backward compatibility: old rows may have saved plain titles directly.
        out.append(text)
    # Unique preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for t in out:
        key = _norm_title(t)
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(t)
    return uniq


def _expand_title_variants(title: str) -> list[str]:
    clean = " ".join((title or "").split()).strip()
    if not clean:
        return []
    variants = [clean]
    parsed = _parse_name(clean)
    parsed_title = " ".join((parsed.get("title") or "").split()).strip()
    if parsed_title:
        variants.append(parsed_title)
    # Remove common suffix qualifiers.
    for sep in [":", "-", "|", "("]:
        if sep in clean:
            part = clean.split(sep, 1)[0].strip()
            if part and len(_tokens(part)) >= 2:
                variants.append(part)
    # Compact form with stop words removed helps with noisy filenames.
    stop = {"the", "a", "an", "movie", "series", "season", "episode", "part", "vol", "volume"}
    token_list = [t for t in _tokens(clean) if t not in stop]
    if token_list:
        variants.append(" ".join(token_list))
    # Unique preserve order
    seen: set[str] = set()
    out: list[str] = []
    for value in variants:
        key = _norm_title(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _guess_type(raw_title: str, hint: str) -> str:
    value = (hint or "").strip().lower()
    if value in {"movie", "series"}:
        return value
    name = (raw_title or "").lower()
    if re.search(r"\b(series|web\s*series|tv|season|episode|s\d{1,2}e\d{1,3})\b", name):
        return "series"
    return "movie"


def _quality_rank(label: str) -> int:
    q = (label or "").upper().replace(" ", "")
    order = {"4K": 6, "2160P": 6, "1440P": 5, "1080P": 4, "720P": 3, "480P": 2, "360P": 1, "HD": 0}
    return order.get(q, 0)


def _series_quality_from_name(name: str) -> str:
    lower = (name or "").lower()
    if "2160" in lower or "4k" in lower:
        return "4K"
    if "1080" in lower:
        return "1080P"
    if "720" in lower:
        return "720P"
    if "480" in lower:
        return "480P"
    if "360" in lower or "380" in lower:
        return "360P"
    return "HD"


def _movie_quality_from_size(size: int) -> str:
    total = int(size or 0)
    gb = 1024 * 1024 * 1024
    if total > 2 * gb:
        return "1080P"
    if total >= 1 * gb:
        return "720P"
    return "480P"


def _series_season_episode(name: str) -> tuple[int | None, int | None]:
    raw = name or ""
    match = _SERIES_SE_RE.search(raw)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = _SERIES_SE_ALT_RE.search(raw)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = _SERIES_WORD_RE.search(raw)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def _is_video_row(item: FileSystemItem) -> bool:
    if item.is_folder:
        return False
    mime = (item.mime_type or "").lower()
    if mime.startswith("video"):
        return True
    lower = (item.name or "").lower()
    return lower.endswith((".mkv", ".mp4", ".avi", ".mov", ".wmv", ".webm", ".m4v"))


def _source_label(item: FileSystemItem) -> str:
    source = (item.source or "").strip().lower()
    return "Bot Listening" if source == "bot" else "Telegram Storage"


def _source_upload_label(item: FileSystemItem) -> str:
    source = (item.source or "").strip().lower()
    return "Uploaded from Bot Listener" if source == "bot" else "Uploaded from Telegram Storage"


def _title_match(target_titles: list[str], file_name: str, row_title: str = "", row_series_title: str = "") -> bool:
    parsed = _parse_name(file_name or "")
    parsed_title = (parsed.get("title") or "").strip()

    samples = [file_name or "", parsed_title, row_title or "", row_series_title or ""]
    sample_norms = [_norm_title(s) for s in samples if s]
    sample_token_sets = [set(_tokens(s)) for s in samples if s]

    for target in target_titles:
        target_norm = _norm_title(target)
        if not target_norm:
            continue
        target_tokens = set(_tokens(target))
        if not target_tokens:
            continue

        for sample_norm in sample_norms:
            if not sample_norm:
                continue
            if target_norm in sample_norm or sample_norm in target_norm:
                return True
            # Fuzzy fallback for reordered/noisy names.
            sim = SequenceMatcher(None, target_norm, sample_norm).ratio()
            token_count = len(target_tokens)
            threshold = 0.84 if token_count <= 2 else (0.78 if token_count == 3 else 0.72)
            if sim >= threshold:
                return True

        for sample_tokens in sample_token_sets:
            if not sample_tokens:
                continue
            overlap = len(target_tokens.intersection(sample_tokens))
            required = 1 if len(target_tokens) <= 2 else min(3, max(2, len(target_tokens) - 1))
            if overlap >= required:
                return True
    return False


def _panel_label(panel: str) -> str:
    labels = {
        "processing": "Searching / Processing",
        "tmdb_not_found": "TMDB Not Found",
        "file_not_found": "File Not Found",
        "incomplete": "Incomplete Content",
        "complete": "Complete Content",
    }
    return labels.get(panel, panel)


def _file_note_suffix(row: dict) -> str:
    name = (row.get("name") or "").strip()
    size_label = format_size(int(row.get("size") or 0))
    if name:
        return f" | {name} ({size_label})"
    return f" | {size_label}"


def _parse_csv_payload(raw: bytes) -> list[dict]:
    text = ""
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except Exception:
            text = ""
    if not text.strip():
        return []

    stream = io.StringIO(text)
    reader = csv.reader(stream)
    all_rows = [list(row) for row in reader if any((cell or "").strip() for cell in row)]
    if not all_rows:
        return []

    header = [str(x or "").strip().lower() for x in all_rows[0]]
    title_idx = -1
    type_idx = -1
    year_idx = -1
    for idx, col in enumerate(header):
        if col in {"title", "name", "content", "content_name"} and title_idx < 0:
            title_idx = idx
        if col in {"type", "content_type", "category"} and type_idx < 0:
            type_idx = idx
        if col in {"year", "release_year"} and year_idx < 0:
            year_idx = idx

    start_row = 1 if title_idx >= 0 else 0
    if title_idx < 0:
        title_idx = 0

    out: list[dict] = []
    for row in all_rows[start_row:]:
        if title_idx >= len(row):
            continue
        title = str(row[title_idx] or "").strip()
        if not title:
            continue
        type_hint = str(row[type_idx] or "").strip().lower() if type_idx >= 0 and type_idx < len(row) else ""
        year = str(row[year_idx] or "").strip() if year_idx >= 0 and year_idx < len(row) else ""
        out.append({"title": title, "type": type_hint, "year": year})
    return out


def _parse_xlsx_payload(raw: bytes) -> list[dict]:
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception as exc:
        raise ValueError("XLSX parsing requires openpyxl. Add openpyxl to requirements.") from exc

    workbook = load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    sheet = workbook.active
    matrix: list[list[str]] = []
    for row in sheet.iter_rows(values_only=True):
        values = [str(x or "").strip() for x in row]
        if any(values):
            matrix.append(values)
    if not matrix:
        return []

    header = [str(x).lower() for x in matrix[0]]
    title_idx = -1
    type_idx = -1
    year_idx = -1
    for idx, col in enumerate(header):
        if col in {"title", "name", "content", "content_name"} and title_idx < 0:
            title_idx = idx
        if col in {"type", "content_type", "category"} and type_idx < 0:
            type_idx = idx
        if col in {"year", "release_year"} and year_idx < 0:
            year_idx = idx

    start_row = 1 if title_idx >= 0 else 0
    if title_idx < 0:
        title_idx = 0

    out: list[dict] = []
    for row in matrix[start_row:]:
        if title_idx >= len(row):
            continue
        title = str(row[title_idx] or "").strip()
        if not title:
            continue
        type_hint = str(row[type_idx] or "").strip().lower() if type_idx >= 0 and type_idx < len(row) else ""
        year = str(row[year_idx] or "").strip() if year_idx >= 0 and year_idx < len(row) else ""
        out.append({"title": title, "type": type_hint, "year": year})
    return out


def _dedupe_rows(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        title = (row.get("title") or "").strip()
        if not title:
            continue
        ctype = _guess_type(title, row.get("type") or "")
        key = f"{ctype}:{_norm_title(title)}"
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "title": title,
            "type": ctype,
            "year": (row.get("year") or "").strip(),
        })
    return out


async def _fetch_storage_candidates(title: str) -> list[FileSystemItem]:
    return await _fetch_storage_candidates_multi([title])


def _search_titles_for_state(state: MassContentState) -> list[str]:
    values: list[str] = []
    values.extend(_extract_title_markers(list(state.source_inputs or [])))
    if state.title:
        values.append(state.title)
    expanded: list[str] = []
    for v in values:
        expanded.extend(_expand_title_variants(v))
    # Unique preserve order
    seen: set[str] = set()
    out: list[str] = []
    for v in expanded:
        key = _norm_title(v)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


async def _get_storage_pool_cached() -> list[FileSystemItem]:
    now = datetime.now()
    expires_at = _STORAGE_POOL_CACHE.get("expires_at") or datetime.min
    rows = _STORAGE_POOL_CACHE.get("rows") or []
    if rows and now < expires_at:
        return rows
    query = {
        "is_folder": False,
        "$or": [
            {"source": "bot"},
            {"source": "storage", "catalog_status": {"$nin": ["published", "used"]}},
        ],
    }
    fresh = await FileSystemItem.find(query).sort("-created_at").limit(5500).to_list()
    _STORAGE_POOL_CACHE["rows"] = fresh
    _STORAGE_POOL_CACHE["expires_at"] = datetime.fromtimestamp(now.timestamp() + _STORAGE_POOL_TTL_SECONDS)
    return fresh


async def _fetch_storage_candidates_multi(search_titles: list[str]) -> list[FileSystemItem]:
    expanded: list[str] = []
    for title in search_titles:
        expanded.extend(_expand_title_variants(title))
    # Deduplicate and keep meaningful titles
    seen: set[str] = set()
    title_variants: list[str] = []
    for title in expanded:
        key = _norm_title(title)
        if not key or key in seen:
            continue
        seen.add(key)
        title_variants.append(title)

    if not title_variants:
        return []

    # First pass: database regex match for efficiency.
    name_or: list[dict] = []
    for title in title_variants[:10]:
        pattern = _build_title_regex(title)
        if not pattern:
            continue
        name_or.append({"name": {"$regex": pattern, "$options": "i"}})
        name_or.append({"title": {"$regex": pattern, "$options": "i"}})
        name_or.append({"series_title": {"$regex": pattern, "$options": "i"}})

    rows: list[FileSystemItem] = []
    if name_or:
        query = {
            "is_folder": False,
            "$and": [
                {"$or": name_or},
                {
                    "$or": [
                        {"source": "bot"},
                        {"source": "storage", "catalog_status": {"$nin": ["published", "used"]}},
                    ]
                },
            ],
        }
        rows = await FileSystemItem.find(query).sort("-created_at").limit(3500).to_list()

    # Fallback pass: broader scan if strict query produced very few hits.
    if len(rows) < 3:
        fallback_rows = await _get_storage_pool_cached()
        if fallback_rows:
            # Merge by id
            merged = {str(x.id): x for x in rows}
            for item in fallback_rows:
                merged[str(item.id)] = item
            rows = list(merged.values())

    filtered = [
        row for row in rows
        if _is_video_row(row)
        and _title_match(
            title_variants,
            row.name or "",
            row_title=(getattr(row, "title", "") or ""),
            row_series_title=(getattr(row, "series_title", "") or ""),
        )
    ]
    # Keep newest first
    filtered.sort(key=lambda x: getattr(x, "created_at", None) or datetime.min, reverse=True)
    return filtered


def _best_row(rows: list[dict], season: int, episode: int, quality: str | None = None) -> dict | None:
    picks = [r for r in rows if int(r.get("season") or 0) == season and int(r.get("episode") or 0) == episode]
    if quality:
        q = (quality or "").upper()
        picks = [r for r in picks if (r.get("quality") or "").upper() == q]
    if not picks:
        return None
    picks.sort(
        key=lambda x: (
            _quality_rank(x.get("quality") or ""),
            1 if (x.get("source") or "").lower() == "storage" else 0,
            int(x.get("size") or 0),
        ),
        reverse=True,
    )
    return picks[0]


async def _scan_files_for_state(
    state: MassContentState,
) -> tuple[list[dict], list[dict], list[dict], list[dict], str, str, bool]:
    search_titles = _search_titles_for_state(state)
    candidates = await _fetch_storage_candidates_multi(search_titles or [state.title])
    found_rows: list[dict] = []
    for row in candidates:
        if state.content_type == "series":
            season, episode = _series_season_episode(row.name or "")
            if season is None or episode is None:
                continue
            quality = _series_quality_from_name(row.name or "")
            found_rows.append(
                {
                    "file_id": str(row.id),
                    "name": row.name,
                    "size": int(row.size or 0),
                    "quality": quality,
                    "season": int(season),
                    "episode": int(episode),
                    "source": row.source or "",
                    "source_label": _source_label(row),
                    "upload_label": _source_upload_label(row),
                }
            )
        else:
            quality = _movie_quality_from_size(int(row.size or 0))
            found_rows.append(
                {
                    "file_id": str(row.id),
                    "name": row.name,
                    "size": int(row.size or 0),
                    "quality": quality,
                    "season": None,
                    "episode": None,
                    "source": row.source or "",
                    "source_label": _source_label(row),
                    "upload_label": _source_upload_label(row),
                }
            )

    # Deduplicate rows by file id.
    uniq: dict[str, dict] = {}
    for row in found_rows:
        uniq[row["file_id"]] = row
    found_rows = list(uniq.values())

    notes: list[dict] = []
    missing: list[dict] = []
    upload_plan: list[dict] = []
    panel = "file_not_found"
    file_status = "missing"
    upload_ready = False

    if state.content_type == "movie":
        by_quality: dict[str, dict] = {}
        for row in found_rows:
            quality = (row.get("quality") or "").upper()
            current = by_quality.get(quality)
            if not current or int(row.get("size") or 0) > int(current.get("size") or 0):
                by_quality[quality] = row

        required = ["1080P", "720P", "480P"]
        for quality in required:
            picked = by_quality.get(quality)
            if picked:
                notes.append(
                    {
                        "season": None,
                        "episode": None,
                        "quality": quality,
                        "state": "found",
                        "text": f"{quality} -> Found ({picked.get('upload_label')}){_file_note_suffix(picked)}",
                    }
                )
            else:
                notes.append(
                    {
                        "season": None,
                        "episode": None,
                        "quality": quality,
                        "state": "missing",
                        "text": f"{quality} -> NOT FOUND (Upload Required)",
                    }
                )
                missing.append({"quality": quality, "note": f"Missing movie quality {quality}"})

        sorted_found = sorted(by_quality.values(), key=lambda x: _quality_rank(x.get("quality") or ""), reverse=True)[:4]
        for row in sorted_found:
            upload_plan.append(
                {
                    "file_id": row.get("file_id"),
                    "quality": (row.get("quality") or "HD").upper(),
                    "season": None,
                    "episode": None,
                    "episode_title": "",
                }
            )

        if not found_rows:
            panel = "file_not_found"
            file_status = "missing"
            upload_ready = False
        elif not missing:
            panel = "complete"
            file_status = "complete"
            upload_ready = True
        else:
            panel = "incomplete"
            file_status = "incomplete"
            upload_ready = False
        return found_rows, notes, missing, upload_plan, panel, file_status, upload_ready

    seasons = list(state.seasons or [])
    if not seasons:
        # If TMDB has no seasons payload, infer from file names.
        grouped: dict[int, set[int]] = {}
        for row in found_rows:
            season = int(row.get("season") or 0)
            episode = int(row.get("episode") or 0)
            if season <= 0 or episode <= 0:
                continue
            grouped.setdefault(season, set()).add(episode)
        seasons = [
            {
                "season": season,
                "name": f"Season {season}",
                "episodes": [{"episode": ep, "name": ""} for ep in sorted(list(episodes))],
            }
            for season, episodes in sorted(grouped.items())
        ]

    season_ready: list[bool] = []
    for season_row in seasons:
        season_no = int(season_row.get("season") or 0)
        episode_rows = list(season_row.get("episodes") or [])
        expected_episodes = sorted([int(x.get("episode") or 0) for x in episode_rows if int(x.get("episode") or 0) > 0])
        if not expected_episodes:
            continue

        season_found = [r for r in found_rows if int(r.get("season") or 0) == season_no]
        quality_to_episodes: dict[str, set[int]] = {}
        for row in season_found:
            quality = (row.get("quality") or "HD").upper()
            quality_to_episodes.setdefault(quality, set()).add(int(row.get("episode") or 0))

        complete_qualities = []
        for quality, covered in quality_to_episodes.items():
            if all(ep in covered for ep in expected_episodes):
                complete_qualities.append(quality)

        chosen_quality = "1080P"
        if complete_qualities:
            complete_qualities.sort(key=_quality_rank, reverse=True)
            chosen_quality = complete_qualities[0]
        elif quality_to_episodes:
            ranked = sorted(
                quality_to_episodes.items(),
                key=lambda kv: (len(kv[1]), _quality_rank(kv[0])),
                reverse=True,
            )
            chosen_quality = ranked[0][0]

        if len(complete_qualities) > 4:
            complete_qualities = sorted(complete_qualities, key=_quality_rank, reverse=True)[:4]
        season_ready.append(len(complete_qualities) > 0)

        for episode_no in expected_episodes:
            episode_title = ""
            for entry in episode_rows:
                if int(entry.get("episode") or 0) == episode_no:
                    episode_title = (entry.get("name") or "").strip()
                    break

            same_quality = _best_row(season_found, season_no, episode_no, chosen_quality)
            any_quality = _best_row(season_found, season_no, episode_no, None)
            if same_quality:
                notes.append(
                    {
                        "season": season_no,
                        "episode": episode_no,
                        "quality": chosen_quality,
                        "state": "found",
                        "text": f"Season {season_no} Episode {episode_no} {chosen_quality} -> Found ({same_quality.get('upload_label')}){_file_note_suffix(same_quality)}",
                    }
                )
                upload_plan.append(
                    {
                        "file_id": same_quality.get("file_id"),
                        "quality": chosen_quality,
                        "season": season_no,
                        "episode": episode_no,
                        "episode_title": episode_title,
                    }
                )
            elif any_quality:
                quality = (any_quality.get("quality") or "HD").upper()
                notes.append(
                    {
                        "season": season_no,
                        "episode": episode_no,
                        "quality": quality,
                        "state": "found_partial",
                        "text": f"Season {season_no} Episode {episode_no} {quality} -> Found ({any_quality.get('upload_label')}){_file_note_suffix(any_quality)}",
                    }
                )
                upload_plan.append(
                    {
                        "file_id": any_quality.get("file_id"),
                        "quality": quality,
                        "season": season_no,
                        "episode": episode_no,
                        "episode_title": episode_title,
                    }
                )
                missing.append(
                    {
                        "season": season_no,
                        "episode": episode_no,
                        "quality": chosen_quality,
                        "note": f"Season {season_no} Episode {episode_no} missing at {chosen_quality}",
                    }
                )
            else:
                notes.append(
                    {
                        "season": season_no,
                        "episode": episode_no,
                        "quality": chosen_quality,
                        "state": "missing",
                        "text": f"Season {season_no} Episode {episode_no} {chosen_quality} -> NOT FOUND (Upload Required)",
                    }
                )
                missing.append(
                    {
                        "season": season_no,
                        "episode": episode_no,
                        "quality": chosen_quality,
                        "note": f"Season {season_no} Episode {episode_no} missing",
                    }
                )

    # Deduplicate upload plan by file id while preserving order.
    seen_ids: set[str] = set()
    clean_upload_plan: list[dict] = []
    for row in upload_plan:
        file_id = (row.get("file_id") or "").strip()
        if not file_id or file_id in seen_ids:
            continue
        seen_ids.add(file_id)
        clean_upload_plan.append(row)
    upload_plan = clean_upload_plan

    if not found_rows:
        panel = "file_not_found"
        file_status = "missing"
        upload_ready = False
    elif season_ready and all(season_ready):
        panel = "complete"
        file_status = "complete"
        upload_ready = True
    else:
        panel = "incomplete"
        file_status = "incomplete"
        upload_ready = False

    return found_rows, notes, missing, upload_plan, panel, file_status, upload_ready


async def _upsert_mass_entry(title: str, content_type: str, year: str, source_note: str) -> MassContentState:
    clean_title = " ".join((title or "").split()).strip()
    if not clean_title:
        raise ValueError("Missing title")
    guessed_type = _guess_type(clean_title, content_type)
    key = f"{guessed_type}:{_norm_title(clean_title)}"

    existing = await MassContentState.find_one(MassContentState.key == key)
    if existing:
        changed = False
        if year and not existing.year:
            existing.year = year
            changed = True
        markers: list[str] = []
        source_marker = _to_source_marker(source_note)
        title_marker = _to_title_marker(clean_title)
        if source_marker:
            markers.append(source_marker)
        if title_marker:
            markers.append(title_marker)
        merged_inputs = list(existing.source_inputs or [])
        for marker in markers:
            if marker and marker not in merged_inputs:
                merged_inputs.append(marker)
                changed = True
        if merged_inputs != list(existing.source_inputs or []):
            existing.source_inputs = merged_inputs
            changed = True
        if existing.panel in {"tmdb_not_found", "file_not_found", "incomplete"}:
            existing.panel = "processing"
            existing.tmdb_status = "pending"
            existing.file_status = "pending"
            existing.last_error = None
            changed = True
        if changed:
            existing.updated_at = datetime.now()
            await existing.save()
        return existing

    row = MassContentState(
        key=key,
        title=clean_title,
        normalized_title=_norm_title(clean_title),
        content_type=guessed_type,
        year=(year or "").strip() or None,
        panel="processing",
        tmdb_status="pending",
        file_status="pending",
        source_inputs=[x for x in [_to_source_marker(source_note), _to_title_marker(clean_title)] if x],
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    await row.insert()
    return row


async def _process_mass_item(item_id: str, mode: str = "full") -> None:
    lock = _MASS_LOCKS.setdefault(item_id, asyncio.Lock())
    async with lock:
        row = await MassContentState.get(item_id)
        if not row:
            return

        row.panel = "processing"
        if mode in {"full", "tmdb"}:
            row.tmdb_status = "pending"
            row.last_error = None
        if mode in {"full", "files"}:
            row.file_status = "pending"
        row.updated_at = datetime.now()
        await row.save()
        await _mass_broadcast_snapshot()

        try:
            if mode in {"full", "tmdb"}:
                search = await _tmdb_search(row.title, row.year or "", row.content_type == "series")
                results = (search or {}).get("results") or []
                if not results:
                    row.tmdb_status = "not_found"
                    row.panel = "tmdb_not_found"
                    row.file_status = "pending"
                    row.upload_ready = False
                    row.updated_at = datetime.now()
                    await row.save()
                    await _mass_broadcast_snapshot()
                    return

                pick = results[0]
                tmdb_id = pick.get("id")
                if not tmdb_id:
                    row.tmdb_status = "not_found"
                    row.panel = "tmdb_not_found"
                    row.updated_at = datetime.now()
                    await row.save()
                    await _mass_broadcast_snapshot()
                    return

                details = await _tmdb_details(int(tmdb_id), row.content_type == "series")
                if not details:
                    row.tmdb_status = "not_found"
                    row.panel = "tmdb_not_found"
                    row.updated_at = datetime.now()
                    await row.save()
                    await _mass_broadcast_snapshot()
                    return

                pre_tmdb_title = (row.title or "").strip()
                tmdb_title = details.get("name") if row.content_type == "series" else details.get("title")
                if tmdb_title:
                    marker = _to_title_marker(pre_tmdb_title)
                    if marker and marker not in (row.source_inputs or []):
                        row.source_inputs = list(row.source_inputs or []) + [marker]
                    row.title = tmdb_title.strip()
                    row.normalized_title = _norm_title(row.title)

                release_date = (details.get("release_date") or details.get("first_air_date") or "").strip()
                year = release_date[:4] if release_date else (row.year or "")
                if year:
                    row.year = year

                poster_path = details.get("poster_path")
                backdrop_path = details.get("backdrop_path")
                row.poster_url = f"https://image.tmdb.org/t/p/w780{poster_path}" if poster_path else (row.poster_url or "")
                row.backdrop_url = f"https://image.tmdb.org/t/p/w1280{backdrop_path}" if backdrop_path else (row.backdrop_url or "")
                row.description = (details.get("overview") or "").strip()
                row.release_date = release_date or (row.release_date or "")
                row.tmdb_id = int(tmdb_id)
                row.genres = [g.get("name") for g in (details.get("genres") or []) if g.get("name")]

                credits = details.get("credits") or {}
                cast_rows = credits.get("cast") or []
                row.actors = [c.get("name") for c in cast_rows[:12] if c.get("name")]
                cast_profiles = []
                for c in cast_rows[:12]:
                    if not c.get("name"):
                        continue
                    profile_path = c.get("profile_path")
                    cast_profiles.append(
                        {
                            "id": c.get("id"),
                            "name": c.get("name"),
                            "role": c.get("character") or "",
                            "image": f"https://image.tmdb.org/t/p/w185{profile_path}" if profile_path else "",
                        }
                    )
                row.cast_profiles = cast_profiles

                director = ""
                for c in credits.get("crew") or []:
                    if c.get("job") == "Director":
                        director = c.get("name") or ""
                        break
                if not director and row.content_type == "series":
                    for c in credits.get("crew") or []:
                        if c.get("job") in {"Creator", "Executive Producer"}:
                            director = c.get("name") or ""
                            break
                row.director = director

                trailer_url = ""
                trailer_key = ""
                for v in (details.get("videos", {}) or {}).get("results", []) or []:
                    if v.get("site") == "YouTube" and v.get("type") in {"Trailer", "Teaser"}:
                        trailer_key = v.get("key") or ""
                        if trailer_key:
                            trailer_url = f"https://www.youtube.com/watch?v={trailer_key}"
                        break
                row.trailer_key = trailer_key
                row.trailer_url = trailer_url

                seasons: list[dict] = []
                if row.content_type == "series":
                    for season in details.get("seasons") or []:
                        season_no = int(season.get("season_number") or 0)
                        if season_no <= 0:
                            continue
                        season_details = await _tmdb_get(f"/tv/{tmdb_id}/season/{season_no}", {})
                        episodes = []
                        for ep in season_details.get("episodes") or []:
                            ep_no = int(ep.get("episode_number") or 0)
                            if ep_no <= 0:
                                continue
                            episodes.append({"episode": ep_no, "name": (ep.get("name") or "").strip()})
                        seasons.append(
                            {
                                "season": season_no,
                                "name": season.get("name") or f"Season {season_no}",
                                "episode_count": int(season.get("episode_count") or len(episodes)),
                                "episodes": episodes,
                            }
                        )
                row.seasons = sorted(seasons, key=lambda x: int(x.get("season") or 0))
                row.tmdb_status = "found"

            if row.tmdb_status != "found":
                row.panel = "tmdb_not_found"
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()
                return

            found_rows, notes, missing, upload_plan, panel, file_status, upload_ready = await _scan_files_for_state(row)
            row.matched_files = found_rows
            row.live_notes = notes
            row.missing_items = missing
            row.upload_plan = upload_plan
            row.panel = panel
            row.file_status = file_status
            row.upload_ready = bool(upload_ready)
            row.last_error = None
            row.updated_at = datetime.now()
            await row.save()
            await _mass_broadcast_snapshot()
        except Exception as exc:
            row.panel = "processing"
            row.last_error = str(exc)
            row.updated_at = datetime.now()
            await row.save()
            await _mass_broadcast_snapshot()


def _schedule_process(item_id: str, mode: str = "full") -> None:
    existing = _MASS_TASKS.get(item_id)
    if existing and not existing.done():
        return

    async def _runner():
        try:
            async with _MASS_PROCESS_SEMAPHORE:
                await _process_mass_item(item_id, mode=mode)
        finally:
            _MASS_TASKS.pop(item_id, None)

    _MASS_TASKS[item_id] = asyncio.create_task(_runner())


def _serialize_row(row: MassContentState) -> dict:
    return {
        "id": str(row.id),
        "title": row.title,
        "content_type": row.content_type,
        "year": row.year or "",
        "panel": row.panel,
        "panel_label": _panel_label(row.panel),
        "tmdb_status": row.tmdb_status,
        "file_status": row.file_status,
        "upload_ready": bool(row.upload_ready),
        "uploaded": bool(row.uploaded),
        "uploaded_at": row.uploaded_at.isoformat() if row.uploaded_at else "",
        "poster_url": row.poster_url or "",
        "release_date": row.release_date or "",
        "missing_count": len(row.missing_items or []),
        "missing_items": list(row.missing_items or []),
        "live_notes": list(row.live_notes or []),
        "matched_files_count": len(row.matched_files or []),
        "source_inputs": list(row.source_inputs or []),
        "upload_state": str(getattr(row, "upload_state", "") or "idle"),
        "upload_message": str(getattr(row, "upload_message", "") or ""),
        "last_error": row.last_error or "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
    }


async def _build_snapshot() -> dict:
    rows = await MassContentState.find_all().sort("-updated_at").to_list()
    payload = [_serialize_row(row) for row in rows]
    panels = {
        "processing": [],
        "tmdb_not_found": [],
        "file_not_found": [],
        "incomplete": [],
        "complete": [],
    }
    for row in payload:
        panel = row.get("panel") or "processing"
        if panel not in panels:
            panel = "processing"
        panels[panel].append(row)
    return {
        "panels": panels,
        "counts": {key: len(value) for key, value in panels.items()},
        "total": len(payload),
        "server_time": datetime.now().isoformat(),
    }


async def _mass_broadcast_snapshot() -> None:
    async with _MASS_BROADCAST_LOCK:
        if not _MASS_WS_CLIENTS:
            return
        snapshot = await _build_snapshot()
        dead: list[WebSocket] = []
        for ws in list(_MASS_WS_CLIENTS):
            try:
                await ws.send_json({"type": "snapshot", "data": snapshot})
            except Exception:
                dead.append(ws)
        for ws in dead:
            _MASS_WS_CLIENTS.discard(ws)


async def _ws_auth_admin(websocket: WebSocket) -> User | None:
    phone = websocket.cookies.get("user_phone")
    if not phone:
        return None
    user = await User.find_one(User.phone_number == phone)
    if not user:
        return None
    if not _is_admin(user):
        return None
    return user


@router.get("/advance-mass-content-adder")
async def advance_mass_content_adder_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    base_ctx = await _admin_context_base(user)
    initial_state = await _build_snapshot()
    return templates.TemplateResponse(
        "advance_mass_content_adder.html",
        {
            "request": request,
            **base_ctx,
            "tmdb_configured": bool(getattr(settings, "TMDB_API_KEY", "")),
            "initial_state": initial_state,
        },
    )


@router.get("/advance-mass-content-adder/state")
async def advance_mass_content_adder_state(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    return await _build_snapshot()


@router.websocket("/advance-mass-content-adder/ws")
async def advance_mass_content_adder_ws(websocket: WebSocket):
    user = await _ws_auth_admin(websocket)
    if not user:
        await websocket.close(code=4403)
        return
    await websocket.accept()
    _MASS_WS_CLIENTS.add(websocket)
    try:
        await websocket.send_json({"type": "snapshot", "data": await _build_snapshot()})
        while True:
            text = await websocket.receive_text()
            cmd = (text or "").strip().lower()
            if cmd in {"refresh", "ping"}:
                await websocket.send_json({"type": "snapshot", "data": await _build_snapshot()})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _MASS_WS_CLIENTS.discard(websocket)


@router.post("/advance-mass-content-adder/manual-add")
async def advance_mass_content_adder_manual_add(
    request: Request,
    title: str = Form(""),
    content_type: str = Form(""),
    year: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    name = (title or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "Title is required."}, status_code=400)
    try:
        row = await _upsert_mass_entry(name, content_type, (year or "").strip(), "manual")
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    _schedule_process(str(row.id), mode="full")
    await _mass_broadcast_snapshot()
    return {"ok": True, "id": str(row.id)}


@router.post("/advance-mass-content-adder/import")
async def advance_mass_content_adder_import(request: Request, file: UploadFile = File(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    filename = (file.filename or "").strip().lower()
    if not filename.endswith(".csv") and not filename.endswith(".xlsx"):
        return JSONResponse({"ok": False, "error": "Only .csv and .xlsx files are supported."}, status_code=400)
    payload = await file.read()
    if not payload:
        return JSONResponse({"ok": False, "error": "Uploaded file is empty."}, status_code=400)

    try:
        if filename.endswith(".csv"):
            rows = _parse_csv_payload(payload)
        else:
            rows = _parse_xlsx_payload(payload)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    rows = _dedupe_rows(rows)
    if not rows:
        return JSONResponse({"ok": False, "error": "No valid content rows found."}, status_code=400)

    count = 0
    for row in rows:
        state = await _upsert_mass_entry(
            title=row.get("title") or "",
            content_type=row.get("type") or "",
            year=row.get("year") or "",
            source_note="csv_excel",
        )
        count += 1
        _schedule_process(str(state.id), mode="full")

    await _mass_broadcast_snapshot()
    return {"ok": True, "count": count}


@router.post("/advance-mass-content-adder/retry-tmdb/{item_id}")
async def advance_mass_content_adder_retry_tmdb(request: Request, item_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    row = await MassContentState.get(item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    row.panel = "processing"
    row.tmdb_status = "pending"
    row.file_status = "pending"
    row.last_error = None
    row.updated_at = datetime.now()
    await row.save()
    _schedule_process(str(row.id), mode="full")
    await _mass_broadcast_snapshot()
    return {"ok": True}


@router.post("/advance-mass-content-adder/retry-files/{item_id}")
async def advance_mass_content_adder_retry_files(request: Request, item_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    row = await MassContentState.get(item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    if row.tmdb_status != "found":
        return JSONResponse({"ok": False, "error": "TMDB data is missing for this item."}, status_code=400)
    row.panel = "processing"
    row.file_status = "pending"
    row.last_error = None
    row.updated_at = datetime.now()
    await row.save()
    _STORAGE_POOL_CACHE["rows"] = []
    _STORAGE_POOL_CACHE["expires_at"] = datetime.min
    _schedule_process(str(row.id), mode="files")
    await _mass_broadcast_snapshot()
    return {"ok": True}


@router.post("/advance-mass-content-adder/delete/{item_id}")
async def advance_mass_content_adder_delete(request: Request, item_id: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")
    row = await MassContentState.get(item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    await row.delete()
    await _mass_broadcast_snapshot()
    return {"ok": True}


def _build_upload_payload(row: MassContentState) -> tuple[list[str], dict[str, dict], str]:
    plan = list(row.upload_plan or [])
    if not plan:
        return [], {}, "No files available for upload."

    raw_file_ids: list[str] = []
    overrides: dict[str, dict] = {}
    for entry in plan:
        file_id = (entry.get("file_id") or "").strip()
        if not file_id:
            continue
        raw_file_ids.append(file_id)
        payload = {"quality": (entry.get("quality") or "HD").upper()}
        if row.content_type == "series":
            payload["season"] = int(entry.get("season") or 1)
            payload["episode"] = int(entry.get("episode") or 1)
            payload["episode_title"] = (entry.get("episode_title") or "").strip()
        overrides[file_id] = payload

    raw_file_ids = list(dict.fromkeys(raw_file_ids))
    if not raw_file_ids:
        return [], {}, "No valid file ids found in upload plan."
    return raw_file_ids, overrides, ""


async def _run_upload_worker(item_id: str, allow_incomplete: bool) -> None:
    lock = _MASS_LOCKS.setdefault(f"upload:{item_id}", asyncio.Lock())
    async with lock:
        row = await MassContentState.get(item_id)
        if not row:
            return

        row.upload_state = "uploading"
        row.upload_message = "Uploading content in background..."
        row.updated_at = datetime.now()
        await row.save()
        await _mass_broadcast_snapshot()

        try:
            if row.tmdb_status != "found":
                row.upload_state = "failed"
                row.upload_message = "TMDB data not found."
                row.last_error = "TMDB data not found."
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()
                return
            if row.panel == "incomplete" and not allow_incomplete:
                row.upload_state = "failed"
                row.upload_message = "Incomplete content requires confirmation."
                row.last_error = "Incomplete content requires confirmation."
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()
                return

            raw_file_ids, overrides, build_err = _build_upload_payload(row)
            if build_err:
                row.upload_state = "failed"
                row.upload_message = build_err
                row.last_error = build_err
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()
                return

            items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(raw_file_ids))).to_list()
            items = [item for item in items if not item.is_folder]
            if not items:
                row.upload_state = "failed"
                row.upload_message = "Selected files are not available anymore."
                row.last_error = "Selected files are not available anymore."
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()
                return

            await _publish_items(
                items=items,
                catalog_type=row.content_type,
                title=row.title,
                year=(row.year or "").strip(),
                desc=(row.description or "").strip(),
                genres_list=list(row.genres or []),
                actors_list=list(row.actors or []),
                director=(row.director or "").strip(),
                trailer_url=(row.trailer_url or "").strip(),
                release_date=(row.release_date or "").strip(),
                poster_url=(row.poster_url or "").strip(),
                backdrop_url=(row.backdrop_url or "").strip(),
                trailer_key=(row.trailer_key or "").strip(),
                cast_profiles=list(row.cast_profiles or []),
                tmdb_id=row.tmdb_id,
                overrides=overrides,
            )
            _STORAGE_POOL_CACHE["rows"] = []
            _STORAGE_POOL_CACHE["expires_at"] = datetime.min

            row.uploaded = True
            row.uploaded_at = datetime.now()
            row.upload_state = "done"
            row.upload_message = "Upload completed successfully."
            row.last_error = None
            row.updated_at = datetime.now()
            await row.save()
            await _mass_broadcast_snapshot()
        except Exception as exc:
            row = await MassContentState.get(item_id)
            if row:
                row.upload_state = "failed"
                row.upload_message = str(exc)
                row.last_error = str(exc)
                row.updated_at = datetime.now()
                await row.save()
                await _mass_broadcast_snapshot()


def _queue_upload_worker(item_id: str, allow_incomplete: bool) -> bool:
    task = _MASS_UPLOAD_TASKS.get(item_id)
    if task and not task.done():
        return False

    async def _runner():
        try:
            await _run_upload_worker(item_id, allow_incomplete)
        finally:
            _MASS_UPLOAD_TASKS.pop(item_id, None)

    _MASS_UPLOAD_TASKS[item_id] = asyncio.create_task(_runner())
    return True


@router.post("/advance-mass-content-adder/upload/{item_id}")
async def advance_mass_content_adder_upload(
    request: Request,
    item_id: str,
    force_upload: str = Form("0"),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    row = await MassContentState.get(item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Item not found")
    if row.tmdb_status != "found":
        return JSONResponse({"ok": False, "error": "TMDB data not found for this item."}, status_code=400)

    allow_incomplete = (force_upload or "").strip().lower() in {"1", "true", "yes", "on"}
    if row.panel == "incomplete" and not allow_incomplete:
        return JSONResponse(
            {
                "ok": False,
                "needs_confirm": True,
                "message": "Content is incomplete. Confirm to upload with missing files.",
                "missing_items": list(row.missing_items or []),
            },
            status_code=409,
        )

    raw_ids, _, build_err = _build_upload_payload(row)
    if build_err:
        return JSONResponse({"ok": False, "error": build_err}, status_code=400)
    if not raw_ids:
        return JSONResponse({"ok": False, "error": "No files available for upload."}, status_code=400)

    queued = _queue_upload_worker(item_id, allow_incomplete)
    row.upload_state = "queued" if queued else "uploading"
    row.upload_message = "Upload queued. You can continue using this page."
    row.updated_at = datetime.now()
    await row.save()
    await _mass_broadcast_snapshot()
    return JSONResponse(
        {
            "ok": True,
            "queued": True,
            "already_running": (not queued),
            "message": "Upload queued in background.",
        },
        status_code=202,
    )


def _rows_for_panel(panel: str, rows: list[MassContentState]) -> list[MassContentState]:
    key = (panel or "").strip().lower()
    valid = {"processing", "tmdb_not_found", "file_not_found", "incomplete", "complete"}
    if key not in valid:
        return []
    return [row for row in rows if (row.panel or "") == key]


@router.get("/advance-mass-content-adder/export/{panel}")
async def advance_mass_content_adder_export(request: Request, panel: str):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    rows = await MassContentState.find_all().sort("-updated_at").to_list()
    filtered = _rows_for_panel(panel, rows)

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        [
            "title",
            "type",
            "year",
            "panel",
            "tmdb_status",
            "file_status",
            "upload_ready",
            "uploaded",
            "missing_count",
            "missing_details",
        ]
    )
    for row in filtered:
        missing_details = " | ".join([(x.get("note") or "") for x in (row.missing_items or []) if (x.get("note") or "")])
        writer.writerow(
            [
                row.title,
                row.content_type,
                row.year or "",
                row.panel,
                row.tmdb_status,
                row.file_status,
                "yes" if row.upload_ready else "no",
                "yes" if row.uploaded else "no",
                len(row.missing_items or []),
                missing_details,
            ]
        )

    payload = out.getvalue().encode("utf-8")
    filename = f"mass_adder_{panel}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return StreamingResponse(io.BytesIO(payload), media_type="text/csv; charset=utf-8", headers=headers)
