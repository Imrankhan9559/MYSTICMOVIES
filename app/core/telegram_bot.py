import logging
import os
import tempfile
import urllib.request
import urllib.parse
import json
import asyncio
import re
from datetime import datetime
from difflib import SequenceMatcher
from itertools import cycle
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.core.config import settings
from app.db.models import FileSystemItem, FilePart, User, SharedCollection
from beanie import PydanticObjectId
from beanie.operators import In
from app.core.telethon_storage import (
    check_storage_access as tl_check_storage,
    get_message as tl_get_message,
    forward_message_to as tl_forward_to_user,
    send_file as tl_send_file,
    send_text as tl_send_text,
    iter_storage_messages,
    iter_download as tl_iter_download
)

# Prefer uvloop before any Pyrogram clients are created
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

# Ensure a loop exists before Pyrogram Client construction (uvloop can require this)
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Pyrogram Clients
user_client = None
bot_client = None
_storage_chat_id_override: int | None = None
_storage_access_notified = False
_bot_handler_clients: set[str] = set()
_bot_api_task: asyncio.Task | None = None
_telegram_lifecycle_lock = asyncio.Lock()
_telegram_started = False

if settings.SESSION_STRING:
    user_client = Client(
        "morganxmystic_user",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        session_string=settings.SESSION_STRING
    )
    tg_client = user_client
    if settings.BOT_TOKEN:
        bot_client = Client(
            "morganxmystic_bot",
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            bot_token=settings.BOT_TOKEN
        )
else:
    tg_client = Client(
        "morganxmystic_bot",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        bot_token=settings.BOT_TOKEN
    )
    bot_client = tg_client

# Optional bot pool for parallel streaming/download
bot_pool: list[Client] = []
_bot_cycle = None
_bot_status_cache: list[dict] = []
_catalog_cache_data: list[dict] = []
_catalog_cache_ts: float = 0.0
_catalog_cache_ttl_sec = 45.0

QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|480p|380p|360p)", re.I)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
SE_RE = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")

QUALITY_ORDER = {
    "2160P": 7,
    "1440P": 6,
    "1080P": 5,
    "720P": 4,
    "480P": 3,
    "380P": 2,
    "360P": 1,
    "HD": 0,
}


def _normalize_phone(phone: str) -> str:
    return re.sub(r"\D+", "", (phone or ""))


def _is_admin_user(user: User | None) -> bool:
    if not user:
        return False
    role = str(getattr(user, "role", "") or "").strip().lower()
    if role == "admin":
        return True
    return _normalize_phone(getattr(user, "phone_number", "")) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))


def _site_base_url() -> str:
    raw = (
        getattr(settings, "SITE_URL", "")
        or os.getenv("SITE_URL", "")
        or os.getenv("RENDER_EXTERNAL_URL", "")
        or "https://mysticmovies.onrender.com"
    )
    value = (raw or "").strip()
    if not value:
        value = "https://mysticmovies.onrender.com"
    if not value.startswith("http://") and not value.startswith("https://"):
        value = "https://" + value
    return value.rstrip("/")


def _site_url(path: str = "/") -> str:
    clean_path = (path or "/").strip()
    if not clean_path.startswith("/"):
        clean_path = "/" + clean_path
    return _site_base_url() + clean_path


def _slugify(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower())
    return value.strip("-")


def _norm_text(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower())
    return re.sub(r"\s+", " ", normalized).strip()


def _extract_quality(name: str) -> str:
    match = QUALITY_RE.search(name or "")
    if match:
        return match.group(1).upper()
    return "HD"


def _quality_rank(value: str) -> int:
    return QUALITY_ORDER.get((value or "").strip().upper(), 0)


def _parse_year(value: str) -> str:
    match = YEAR_RE.search(value or "")
    return match.group(1) if match else ""


def _parse_season(name: str, season_value: int | None) -> int | None:
    if season_value:
        try:
            return int(season_value)
        except Exception:
            pass
    match = SE_RE.search(name or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _display_title(item: FileSystemItem) -> str:
    title = (getattr(item, "series_title", "") or getattr(item, "title", "") or "").strip()
    if title:
        return title
    raw = re.sub(r"\.[^.]+$", "", (item.name or "").strip())
    raw = re.sub(r"[._]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    raw = QUALITY_RE.sub("", raw)
    raw = re.sub(r"\bS\d{1,2}E\d{1,3}\b", "", raw, flags=re.I)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw or (item.name or "Unknown Title")


def _content_type(item: FileSystemItem) -> str:
    catalog_type = (getattr(item, "catalog_type", "") or "").strip().lower()
    if catalog_type in ("movie", "series"):
        return catalog_type
    if getattr(item, "season", None) or getattr(item, "episode", None):
        return "series"
    if SE_RE.search(item.name or ""):
        return "series"
    return "movie"


def _format_release_date(value: str, year: str) -> str:
    raw = (value or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%d %b %Y")
        except Exception:
            pass
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        try:
            dt = datetime.strptime(raw + "-01", "%Y-%m-%d")
            return dt.strftime("%d %b %Y")
        except Exception:
            pass
    clean_year = (year or _parse_year(raw)).strip()
    if re.fullmatch(r"\d{4}", clean_year):
        return f"01 Jan {clean_year}"
    return "Unknown"


def _format_seasons(seasons: list[int]) -> str:
    nums = sorted({int(s) for s in seasons if isinstance(s, int) or str(s).isdigit()})
    if not nums:
        return "Not listed"
    return ", ".join(str(n) for n in nums)


async def _google_spelling_suggestion(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return ""

    def _fetch() -> str:
        url = (
            "https://suggestqueries.google.com/complete/search?client=firefox&q="
            + urllib.parse.quote_plus(q)
        )
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            },
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list):
            for candidate in payload[1]:
                text = (candidate or "").strip()
                if text and _norm_text(text) != _norm_text(q):
                    return text
        return ""

    try:
        return await asyncio.to_thread(_fetch)
    except Exception:
        return ""


async def _build_published_catalog(limit: int = 5000) -> list[dict]:
    global _catalog_cache_data, _catalog_cache_ts
    now = asyncio.get_event_loop().time()
    if _catalog_cache_data and (now - _catalog_cache_ts) <= _catalog_cache_ttl_sec:
        return _catalog_cache_data

    rows = await FileSystemItem.find(
        FileSystemItem.is_folder == False,
        FileSystemItem.catalog_status == "published",
    ).sort("-created_at").limit(limit).to_list()

    grouped: dict[tuple[str, str, str], dict] = {}
    for item in rows:
        if not _is_video_item(item):
            continue
        title = _display_title(item)
        if not title:
            continue
        year = (getattr(item, "year", "") or _parse_year(item.name or "") or "").strip()
        ctype = _content_type(item)
        key = (_norm_text(title), year, ctype)
        if key not in grouped:
            slug = _slugify(title)
            if year:
                slug = f"{slug}-{year}" if slug else year
            grouped[key] = {
                "id": str(item.id),
                "title": title,
                "title_norm": key[0],
                "year": year,
                "type": ctype,
                "release_date": (getattr(item, "release_date", "") or "").strip(),
                "poster": (getattr(item, "poster_url", "") or "").strip(),
                "slug": slug,
                "qualities": set(),
                "seasons": set(),
            }
        group = grouped[key]
        quality = (getattr(item, "quality", "") or _extract_quality(item.name or "") or "HD").upper()
        group["qualities"].add(quality)
        if not group["release_date"] and getattr(item, "release_date", ""):
            group["release_date"] = (getattr(item, "release_date", "") or "").strip()
        if not group["poster"] and getattr(item, "poster_url", ""):
            group["poster"] = (getattr(item, "poster_url", "") or "").strip()
        season_no = _parse_season(item.name or "", getattr(item, "season", None))
        if ctype == "series" and season_no:
            group["seasons"].add(season_no)

    catalog = []
    for row in grouped.values():
        qualities = sorted(row["qualities"], key=lambda q: (-_quality_rank(q), q))
        seasons = sorted(row["seasons"])
        catalog.append({
            "id": row["id"],
            "title": row["title"],
            "title_norm": row["title_norm"],
            "year": row["year"],
            "type": row["type"],
            "release_date": row["release_date"],
            "poster": row["poster"],
            "slug": row["slug"],
            "qualities": qualities,
            "seasons": seasons,
        })

    _catalog_cache_data = catalog
    _catalog_cache_ts = now
    return catalog


def _rank_catalog_matches(query: str, catalog: list[dict], limit: int = 5) -> list[dict]:
    q_raw = (query or "").strip()
    q_norm = _norm_text(q_raw)
    if not q_norm:
        return []
    q_tokens = set(q_norm.split())

    scored = []
    for item in catalog:
        t_norm = item.get("title_norm", "")
        if not t_norm:
            continue
        t_tokens = set(t_norm.split())
        ratio = SequenceMatcher(None, q_norm, t_norm).ratio()
        overlap = len(q_tokens & t_tokens)
        contains = q_norm in t_norm
        reverse_contains = t_norm in q_norm
        score = ratio * 100.0
        if q_norm == t_norm:
            score += 80
        if contains:
            score += 35
        if reverse_contains:
            score += 20
        if overlap:
            score += overlap * 9
        q_year = _parse_year(q_raw)
        if q_year and q_year == (item.get("year", "") or ""):
            score += 8
        if score < 40:
            continue
        scored.append((score, item))

    scored.sort(key=lambda row: (-row[0], (row[1].get("title") or "").lower()))
    return [row[1] for row in scored[:limit]]


def _content_caption(item: dict, corrected_query: str = "") -> str:
    title = item.get("title", "Unknown")
    release_label = _format_release_date(item.get("release_date", ""), item.get("year", ""))
    qualities = ", ".join(item.get("qualities") or ["HD"])
    lines = []
    if corrected_query:
        lines.append(f"Showing results for: {corrected_query}")
        lines.append("")
    lines.append(f"Name: {title}")
    lines.append(f"Release Date: {release_label}")
    lines.append(f"Quality: {qualities}")
    if item.get("type") == "series":
        lines.append(f"Available Seasons: {_format_seasons(item.get('seasons') or [])}")
    lines.append(f"Type: {(item.get('type') or 'movie').title()}")
    return "\n".join(lines)


def _welcome_text(display_name: str, is_admin: bool) -> str:
    name = (display_name or "there").strip()
    lines = [
        f"Hi {name}, welcome to MysticMovies.",
        "Here you can watch, download, watch together, and get files in Telegram of your movies and series for free.",
        "Just send the file name.",
    ]
    if is_admin:
        lines.append("Admin upload mode is enabled for your account.")
    return "\n\n".join(lines)


async def _send_welcome_message(client: Client, chat_id: int, display_name: str, is_admin: bool):
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Visit Site", url=_site_url("/"))]]
    )
    await client.send_message(
        chat_id,
        _welcome_text(display_name, is_admin),
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def _bot_api_keyboard(buttons: list[list[dict]]) -> str:
    return json.dumps({"inline_keyboard": buttons})


async def _send_welcome_message_api(chat_id: int, display_name: str, is_admin: bool):
    await _bot_api_call(
        settings.BOT_TOKEN,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": _welcome_text(display_name, is_admin),
            "disable_web_page_preview": "true",
            "reply_markup": _bot_api_keyboard(
                [[{"text": "Visit Site", "url": _site_url("/")}]])
        },
    )


async def _send_content_result(client: Client, chat_id: int, item: dict, corrected_query: str = ""):
    title = item.get("title") or "Content"
    view_path = f"/content/details/{item.get('slug') or item.get('id')}"
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"View {title[:40]}", url=_site_url(view_path))]]
    )
    caption = _content_caption(item, corrected_query=corrected_query)
    poster = (item.get("poster") or "").strip()
    if poster:
        try:
            await client.send_photo(chat_id, poster, caption=caption, reply_markup=keyboard)
            return
        except Exception:
            pass
    await client.send_message(chat_id, caption, reply_markup=keyboard, disable_web_page_preview=True)


async def _send_not_found(client: Client, chat_id: int, query: str):
    text = (
        f"'{query}' is not uploaded yet.\n"
        "You can request it on the website."
    )
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Request on Website", url=_site_url("/request-content"))]]
    )
    await client.send_message(chat_id, text, reply_markup=keyboard, disable_web_page_preview=True)


async def _send_content_result_api(chat_id: int, item: dict, corrected_query: str = ""):
    title = item.get("title") or "Content"
    view_path = f"/content/details/{item.get('slug') or item.get('id')}"
    caption = _content_caption(item, corrected_query=corrected_query)
    reply_markup = _bot_api_keyboard([[{"text": f"View {title[:40]}", "url": _site_url(view_path)}]])
    poster = (item.get("poster") or "").strip()
    if poster:
        resp = await _bot_api_call(
            settings.BOT_TOKEN,
            "sendPhoto",
            {
                "chat_id": chat_id,
                "photo": poster,
                "caption": caption,
                "reply_markup": reply_markup,
            },
        )
        if resp.get("ok"):
            return
    await _bot_api_call(
        settings.BOT_TOKEN,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": caption,
            "disable_web_page_preview": "true",
            "reply_markup": reply_markup,
        },
    )


async def _send_not_found_api(chat_id: int, query: str):
    await _bot_api_call(
        settings.BOT_TOKEN,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": f"'{query}' is not uploaded yet.\nYou can request it on the website.",
            "disable_web_page_preview": "true",
            "reply_markup": _bot_api_keyboard(
                [[{"text": "Request on Website", "url": _site_url("/request-content")}]]
            ),
        },
    )


async def _linked_user_from_tg_id(telegram_user_id: int | None) -> User | None:
    if not telegram_user_id:
        return None
    try:
        return await User.find_one(User.telegram_user_id == int(telegram_user_id))
    except Exception:
        return None


def _client_key(client: Client | None) -> str:
    if not client:
        return ""
    return getattr(client, "name", None) or str(id(client))

def _is_client_connected(client: Client | None) -> bool:
    if not client:
        return False
    value = getattr(client, "is_connected", False)
    try:
        return bool(value() if callable(value) else value)
    except Exception:
        return False

def _forget_bot_handlers(client: Client | None) -> None:
    key = _client_key(client)
    if key:
        _bot_handler_clients.discard(key)

async def _safe_stop_client(client: Client | None, label: str, timeout_sec: float = 15.0) -> None:
    if not client or not _is_client_connected(client):
        return
    try:
        await asyncio.wait_for(client.stop(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        logger.warning("Timed out stopping %s after %.1fs", label, timeout_sec)
    except Exception as e:
        logger.warning("Failed to stop %s: %s", label, e)

async def _stop_bot_api_task(timeout_sec: float = 5.0) -> None:
    global _bot_api_task
    task = _bot_api_task
    _bot_api_task = None
    if not task:
        return
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=timeout_sec)
    except asyncio.CancelledError:
        pass
    except asyncio.TimeoutError:
        logger.warning("Timed out stopping Bot API polling task after %.1fs", timeout_sec)
    except Exception as e:
        logger.warning("Bot API polling task stop failed: %s", e)

def _get_pool_tokens() -> list[str]:
    raw = getattr(settings, "BOT_POOL_TOKENS", "") or ""
    return [t.strip() for t in raw.split(",") if t.strip()]

async def _stop_pool():
    global bot_pool, _bot_cycle
    for idx, bot in enumerate(bot_pool):
        await _safe_stop_client(bot, f"pool_{idx}")
        _forget_bot_handlers(bot)
    bot_pool = []
    _bot_cycle = None

async def reload_bot_pool(tokens: list[str]):
    """Stop existing pool and start a new one with provided tokens."""
    await _stop_pool()
    for idx, token in enumerate(tokens):
        bot = Client(f"morganxmystic_pool_{idx}", api_id=settings.API_ID, api_hash=settings.API_HASH, bot_token=token)
        try:
            await bot.start()
            bot_me = await bot.get_me()
            bot._is_bot = getattr(bot_me, "is_bot", False)
            bot_pool.append(bot)
            try:
                await bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                _clear_bot_webhook_http(token)
            await verify_storage_access_v2(bot)
            _register_bot_handlers(bot)
        except Exception:
            await _safe_stop_client(bot, f"pool_{idx}")
            _forget_bot_handlers(bot)
            continue

async def pool_status() -> list[dict]:
    """Return status for tg_client, bot_client, and pool bots."""
    results = []
    tokens = _get_pool_tokens()

    async def check(label, client: Client | None):
        ok = False
        detail = ""
        if not client:
            results.append({"label": label, "ok": False, "detail": "not configured"})
            return
        try:
            me = await client.get_me()
            await verify_storage_access_v2(client)
            ok = True
            detail = f"@{getattr(me, 'username', '') or me.id}"
        except Exception as e:
            ok = False
            detail = str(e)
        results.append({"label": label, "ok": ok, "detail": detail})

    await check("tg_client", tg_client)
    await check("bot_client", bot_client)
    for idx, bot in enumerate(bot_pool):
        await check(f"pool_{idx}", bot)
    # Show configured tokens that failed to start
    if tokens:
        started = len(bot_pool)
        for idx in range(started, len(tokens)):
            results.append({"label": f"pool_{idx}", "ok": False, "detail": "not started (check token/access)"})
    return results

async def speed_test(sample_bytes: int = 1_000_000) -> dict:
    """Download ~sample_bytes from storage via Pyrogram (works with user or bot)."""
    start_ts = asyncio.get_event_loop().time()
    chat_id = normalize_chat_id(get_storage_chat_id() or "me")

    # Pick a client that can access storage: prefer user_client, then bot_client, then pool
    client: Client | None = user_client or bot_client or (bot_pool[0] if bot_pool else None)
    if not client:
        return {"ok": False, "error": "No Pyrogram client available"}

    try:
        # Ensure access
        if not await ensure_peer_access(client, chat_id):
            return {"ok": False, "error": "Client cannot access storage channel"}

        # Grab last media message
        msg = None
        async for m in client.get_chat_history(chat_id, limit=10):
            if getattr(m, "document", None) or getattr(m, "video", None) or getattr(m, "audio", None):
                msg = m
                break
        if not msg:
            return {"ok": False, "error": "No media found in storage channel"}

        # Stream a slice
        file_id = None
        if msg.document:
            file_id = msg.document.file_id
        elif msg.video:
            file_id = msg.video.file_id
        elif msg.audio:
            file_id = msg.audio.file_id
        if not file_id:
            return {"ok": False, "error": "Media missing file_id"}

        received = 0
        async for chunk in client.stream_media(file_id, offset=0, limit=sample_bytes):
            received += len(chunk)
            if received >= sample_bytes:
                break
    except Exception as e:
        return {"ok": False, "error": str(e)}

    elapsed = asyncio.get_event_loop().time() - start_ts
    mbps = (received / 1024 / 1024) / elapsed if elapsed > 0 else 0
    return {"ok": True, "bytes": received, "seconds": elapsed, "mb_per_s": mbps}

def get_pool_client() -> Client | None:
    global _bot_cycle
    if not bot_pool:
        return None
    if _bot_cycle is None:
        _bot_cycle = cycle(bot_pool)
    for _ in range(len(bot_pool)):
        candidate = next(_bot_cycle)
        if _is_client_connected(candidate):
            return candidate
    return None

def get_storage_client() -> Client:
    """Choose a client that can access the storage channel."""
    pool_client = get_pool_client()
    if pool_client:
        return pool_client
    if _is_client_connected(bot_client):
        return bot_client
    if _is_client_connected(tg_client):
        return tg_client
    return bot_client or tg_client

def normalize_chat_id(chat_id: int | str) -> int | str:
    if isinstance(chat_id, str):
        raw = chat_id.strip()
        if raw == "me":
            return raw
        # Numeric ids should stay numeric for Pyrogram
        if raw.lstrip("-").isdigit():
            try:
                return int(raw)
            except Exception:
                return raw
        if raw.startswith("@") or raw.startswith("https://"):
            return raw
        return f"@{raw}"
    return chat_id


def _cast_ids(raw_ids: list[str]) -> list:
    casted = []
    for value in raw_ids or []:
        try:
            casted.append(PydanticObjectId(str(value)))
        except Exception:
            pass
    return casted or (raw_ids or [])


def _is_video_item(item: FileSystemItem) -> bool:
    name = (item.name or "").lower()
    return ("video" in (item.mime_type or "")) or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi"))


async def _collect_folder_files(folder_id: str) -> list[FileSystemItem]:
    items: list[FileSystemItem] = []
    children = await FileSystemItem.find(FileSystemItem.parent_id == str(folder_id)).to_list()
    for child in children:
        if child.is_folder:
            items.extend(await _collect_folder_files(str(child.id)))
        else:
            items.append(child)
    return items


async def _resolve_shared_items(token: str) -> list[FileSystemItem]:
    folder = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == True)
    if folder:
        items = await _collect_folder_files(str(folder.id))
        return items

    collection = await SharedCollection.find_one(SharedCollection.token == token)
    if collection:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(collection.item_ids))).to_list()
        if any(item.is_folder for item in items):
            expanded: list[FileSystemItem] = []
            for item in items:
                if item.is_folder:
                    expanded.extend(await _collect_folder_files(str(item.id)))
                else:
                    expanded.append(item)
            items = expanded
        # Deduplicate while preserving order
        seen = set()
        unique_items: list[FileSystemItem] = []
        for item in items:
            key = str(item.id)
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(item)
        items = unique_items
        return items

    item = await FileSystemItem.find_one(FileSystemItem.share_token == token, FileSystemItem.is_folder == False)
    if item:
        return [item]

    return []

def get_storage_chat_id() -> int | str:
    global _storage_chat_id_override
    if _storage_chat_id_override:
        return _storage_chat_id_override
    if getattr(settings, "STORAGE_CHANNEL_USERNAME", ""):
        return settings.STORAGE_CHANNEL_USERNAME
    return settings.STORAGE_CHANNEL_ID or "me"

async def ensure_peer_access(client: Client, chat_id: int | str) -> bool:
    """Ensure the client has access to the given chat id."""
    if not _is_client_connected(client):
        return False
    chat_id = normalize_chat_id(chat_id)
    if chat_id == "me":
        return True
    try:
        await client.get_chat(chat_id)
        return True
    except Exception as e:
        logger.error(f"Peer access check failed for {chat_id}: {e}")
        return False

async def verify_storage_access_v2(client: Client):
    """Check storage channel access using Telethon first, then log Pyrogram status if needed."""
    global _storage_access_notified
    try:
        if await tl_check_storage():
            logger.info("Telethon storage check: OK")
            if not _storage_access_notified:
                try:
                    await tl_send_text("MorganXMystic: bot can access the storage channel.")
                    _storage_access_notified = True
                except Exception as notify_err:
                    logger.warning(f"Storage notify failed: {notify_err}")
            return
        logger.error("Telethon storage check failed.")
    except Exception as tele_err:
        logger.error(f"Telethon storage check error: {tele_err}")

    # Optional Pyrogram check (non-fatal)
    try:
        chat_id = normalize_chat_id(get_storage_chat_id())
        if chat_id == "me":
            logger.info("STORAGE_CHANNEL_ID/USERNAME not set. Using Saved Messages (me).")
            return
        chat = await client.get_chat(chat_id)
        logger.info(f"Storage channel reachable (Pyrogram): {getattr(chat, 'title', '') or chat.id}")
    except Exception as e:
        logger.error(f"Pyrogram storage check failed: {e}")

async def _try_join_storage(client: Client, chat_id: int | str) -> bool:
    invite = getattr(settings, "STORAGE_CHANNEL_INVITE", "")
    if not invite:
        return False
    if getattr(client, "_is_bot", False):
        return False
    try:
        await client.join_chat(invite)
    except Exception:
        try:
            await client.get_chat(invite)
        except Exception:
            return False
    return await ensure_peer_access(client, chat_id)

async def resolve_storage_chat_id(client: Client):
    global _storage_chat_id_override
    if _storage_chat_id_override:
        return
    invite = getattr(settings, "STORAGE_CHANNEL_INVITE", "")
    if not invite:
        return
    if getattr(client, "_is_bot", False):
        return
    try:
        chat = await client.join_chat(invite)
    except Exception:
        try:
            chat = await client.get_chat(invite)
        except Exception as e:
            logger.error(f"Storage invite resolve failed: {e}")
            return
    if chat and getattr(chat, "id", None):
        _storage_chat_id_override = chat.id
        logger.info(f"Resolved storage channel id via invite: {_storage_chat_id_override}")

async def ensure_bot_member(user: Client):
    if not bot_client:
        return
    bot_username = getattr(settings, "BOT_USERNAME", "") or ""
    if not bot_username:
        return
    chat_id = normalize_chat_id(get_storage_chat_id())
    if chat_id == "me":
        return
    try:
        member = await user.get_chat_member(chat_id, bot_username)
        if member:
            return
    except Exception:
        pass
    try:
        await user.add_chat_members(chat_id, bot_username)
        logger.info("Added bot to storage channel via user session.")
    except Exception as e:
        logger.error(f"Failed to add bot to storage channel: {e}")

async def pick_storage_client(chat_id: int | str) -> Client:
    candidates = []
    if bot_pool:
        candidates.extend(bot_pool)
    if bot_client:
        candidates.append(bot_client)
    if user_client:
        candidates.append(user_client)
    if tg_client not in candidates:
        candidates.append(tg_client)

    for client in candidates:
        if not _is_client_connected(client):
            continue
        if await ensure_peer_access(client, chat_id):
            return client
        if await _try_join_storage(client, chat_id):
            return client
    # Retry after resolving via invite link (user session only)
    if user_client:
        await resolve_storage_chat_id(user_client)
        new_chat_id = normalize_chat_id(get_storage_chat_id())
        if new_chat_id != chat_id:
            for client in candidates:
                if not _is_client_connected(client):
                    continue
                if await ensure_peer_access(client, new_chat_id):
                    return client
                if await _try_join_storage(client, new_chat_id):
                    return client

    raise Exception("Storage channel not accessible for any client. Check channel membership or invite.")

async def verify_storage_access(client: Client):
    """Check if the client can access and post to the storage channel."""
    chat_id = get_storage_chat_id()
    if chat_id == "me":
        logger.info("STORAGE_CHANNEL_ID/USERNAME not set. Using Saved Messages (me).")
        return
    try:
        chat = await client.get_chat(chat_id)
        logger.info(f"Storage channel reachable: {getattr(chat, 'title', '') or chat.id}")
        try:
            test_msg = await client.send_message(chat_id, "MorganXMystic storage check OK")
            await client.delete_messages(chat_id, test_msg.id)
            logger.info("Storage channel post check: OK")
        except Exception as post_err:
            logger.error(f"Storage channel post check failed: {post_err}")
    except Exception as e:
        logger.error(f"Storage channel access failed: {e}")

async def handle_private_upload(client: Client, message):
    """Forward user files sent to bot into storage channel and create DB items."""
    try:
        logger.info(
            "Bot upload received: chat_id=%s msg_id=%s from_user=%s",
            getattr(message.chat, "id", None),
            getattr(message, "id", None),
            getattr(message.from_user, "id", None) if message.from_user else None
        )
        if not (message.document or message.video or message.audio or message.photo):
            return

        storage_target = get_storage_chat_id()
        storage_chat_id = normalize_chat_id(storage_target)
        if not storage_chat_id or storage_chat_id == "me":
            await client.send_message(
                message.chat.id,
                "Storage channel not configured. Please set STORAGE_CHANNEL_ID/USERNAME and try again."
            )
            return

        # Quick ack so user knows the bot received the file
        try:
            await client.send_message(message.chat.id, "Got it! Uploading to storage...")
        except Exception:
            pass

        if not await ensure_peer_access(client, storage_chat_id):
            logger.warning("Storage channel not reachable by Pyrogram client; will still try Telethon upload.")

        owner = await _linked_user_from_tg_id(
            getattr(message.from_user, "id", None) if message.from_user else None
        )
        if not owner:
            logger.warning("Telegram upload blocked: user is not linked.")
            try:
                await client.send_message(
                    message.chat.id,
                    "Please login on the website first to link your Telegram, then send the file again."
                )
            except Exception:
                pass
            return
        if not _is_admin_user(owner):
            try:
                await client.send_message(
                    message.chat.id,
                    "Only admins can upload files via Telegram bot. Send a content name to search instead."
                )
            except Exception:
                pass
            return
        owner_phone = owner.phone_number

        forwarded = None
        # Prefer a native copy to storage channel (fast, no download)
        try:
            forwarded = await client.copy_message(storage_chat_id, message.chat.id, message.id)
        except Exception as copy_err:
            logger.warning(f"Copy to storage failed, falling back to upload: {copy_err}")
            # Download via Pyrogram and re-upload via Telethon to storage channel
            original_name = None
            if message.document:
                original_name = message.document.file_name
            elif message.video:
                original_name = message.video.file_name
            elif message.audio:
                original_name = message.audio.file_name
            if not original_name:
                original_name = "file"

            fd, tmp_path = tempfile.mkstemp()
            os.close(fd)
            try:
                await client.download_media(message, file_name=tmp_path)
                forwarded = await tl_send_file(
                    tmp_path,
                    file_name=original_name,
                    caption="Uploaded via MorganXMystic"
                )
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        if not forwarded:
            await client.send_message(
                message.chat.id,
                "Upload failed: storage channel is not reachable by the bot."
            )
            return

        if hasattr(forwarded, "document") or hasattr(forwarded, "video") or hasattr(forwarded, "audio") or hasattr(forwarded, "photo"):
            # Pyrogram Message
            if forwarded.document:
                file_id = forwarded.document.file_id
                size = forwarded.document.file_size
                mime_type = forwarded.document.mime_type or "application/octet-stream"
                name = forwarded.document.file_name or "file"
            elif forwarded.video:
                file_id = forwarded.video.file_id
                size = forwarded.video.file_size
                mime_type = forwarded.video.mime_type or "video/mp4"
                name = forwarded.video.file_name or "video"
            elif forwarded.audio:
                file_id = forwarded.audio.file_id
                size = forwarded.audio.file_size
                mime_type = forwarded.audio.mime_type or "audio/mpeg"
                name = forwarded.audio.file_name or "audio"
            elif forwarded.photo:
                file_id = forwarded.photo.file_id
                size = 0
                mime_type = "image/jpeg"
                name = "photo.jpg"
            else:
                return
        else:
            # Telethon Message
            file_id = str(forwarded.id)
            size = getattr(forwarded.file, "size", 0)
            mime_type = getattr(forwarded.file, "mime_type", None) or "application/octet-stream"
            name = getattr(forwarded.file, "name", None) or "file"

        # Ensure Bot Uploads folder exists for this user (rename legacy if needed)
        folder = await FileSystemItem.find_one(
            FileSystemItem.owner_phone == owner_phone,
            FileSystemItem.parent_id == None,
            FileSystemItem.is_folder == True,
            FileSystemItem.name == "Bot Uploads"
        )
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Uploads"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Shared"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            folder = FileSystemItem(
                name="Bot Uploads",
                is_folder=True,
                parent_id=None,
                owner_phone=owner_phone,
                source="bot"
            )
            await folder.insert()

        new_file = FileSystemItem(
            name=name,
            is_folder=False,
            parent_id=str(folder.id),
            owner_phone=owner_phone,
            size=size,
            mime_type=mime_type,
            source="bot",
            parts=[FilePart(
                telegram_file_id=file_id,
                message_id=forwarded.id,
                chat_id=storage_chat_id,
                part_number=1,
                size=size
            )]
        )
        await new_file.insert()
        logger.info(f"Bot-ingested file for {owner_phone}: {name}")
        try:
            await client.send_message(message.chat.id, "File added to Bot Uploads folder.")
        except Exception:
            pass
    except Exception as e:
        logger.exception(f"Bot ingestion failed: {e}")
        try:
            await client.send_message(message.chat.id, f"Upload failed: {e}")
        except Exception:
            pass

async def handle_start_command(client: Client, message):
    """Handle /start command and shared deep links."""
    try:
        if not message.text:
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            linked_user = await _linked_user_from_tg_id(
                getattr(message.from_user, "id", None) if message.from_user else None
            )
            display_name = (
                (getattr(message.from_user, "first_name", "") or "").strip()
                or (getattr(linked_user, "first_name", "") or "").strip()
                or "there"
            )
            await _send_welcome_message(
                client,
                message.chat.id,
                display_name,
                _is_admin_user(linked_user),
            )
            return

        payload = parts[1].strip()
        if payload.startswith("share_"):
            token = payload.replace("share_", "", 1)
            items = await _resolve_shared_items(token)
            if not items:
                await client.send_message(message.chat.id, "File not found or expired.")
                return

            items = [i for i in items if _is_video_item(i)]
            if not items:
                await client.send_message(message.chat.id, "No video files found.")
                return

            unavailable = 0
            sent = 0
            for item in items:
                if not item.parts:
                    continue
                chat_id = (item.parts[0].chat_id if item.parts else None) or get_storage_chat_id() or "me"
                chat_id = normalize_chat_id(chat_id)
                if chat_id == "me":
                    unavailable += 1
                    continue
                msg = await tl_get_message(item.parts[0].message_id)
                await tl_forward_to_user(message.chat.id, msg)
                sent += 1
                await asyncio.sleep(0.35)

            if sent:
                await client.send_message(message.chat.id, f"Sent {sent} file(s).")
            if unavailable:
                await client.send_message(message.chat.id, "Some files were not available from storage.")
            return

        linked_user = await _linked_user_from_tg_id(
            getattr(message.from_user, "id", None) if message.from_user else None
        )
        display_name = (
            (getattr(message.from_user, "first_name", "") or "").strip()
            or (getattr(linked_user, "first_name", "") or "").strip()
            or "there"
        )
        await _send_welcome_message(
            client,
            message.chat.id,
            display_name,
            _is_admin_user(linked_user),
        )
    except Exception as e:
        logger.error(f"Start command failed: {e}")


async def handle_text_query(client: Client, message):
    raw_query = (message.text or "").strip()
    if not raw_query:
        return

    query = raw_query[:120]
    catalog = await _build_published_catalog()
    results = _rank_catalog_matches(query, catalog, limit=5)
    corrected = ""

    if not results:
        await client.send_message(message.chat.id, "Searching on the web for spelling correction...")
        suggestion = await _google_spelling_suggestion(query)
        if suggestion:
            corrected = suggestion
            results = _rank_catalog_matches(suggestion, catalog, limit=5)

    if not results:
        await _send_not_found(client, message.chat.id, query)
        return

    best = results[0]
    await _send_content_result(
        client,
        message.chat.id,
        best,
        corrected_query=corrected if corrected and _norm_text(corrected) != _norm_text(query) else "",
    )


async def handle_bot_message(client: Client, message):
    """Single entrypoint for bot messages to avoid filter mismatches."""
    try:
        if getattr(getattr(message, "chat", None), "type", "") != "private":
            return
        if message.text and message.text.strip().startswith("/start"):
            await handle_start_command(client, message)
            return
        if message.text:
            if message.text.strip().startswith("/"):
                linked_user = await _linked_user_from_tg_id(
                    getattr(message.from_user, "id", None) if message.from_user else None
                )
                display_name = (
                    (getattr(message.from_user, "first_name", "") or "").strip()
                    or (getattr(linked_user, "first_name", "") or "").strip()
                    or "there"
                )
                await _send_welcome_message(
                    client,
                    message.chat.id,
                    display_name,
                    _is_admin_user(linked_user),
                )
                return
            await handle_text_query(client, message)
            return
        if message.document or message.video or message.audio or message.photo:
            await handle_private_upload(client, message)
            return
    except Exception as e:
        logger.exception(f"Bot message handler failed: {e}")
        try:
            await client.send_message(message.chat.id, f"Upload failed: {e}")
        except Exception:
            pass

def _register_bot_handlers(client: Client):
    client_key = _client_key(client)
    if client_key in _bot_handler_clients:
        return
    try:
        # Single handler for all incoming messages (simpler, more reliable)
        client.add_handler(MessageHandler(handle_bot_message, filters.incoming))
        _bot_handler_clients.add(client_key)
        logger.info("Bot handlers registered.")
    except Exception as e:
        logger.error(f"Failed to register bot handlers: {e}")


def _clear_bot_webhook_http(token: str) -> None:
    if not token:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=true"
        with urllib.request.urlopen(url, timeout=10) as resp:
            _ = resp.read()
        logger.info("Cleared bot webhook via HTTP.")
    except Exception as e:
        logger.warning(f"Failed to clear bot webhook via HTTP: {e}")


async def _bot_api_call(token: str, method: str, params: dict | None = None) -> dict:
    params = params or {}
    def _do():
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/{method}",
            data=data
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload)
    return await asyncio.to_thread(_do)


async def _handle_bot_api_message(message: dict):
    try:
        chat = message.get("chat") or {}
        if (chat.get("type") or "") != "private":
            return

        chat_id = chat.get("id")
        msg_id = message.get("message_id")
        text = (message.get("text") or "").strip()
        from_user = message.get("from") or {}
        linked_user = await _linked_user_from_tg_id(from_user.get("id"))
        display_name = (
            (from_user.get("first_name") or "").strip()
            or (getattr(linked_user, "first_name", "") or "").strip()
            or "there"
        )

        if text.startswith("/start"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                payload = parts[1].strip()
                if payload.startswith("share_"):
                    token = payload.replace("share_", "", 1)
                    items = await _resolve_shared_items(token)
                    if not items:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "File not found or expired."}
                        )
                        return

                    items = [i for i in items if _is_video_item(i)]
                    if not items:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "No video files found."}
                        )
                        return

                    sent = 0
                    unavailable = 0
                    for item in items:
                        if not item.parts:
                            continue
                        part = item.parts[0]
                        source_chat = part.chat_id or normalize_chat_id(get_storage_chat_id())
                        if not source_chat or source_chat == "me":
                            unavailable += 1
                            continue
                        copy_resp = await _bot_api_call(
                            settings.BOT_TOKEN,
                            "copyMessage",
                            {
                                "chat_id": chat_id,
                                "from_chat_id": source_chat,
                                "message_id": part.message_id
                            }
                        )
                        if not copy_resp.get("ok"):
                            await _bot_api_call(
                                settings.BOT_TOKEN,
                                "sendMessage",
                                {"chat_id": chat_id, "text": f"Failed to send file: {copy_resp.get('description', 'unknown error')}"}
                            )
                            continue
                        sent += 1
                        await asyncio.sleep(0.35)

                    if sent:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": f"Sent {sent} file(s)."}
                        )
                    if unavailable:
                        await _bot_api_call(
                            settings.BOT_TOKEN,
                            "sendMessage",
                            {"chat_id": chat_id, "text": "Some files were not available from storage."}
                        )
                    return

            await _send_welcome_message_api(chat_id, display_name, _is_admin_user(linked_user))
            return

        if text:
            if text.startswith("/"):
                await _send_welcome_message_api(chat_id, display_name, _is_admin_user(linked_user))
                return

            query = text[:120]
            catalog = await _build_published_catalog()
            results = _rank_catalog_matches(query, catalog, limit=5)
            corrected = ""

            if not results:
                await _bot_api_call(
                    settings.BOT_TOKEN,
                    "sendMessage",
                    {"chat_id": chat_id, "text": "Searching on the web for spelling correction..."}
                )
                suggestion = await _google_spelling_suggestion(query)
                if suggestion:
                    corrected = suggestion
                    results = _rank_catalog_matches(suggestion, catalog, limit=5)

            if not results:
                await _send_not_found_api(chat_id, query)
                return

            best = results[0]
            await _send_content_result_api(
                chat_id,
                best,
                corrected_query=corrected if corrected and _norm_text(corrected) != _norm_text(query) else "",
            )
            return

        media = None
        media_type = None
        if message.get("document"):
            media = message["document"]
            media_type = "document"
        elif message.get("video"):
            media = message["video"]
            media_type = "video"
        elif message.get("audio"):
            media = message["audio"]
            media_type = "audio"
        elif message.get("photo"):
            media = message["photo"][-1]
            media_type = "photo"
        else:
            return

        if not linked_user:
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Please login on the website first so I can link your Telegram account."}
            )
            return

        if not _is_admin_user(linked_user):
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Only admins can upload files via Telegram bot. Send a content name to search instead."}
            )
            return

        storage_chat_id = normalize_chat_id(get_storage_chat_id())
        if not storage_chat_id or storage_chat_id == "me":
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": "Storage channel not configured. Please set STORAGE_CHANNEL_ID/USERNAME."}
            )
            return

        await _bot_api_call(
            settings.BOT_TOKEN,
            "sendMessage",
            {"chat_id": chat_id, "text": "Got it! Uploading to storage..."}
        )

        copy_resp = await _bot_api_call(
            settings.BOT_TOKEN,
            "copyMessage",
            {"chat_id": storage_chat_id, "from_chat_id": chat_id, "message_id": msg_id}
        )
        if not copy_resp.get("ok"):
            await _bot_api_call(
                settings.BOT_TOKEN,
                "sendMessage",
                {"chat_id": chat_id, "text": f"Upload failed: {copy_resp.get('description', 'unknown error')}"}
            )
            return

        forwarded_msg_id = copy_resp["result"]["message_id"]
        owner_phone = linked_user.phone_number

        folder = await FileSystemItem.find_one(
            FileSystemItem.owner_phone == owner_phone,
            FileSystemItem.parent_id == None,
            FileSystemItem.is_folder == True,
            FileSystemItem.name == "Bot Uploads"
        )
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Uploads"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            legacy = await FileSystemItem.find_one(
                FileSystemItem.owner_phone == owner_phone,
                FileSystemItem.parent_id == None,
                FileSystemItem.is_folder == True,
                FileSystemItem.name == "Telegram Shared"
            )
            if legacy:
                legacy.name = "Bot Uploads"
                await legacy.save()
                folder = legacy
        if not folder:
            folder = FileSystemItem(
                name="Bot Uploads",
                is_folder=True,
                parent_id=None,
                owner_phone=owner_phone,
                source="bot"
            )
            await folder.insert()

        file_id = media.get("file_id", "")
        file_name = media.get("file_name") or ("photo.jpg" if media_type == "photo" else "file")
        file_size = media.get("file_size", 0) or 0
        mime_type = media.get("mime_type") or ("image/jpeg" if media_type == "photo" else "application/octet-stream")

        new_file = FileSystemItem(
            name=file_name,
            is_folder=False,
            parent_id=str(folder.id),
            owner_phone=owner_phone,
            size=file_size,
            mime_type=mime_type,
            source="bot",
            parts=[FilePart(
                telegram_file_id=file_id or str(forwarded_msg_id),
                message_id=forwarded_msg_id,
                chat_id=storage_chat_id,
                part_number=1,
                size=file_size
            )]
        )
        await new_file.insert()

        await _bot_api_call(
            settings.BOT_TOKEN,
            "sendMessage",
            {"chat_id": chat_id, "text": "File added to Bot Uploads folder."}
        )
    except Exception as e:
        logger.exception(f"Bot API handler failed: {e}")
        try:
            chat_id = (message.get("chat") or {}).get("id")
            if chat_id:
                await _bot_api_call(
                    settings.BOT_TOKEN,
                    "sendMessage",
                    {"chat_id": chat_id, "text": f"Upload failed: {e}"}
                )
        except Exception:
            pass

async def _bot_api_poll_loop():
    if not settings.BOT_TOKEN:
        return
    offset = 0
    while True:
        try:
            resp = await _bot_api_call(
                settings.BOT_TOKEN,
                "getUpdates",
                {"timeout": 25, "offset": offset}
            )
            if not resp.get("ok"):
                await asyncio.sleep(2)
                continue
            for upd in resp.get("result", []):
                offset = upd.get("update_id", offset) + 1
                msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post")
                if msg:
                    await _handle_bot_api_message(msg)
        except Exception as e:
            logger.warning(f"Bot API polling error: {e}")
            await asyncio.sleep(2)


async def start_telegram():
    global bot_client, _bot_api_task, _telegram_started
    async with _telegram_lifecycle_lock:
        if _telegram_started:
            return

        logger.info("Connecting to Telegram...")
        started_clients: list[tuple[str, Client]] = []

        try:
            if not _is_client_connected(tg_client):
                await tg_client.start()
                started_clients.append(("tg_client", tg_client))
            me = await tg_client.get_me()
            tg_client._is_bot = getattr(me, "is_bot", False)
            logger.info(f"Connected as {me.first_name} (@{me.username})")
            if getattr(tg_client, "_is_bot", False):
                try:
                    await tg_client.delete_webhook(drop_pending_updates=True)
                    logger.info("Cleared bot webhook for long polling (tg_client).")
                except Exception as e:
                    logger.warning(f"Failed to clear bot webhook (tg_client): {e}")
                    _clear_bot_webhook_http(settings.BOT_TOKEN)
            await resolve_storage_chat_id(tg_client)
            if getattr(tg_client, "_is_bot", False):
                _register_bot_handlers(tg_client)
            if user_client and tg_client is user_client:
                await ensure_bot_member(user_client)
            await verify_storage_access_v2(tg_client)

            if bot_client and bot_client is not tg_client:
                try:
                    if not _is_client_connected(bot_client):
                        await bot_client.start()
                        started_clients.append(("bot_client", bot_client))
                    bot_me = await bot_client.get_me()
                    bot_client._is_bot = getattr(bot_me, "is_bot", False)
                    logger.info(f"Bot client connected as {bot_me.first_name} (@{bot_me.username})")
                    try:
                        await bot_client.delete_webhook(drop_pending_updates=True)
                        logger.info("Cleared bot webhook for long polling (bot_client).")
                    except Exception as e:
                        logger.warning(f"Failed to clear bot webhook (bot_client): {e}")
                        _clear_bot_webhook_http(settings.BOT_TOKEN)
                    await verify_storage_access_v2(bot_client)
                    _register_bot_handlers(bot_client)
                except Exception as e:
                    logger.warning(f"Bot client start failed: {e}")
                    failed_bot = bot_client
                    if failed_bot:
                        await _safe_stop_client(failed_bot, "bot_client")
                        _forget_bot_handlers(failed_bot)
                        started_clients = [entry for entry in started_clients if entry[1] is not failed_bot]
                    bot_client = None

            # Start bot pool (if any)
            await _stop_pool()
            tokens = _get_pool_tokens()
            for idx, token in enumerate(tokens):
                bot = Client(
                    f"morganxmystic_pool_{idx}",
                    api_id=settings.API_ID,
                    api_hash=settings.API_HASH,
                    bot_token=token
                )
                try:
                    await bot.start()
                    bot_me = await bot.get_me()
                    bot._is_bot = getattr(bot_me, "is_bot", False)
                    bot_pool.append(bot)
                    logger.info(f"Started bot pool #{idx}")
                    try:
                        await bot.delete_webhook(drop_pending_updates=True)
                        logger.info(f"Cleared bot webhook for pool #{idx}.")
                    except Exception as e:
                        logger.warning(f"Failed to clear webhook for pool #{idx}: {e}")
                        _clear_bot_webhook_http(token)
                    await verify_storage_access_v2(bot)
                    _register_bot_handlers(bot)
                except Exception as e:
                    logger.error(f"Failed to start bot pool #{idx}: {e}")
                    await _safe_stop_client(bot, f"pool_{idx}")
                    _forget_bot_handlers(bot)

            # Start Bot API polling fallback
            if settings.BOT_TOKEN and (_bot_api_task is None or _bot_api_task.done()):
                _bot_api_task = asyncio.create_task(_bot_api_poll_loop(), name="bot_api_poll_loop")
                logger.info("Bot API polling started")

            _telegram_started = True
        except Exception:
            logger.exception("Telegram startup failed.")
            await _stop_bot_api_task()
            await _stop_pool()
            for label, client in reversed(started_clients):
                await _safe_stop_client(client, label)
                _forget_bot_handlers(client)
            _telegram_started = False
            raise

async def stop_telegram():
    global _telegram_started
    logger.info("Stopping Telegram Client...")
    async with _telegram_lifecycle_lock:
        await _stop_bot_api_task()
        await _stop_pool()
        if bot_client and bot_client is not tg_client:
            await _safe_stop_client(bot_client, "bot_client")
            _forget_bot_handlers(bot_client)
        await _safe_stop_client(tg_client, "tg_client")
        _forget_bot_handlers(tg_client)
        _bot_handler_clients.clear()
        _telegram_started = False


