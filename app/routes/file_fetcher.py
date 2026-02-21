import asyncio
import hashlib
import json
import re
import time
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, Body, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.core.telegram_bot import start_telegram, user_client
from app.db.models import FileFetcherSettings, User
from app.routes.admin import _admin_context_base, _is_admin
from app.routes.dashboard import get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_SEARCH_CACHE: dict[str, dict[str, Any]] = {}
_SEARCH_CACHE_TTL_SEC = 15 * 60

_SIZE_RE = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>TB|GB|MB|KB)\b", re.I)
_TME_URL_RE = re.compile(r"https?://t\.me/[^\s)>\]]+", re.I)
_CHANNEL_MENTION_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_]{4,})")
_FORCE_SUB_RE = re.compile(r"(join|subscribe|updates?\s+channel|force\s*sub|important)", re.I)
_BOT_USERNAME_RE = re.compile(r"^@[A-Za-z0-9_]{4,}$")
_NUMBERED_LINE_RE = re.compile(r"^\s*(\d+)\s*[\.\)]\s*(.+?)\s*$")
_NOISY_BUTTON_RE = re.compile(
    r"(remove\s*ads?|send[\s_]*all|quality|language|season|page|pages|filters?)",
    re.I,
)
_NOISY_TEXT_RE = re.compile(
    r"(title\s*:|total\s*files|result\s*in|requested\s*by|powered\s*by|your\s*requested\s*files|join\s*updates\s*channel|important)",
    re.I,
)
_MEDIA_EXT_RE = re.compile(r"\.(mkv|mp4|avi|m4v|webm|ts|mov)\b", re.I)
_QUALITY_HINT_RE = re.compile(r"\b(4k|2k|2160p?|1440p?|1080p?|720p?|540p?|480p?|360p?|240p?|144p?)\b", re.I)


def _now_ts() -> float:
    return time.time()


def _client_connected(client) -> bool:
    try:
        return bool(client and getattr(client, "is_connected", False))
    except Exception:
        return False


def _norm_bot(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("https://t.me/") or text.startswith("http://t.me/"):
        parsed = urlparse(text)
        segs = [x for x in parsed.path.split("/") if x]
        if not segs:
            return ""
        text = segs[0]
    if text.startswith("@"):
        return text.lower()
    return f"@{text.lower()}"


def _norm_channel(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("https://t.me/") or text.startswith("http://t.me/"):
        return text
    if text.startswith("t.me/"):
        return f"https://{text}"
    if text.startswith("@"):
        return text.lower()
    return f"@{text.lower()}"


def _norm_chat_ref(raw: str) -> int | str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    # Keep first token only if user pasted extra text.
    text = re.split(r"[\s,]+", text, maxsplit=1)[0].strip()
    if not text:
        return None
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except Exception:
            return None
    if text.startswith("https://t.me/") or text.startswith("http://t.me/"):
        parsed = urlparse(text)
        segs = [x for x in parsed.path.split("/") if x]
        if not segs:
            return None
        if segs[0].startswith("+") or segs[0] == "joinchat":
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return _norm_bot(segs[0])
    if text.startswith("t.me/+") or text.startswith("t.me/joinchat/"):
        return f"https://{text}"
    if text.startswith("@"):
        return text.lower()
    return _norm_bot(text)


def _chat_id_variants(chat_ref: int | str) -> list[int | str]:
    if not isinstance(chat_ref, int):
        return [chat_ref]
    out: list[int] = [int(chat_ref)]
    s_abs = str(abs(int(chat_ref)))
    # Convert supergroup/full form <-> short negative form when needed.
    if s_abs.startswith("100") and len(s_abs) > 3:
        short_form = -int(s_abs[3:])
        if short_form not in out:
            out.append(short_form)
    else:
        full_form = -int(f"100{s_abs}")
        if full_form not in out:
            out.append(full_form)
    return out


def _is_valid_bot_username(raw: str) -> bool:
    return bool(_BOT_USERNAME_RE.fullmatch(str(raw or "").strip()))


def _dedupe_keep_order(rows: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        val = str(row or "").strip()
        key = val.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(val)
    return out


def _dedupe_chat_refs(rows: list[int | str]) -> list[int | str]:
    seen: set[str] = set()
    out: list[int | str] = []
    for row in rows:
        if row in ("", None):
            continue
        key = f"{type(row).__name__}:{row}"
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _parse_multiline_refs(raw: str, *, channel: bool = False) -> list[str]:
    parts: list[str] = []
    for line in str(raw or "").replace(",", "\n").splitlines():
        for token in line.split():
            val = token.strip()
            if not val:
                continue
            parts.append(_norm_channel(val) if channel else _norm_bot(val))
    return _dedupe_keep_order([x for x in parts if x])


def _size_to_bytes(num: float, unit: str) -> int:
    unit_u = (unit or "").upper()
    mul = {"KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}.get(unit_u, 1)
    return int(float(num) * mul)


def _extract_size(text: str) -> tuple[int, str]:
    raw = str(text or "")
    m = _SIZE_RE.search(raw)
    if not m:
        return 0, ""
    num = float(m.group("num"))
    unit = m.group("unit").upper()
    return _size_to_bytes(num, unit), f"{num:.2f} {unit}"


def _format_size(size: int) -> str:
    s = int(size or 0)
    if s <= 0:
        return ""
    if s >= 1024**3:
        return f"{(s / (1024**3)):.2f} GB"
    if s >= 1024**2:
        return f"{(s / (1024**2)):.2f} MB"
    if s >= 1024:
        return f"{(s / 1024):.2f} KB"
    return f"{s} B"


def _extract_channels(text: str) -> list[str]:
    out: list[str] = []
    raw = str(text or "")
    for m in _CHANNEL_MENTION_RE.finditer(raw):
        out.append(_norm_channel(f"@{m.group(1)}"))
    for url in _TME_URL_RE.findall(raw):
        parsed = urlparse(url)
        path = parsed.path or ""
        if "/+" in path or "/joinchat/" in path:
            out.append(_norm_channel(url))
            continue
        segs = [x for x in path.split("/") if x]
        if segs:
            out.append(_norm_channel(f"@{segs[0]}"))
    return _dedupe_keep_order([x for x in out if x])


def _msg_ts(msg) -> float:
    dt = getattr(msg, "date", None)
    if not dt:
        return _now_ts()
    try:
        return float(dt.timestamp())
    except Exception:
        return _now_ts()


def _has_media(msg) -> bool:
    return bool(getattr(msg, "document", None) or getattr(msg, "video", None) or getattr(msg, "audio", None))


def _media_name(msg) -> str:
    doc = getattr(msg, "document", None)
    if doc and getattr(doc, "file_name", None):
        return str(doc.file_name or "")
    vid = getattr(msg, "video", None)
    if vid and getattr(vid, "file_name", None):
        return str(vid.file_name or "")
    aud = getattr(msg, "audio", None)
    if aud and getattr(aud, "file_name", None):
        return str(aud.file_name or "")
    return ""


def _media_size(msg) -> int:
    for attr in ("document", "video", "audio"):
        media = getattr(msg, attr, None)
        size = int(getattr(media, "file_size", 0) or 0) if media else 0
        if size > 0:
            return size
    return 0


def _best_title(text: str, fallback: str = "") -> str:
    raw = " ".join(str(text or "").split()).strip()
    if not raw:
        return fallback
    line = raw.split("\n")[0].strip()
    line = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", line)
    line = re.sub(r"^\s*[-\u2022]\s*", "", line).strip()
    if len(line) > 220:
        line = line[:220].rstrip()
    return line or fallback


def _candidate_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _msg_sender_label(msg) -> str:
    user = getattr(msg, "from_user", None)
    if user:
        username = str(getattr(user, "username", "") or "").strip()
        if username:
            return _norm_bot(username)
        first_name = str(getattr(user, "first_name", "") or "").strip()
        if first_name:
            return first_name
    sender_chat = getattr(msg, "sender_chat", None)
    if sender_chat:
        username = str(getattr(sender_chat, "username", "") or "").strip()
        if username:
            return _norm_bot(username)
        title = str(getattr(sender_chat, "title", "") or "").strip()
        if title:
            return title
    return "unknown"


def _msg_sender_is_bot(msg) -> bool:
    user = getattr(msg, "from_user", None)
    if user and bool(getattr(user, "is_bot", False)):
        return True
    label = _msg_sender_label(msg).lower()
    return label.endswith("_bot") or label.endswith("bot")


def _canon_sender(value: str) -> str:
    raw = str(value or "").lower().strip()
    raw = raw.lstrip("@")
    raw = re.sub(r"[^a-z0-9]+", "", raw)
    if raw.endswith("bot"):
        raw = raw[:-3]
    return raw


def _sender_matches_filter(msg, sender_label: str, filters: list[str]) -> bool:
    if not filters:
        return _msg_sender_is_bot(msg)
    if sender_label in filters:
        return True
    sender_low = sender_label.lower().lstrip("@")
    sender_canon = _canon_sender(sender_label)
    for fil in filters:
        check = str(fil or "").lower().lstrip("@")
        check_canon = _canon_sender(fil)
        if check and (check == sender_low or check in sender_low):
            return True
        if check_canon and (check_canon == sender_canon or check_canon in sender_canon or sender_canon in check_canon):
            return True
    return False


def _extract_entity_urls(msg, text: str) -> list[str]:
    urls: list[str] = []
    entities = []
    for name in ("entities", "caption_entities"):
        rows = getattr(msg, name, None)
        if isinstance(rows, list):
            entities.extend(rows)
    for ent in entities:
        text_url = str(getattr(ent, "url", "") or "").strip()
        if text_url:
            urls.append(text_url)
            continue
        etype = str(getattr(ent, "type", "")).lower()
        if "url" not in etype:
            continue
        try:
            offset = int(getattr(ent, "offset", 0) or 0)
            length = int(getattr(ent, "length", 0) or 0)
            frag = text[offset: offset + length].strip()
            if frag.startswith("http://") or frag.startswith("https://") or frag.startswith("t.me/"):
                if frag.startswith("t.me/"):
                    frag = f"https://{frag}"
                urls.append(frag)
        except Exception:
            continue
    return _dedupe_keep_order(urls)


def _extract_entity_url_rows(msg, text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    entities = []
    for name in ("entities", "caption_entities"):
        vals = getattr(msg, name, None)
        if isinstance(vals, list):
            entities.extend(vals)
    for ent in entities:
        try:
            offset = int(getattr(ent, "offset", 0) or 0)
            length = int(getattr(ent, "length", 0) or 0)
        except Exception:
            offset = 0
            length = 0
        frag = ""
        if offset >= 0 and length > 0:
            try:
                frag = str(text[offset: offset + length] or "").strip()
            except Exception:
                frag = ""
        url = str(getattr(ent, "url", "") or "").strip()
        if not url:
            etype = str(getattr(ent, "type", "")).lower()
            if "url" in etype and frag:
                if frag.startswith("t.me/"):
                    url = f"https://{frag}"
                elif frag.startswith("http://") or frag.startswith("https://"):
                    url = frag
        if not url:
            continue
        rows.append({"url": url, "frag": frag, "offset": str(offset)})

    # stable order by offset when available
    rows.sort(key=lambda x: int(x.get("offset") or "0"))
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": url, "frag": str(row.get("frag") or "").strip()})
    return out


def _extract_button_urls(msg) -> list[str]:
    urls: list[str] = []
    reply_markup = getattr(msg, "reply_markup", None)
    inline = getattr(reply_markup, "inline_keyboard", None) if reply_markup else None
    if not inline:
        return urls
    for row in inline:
        for btn in row or []:
            burl = str(getattr(btn, "url", "") or "").strip()
            if burl:
                urls.append(burl)
    return _dedupe_keep_order(urls)


def _is_pager_button(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    if raw in {">", ">>", "<", "<<", "next", "prev", "previous", "back"}:
        return True
    if re.search(r"\b(next|prev|previous)\b", raw):
        return True
    if re.fullmatch(r"\d+\s*/\s*\d+", raw):
        return True
    if "page" in raw and ("/" in raw or "next" in raw):
        return True
    return False


def _is_next_button(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    if raw in {"next", "next >", "next >", ">", ">>", "⏭", "➡", "➡️", "next ⏩"}:
        return True
    if "next" in raw:
        return True
    if raw.endswith(">") or raw.endswith(">>"):
        return True
    return False


def _is_noisy_button(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    if _is_next_button(raw):
        return False
    return bool(_NOISY_BUTTON_RE.search(raw))


def _is_file_start_payload(start_payload: str) -> bool:
    raw = str(start_payload or "").strip().lower()
    if not raw:
        return False
    return raw.startswith("file_") or raw.startswith("file-") or raw.startswith("file")


def _looks_like_file_text(text: str) -> bool:
    raw = " ".join(str(text or "").split()).strip()
    if not raw:
        return False
    if _NOISY_TEXT_RE.search(raw):
        return False
    if _SIZE_RE.search(raw):
        return True
    if _MEDIA_EXT_RE.search(raw):
        return True
    if _QUALITY_HINT_RE.search(raw):
        return True
    if re.search(r"\bS\d{1,2}\s*E\d{1,3}\b", raw, re.I):
        return True
    if re.search(r"\bSeason\s*\d+\b", raw, re.I) and re.search(r"\bEpisode\s*\d+\b", raw, re.I):
        return True
    if _NUMBERED_LINE_RE.match(raw):
        return True
    return False


def _extract_numbered_file_lines(text: str) -> list[str]:
    out: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        m = _NUMBERED_LINE_RE.match(line)
        if not m:
            continue
        body = str(m.group(2) or "").strip()
        if _looks_like_file_text(body):
            out.append(body)
    return out


def _clean_title_without_url(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = _TME_URL_RE.sub("", text)
    text = text.strip()
    text = re.sub(r"^[\(\[]\s*", "", text)
    text = re.sub(r"\s*[\)\]]$", "", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


def _extract_numbered_entries_with_urls(text: str) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    lines = [str(x or "") for x in str(text or "").splitlines()]
    i = 0
    while i < len(lines):
        raw_line = " ".join(lines[i].split()).strip()
        i += 1
        if not raw_line:
            continue
        m = _NUMBERED_LINE_RE.match(raw_line)
        if not m:
            continue
        body = str(m.group(2) or "").strip()
        url = ""
        um = _TME_URL_RE.search(body)
        if um:
            url = str(um.group(0) or "").strip()
        else:
            # URL often comes on immediate next line in parentheses.
            j = i
            while j < len(lines):
                nxt = " ".join(lines[j].split()).strip()
                if not nxt:
                    j += 1
                    continue
                um2 = _TME_URL_RE.search(nxt)
                if um2:
                    url = str(um2.group(0) or "").strip()
                    i = j + 1
                break
        title = _clean_title_without_url(body)
        title = _best_title(title, fallback="")
        if not title:
            continue
        if not _looks_like_file_text(title):
            continue
        entries.append(
            {
                "n": str(m.group(1) or "").strip(),
                "title": title,
                "url": url,
            }
        )
    return entries


def _message_line_for_url(text: str, url: str) -> str:
    lines = [x.strip() for x in str(text or "").splitlines() if x.strip()]
    for idx, line in enumerate(lines):
        if url in line:
            cleaned = line.strip("()[] ")
            if cleaned == url and idx > 0:
                prev = lines[idx - 1].strip()
                if prev:
                    return prev
            return line
    return lines[0] if lines else ""


def _extract_size_lines(text: str) -> list[tuple[str, int, str]]:
    rows: list[tuple[str, int, str]] = []
    for raw_line in str(text or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        b, l = _extract_size(line)
        if b > 0:
            rows.append((line, b, l))
    return rows


def _dedupe_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    out.sort(key=lambda x: (str(x.get("source_bot", "")), int(x.get("size_bytes", 0) or 0), str(x.get("title", ""))), reverse=True)
    return out


def _dedupe_pagers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    out.sort(key=lambda x: (str(x.get("source_bot", "")), str(x.get("button_text", ""))))
    return out


def _user_cache_key(user: User) -> str:
    return f"user:{(user.phone_number or '').strip()}"


def _put_cache(
    user_key: str,
    *,
    items: list[dict[str, Any]],
    pagers: list[dict[str, Any]],
    query: str,
    source_chat: int | str,
) -> None:
    _SEARCH_CACHE[user_key] = {
        "ts": _now_ts(),
        "items": items,
        "pagers": pagers,
        "query": query,
        "source_chat": source_chat,
    }
    cutoff = _now_ts() - _SEARCH_CACHE_TTL_SEC
    stale = [k for k, v in _SEARCH_CACHE.items() if float(v.get("ts", 0)) < cutoff]
    for key in stale:
        _SEARCH_CACHE.pop(key, None)


def _get_cache(user_key: str) -> dict[str, Any]:
    row = _SEARCH_CACHE.get(user_key) or {}
    ts = float(row.get("ts", 0))
    if (_now_ts() - ts) > _SEARCH_CACHE_TTL_SEC:
        _SEARCH_CACHE.pop(user_key, None)
        return {}
    return row


async def _ensure_settings() -> FileFetcherSettings:
    row = await FileFetcherSettings.find_one(FileFetcherSettings.key == "main")
    if row:
        return row
    row = FileFetcherSettings(
        key="main",
        source_chat_id="",
        source_bots=[],
        destination_bot="mysticmovies_bot",
        force_sub_channels=[],
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    await row.insert()
    return row


async def _ensure_user_session_client() -> tuple[Any, str]:
    try:
        await start_telegram()
    except Exception as e:
        return None, f"Telegram startup failed: {e}"
    client = user_client
    if not client:
        return None, "User session is not configured (SESSION_STRING missing)."
    if not _client_connected(client):
        try:
            await client.start()
        except Exception as e:
            return None, f"User session is not connected: {e}"
    return client, ""


async def _prepare_source_chat(client, source_chat: int | str, logs: list[str]) -> int | str:
    if not isinstance(source_chat, str):
        return source_chat
    raw = source_chat.strip()
    if not raw:
        return source_chat
    if not ("t.me/+" in raw or "t.me/joinchat/" in raw):
        return source_chat
    try:
        joined = await client.join_chat(raw)
        joined_id = getattr(joined, "id", None)
        if joined_id is not None:
            logs.append(f"Joined source group via invite link -> {joined_id}")
            return int(joined_id)
        logs.append("Joined source group via invite link.")
        return source_chat
    except Exception as e:
        text = str(e or "")
        logs.append(f"Invite-link join failed: {text}")
        # If already joined, try resolving chat id via invite metadata.
        try:
            lower = text.lower()
            if "already participant" in lower or "already_member" in lower or "already joined" in lower:
                invite = await client.check_chat_invite(raw)
                inv_chat = getattr(invite, "chat", None)
                inv_id = getattr(inv_chat, "id", None)
                if inv_id is not None:
                    logs.append(f"Resolved source group id via invite -> {inv_id}")
                    return int(inv_id)
        except Exception:
            pass
        return source_chat


async def _warm_peer_cache_for_chat(client, source_chat: int, logs: list[str], limit: int = 240) -> int | None:
    target_variants = {int(x) for x in _chat_id_variants(source_chat) if isinstance(x, int)}
    try:
        async for dlg in client.get_dialogs(limit=limit):
            chat = getattr(dlg, "chat", None)
            if not chat:
                continue
            cid = int(getattr(chat, "id", 0) or 0)
            if not cid:
                continue
            if cid in target_variants:
                title = str(getattr(chat, "title", "") or getattr(chat, "first_name", "") or "").strip()
                logs.append(f"Resolved source chat from dialogs: {cid}" + (f" ({title})" if title else ""))
                return cid
    except Exception as e:
        logs.append(f"Dialog peer warmup failed: {e}")
    logs.append("Source chat id not found in user-session dialogs.")
    return None


def _extract_from_message(msg, source_label: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    items: list[dict[str, Any]] = []
    pagers: list[dict[str, Any]] = []

    chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
    message_id = int(getattr(msg, "id", 0) or 0)
    body_text = str(getattr(msg, "text", "") or getattr(msg, "caption", "") or "")

    media_name = _media_name(msg)
    media_text = media_name or body_text
    media_size_bytes, media_size_label = _extract_size(media_text)
    if not media_size_bytes and _has_media(msg):
        media_size_bytes = _media_size(msg)
        media_size_label = _format_size(media_size_bytes)

    if _has_media(msg):
        payload = {
            "source_bot": source_label,
            "chat_id": chat_id,
            "message_id": message_id,
            "action_type": "direct_media",
            "action": {},
            "title": _best_title(media_name or body_text, fallback=f"File from {source_label}"),
            "size_bytes": int(media_size_bytes or 0),
            "size_label": media_size_label,
        }
        payload["id"] = _candidate_id(payload)
        items.append(payload)

    reply_markup = getattr(msg, "reply_markup", None)
    inline = getattr(reply_markup, "inline_keyboard", None) if reply_markup else None
    if inline:
        for r_idx, row in enumerate(inline):
            for c_idx, btn in enumerate(row or []):
                btxt = str(getattr(btn, "text", "") or "").strip()
                burl = str(getattr(btn, "url", "") or "").strip()
                bcb = str(getattr(btn, "callback_data", "") or "").strip()
                if not btxt and not burl and not bcb:
                    continue
                if _is_noisy_button(btxt):
                    continue
                action = {
                    "row": r_idx,
                    "col": c_idx,
                    "button_text": btxt,
                    "url": burl,
                    "callback_data": bcb,
                }
                if _is_pager_button(btxt):
                    # keep only true "next" controls as pager actions
                    if not _is_next_button(btxt):
                        continue
                    pager_payload = {
                        "source_bot": source_label,
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "action_type": "pager_callback" if bcb else "pager_url",
                        "action": action,
                        "button_text": btxt,
                        "scope": f"{chat_id}:{message_id}:{source_label}",
                    }
                    pager_payload["id"] = _candidate_id(pager_payload)
                    pagers.append(pager_payload)
                    continue
                size_bytes, size_label = _extract_size(f"{btxt} {body_text}")
                action_type = "button_url" if burl else "button_callback"
                if action_type == "button_url":
                    parsed = _parse_tme_action(burl)
                    kind = str(parsed.get("kind") or "")
                    if kind == "join":
                        continue
                    if kind == "bot_start" and not _is_file_start_payload(str(parsed.get("start") or "")):
                        # skip non-file starts (ads/menu/etc.)
                        continue
                if not _looks_like_file_text(btxt) and action_type != "button_url":
                    continue
                payload = {
                    "source_bot": source_label,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "action_type": action_type,
                    "action": action,
                    "title": _best_title(btxt or body_text, fallback=f"Button from {source_label}"),
                    "size_bytes": int(size_bytes or 0),
                    "size_label": size_label,
                }
                payload["id"] = _candidate_id(payload)
                items.append(payload)

    numbered_entries = _extract_numbered_entries_with_urls(body_text)
    numbered_lines = [str(x.get("title") or "").strip() for x in numbered_entries if str(x.get("title") or "").strip()]
    numbered_by_url: dict[str, str] = {}
    for row in numbered_entries:
        u = str(row.get("url") or "").strip()
        t = str(row.get("title") or "").strip()
        if not u or not t:
            continue
        numbered_by_url[u.lower()] = t
    numbered_idx = 0
    url_rows: list[dict[str, str]] = []
    for row in _extract_entity_url_rows(msg, body_text):
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        url_rows.append({"url": url, "frag": str(row.get("frag") or "").strip()})
    # add plain urls missing from entity map
    seen_url_keys = {str(x.get("url") or "").lower() for x in url_rows}
    for raw_url in _TME_URL_RE.findall(body_text):
        key = str(raw_url or "").lower()
        if key and key not in seen_url_keys:
            seen_url_keys.add(key)
            url_rows.append({"url": raw_url, "frag": ""})

    for row in url_rows:
        url = str(row.get("url") or "").strip()
        frag = str(row.get("frag") or "").strip()
        parsed = _parse_tme_action(url)
        kind = str(parsed.get("kind") or "")
        line_for_url = frag or _message_line_for_url(body_text, url)
        line_for_url = _best_title(line_for_url, fallback="")

        # Some bots provide page controls as deep links in message text.
        if line_for_url and _is_pager_button(line_for_url):
            pager_payload = {
                "source_bot": source_label,
                "chat_id": chat_id,
                "message_id": message_id,
                "action_type": "pager_url",
                "action": {"url": url},
                "button_text": line_for_url,
                "scope": f"{chat_id}:{message_id}:{source_label}",
            }
            pager_payload["id"] = _candidate_id(pager_payload)
            pagers.append(pager_payload)
            continue

        if kind == "join":
            continue
        if kind == "bot_start" and not _is_file_start_payload(str(parsed.get("start") or "")):
            continue

        line = ""
        by_url_title = numbered_by_url.get(url.lower(), "")
        if by_url_title:
            line = by_url_title
        elif line_for_url and _looks_like_file_text(line_for_url) and line_for_url.lower() != url.lower():
            line = line_for_url
        elif numbered_idx < len(numbered_lines):
            line = numbered_lines[numbered_idx]
            numbered_idx += 1
        else:
            # only use entity fragment when line lookup failed and fragment clearly looks like full file row
            frag_clean = _clean_title_without_url(frag)
            if frag_clean and _looks_like_file_text(frag_clean) and len(frag_clean) >= 20:
                line = frag_clean
            else:
                line = _message_line_for_url(body_text, url)
        line = _best_title(line, fallback="")
        if not _looks_like_file_text(line):
            continue
        size_bytes, size_label = _extract_size(line or body_text)
        payload = {
            "source_bot": source_label,
            "chat_id": chat_id,
            "message_id": message_id,
            "action_type": "line_url",
            "action": {"url": url},
            "title": _best_title(line or body_text, fallback=f"Link from {source_label}"),
            "size_bytes": int(size_bytes or 0),
            "size_label": size_label,
        }
        payload["id"] = _candidate_id(payload)
        items.append(payload)

    # Only expose actionable rows (file links/buttons/direct media). Plain text-only rows are hidden.
    return items, pagers


async def _collect_from_group(
    client,
    *,
    source_chat: int | str,
    query: str,
    since_ts: float,
    source_bots_filter: list[str],
    logs: list[str],
    limit: int = 260,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    items: list[dict[str, Any]] = []
    pagers: list[dict[str, Any]] = []
    try:
        async for msg in client.get_chat_history(source_chat, limit=limit):
            ts = _msg_ts(msg)
            if ts < since_ts:
                break
            if getattr(msg, "outgoing", False):
                continue
            sender = _msg_sender_label(msg)
            allowed = _sender_matches_filter(msg, sender, source_bots_filter)
            if not allowed:
                # fallback: keep likely bot-generated result messages even if sender label format differs
                has_markup = bool(getattr(getattr(msg, "reply_markup", None), "inline_keyboard", None))
                text = str(getattr(msg, "text", "") or getattr(msg, "caption", "") or "")
                has_links = bool(_TME_URL_RE.search(text))
                if not (_msg_sender_is_bot(msg) or has_markup or has_links or _has_media(msg)):
                    continue
            msg_items, msg_pagers = _extract_from_message(msg, sender)
            items.extend(msg_items)
            pagers.extend(msg_pagers)
    except Exception as e:
        logs.append(f"Read group failed: {e}")

    qlow = str(query or "").strip().lower()
    if qlow:
        items.sort(
            key=lambda x: (
                1 if qlow in str(x.get("title", "")).lower() else 0,
                int(x.get("size_bytes", 0) or 0),
            ),
            reverse=True,
        )
    return _dedupe_candidates(items), _dedupe_pagers(pagers)


async def _join_channels_and_save(
    client,
    cfg: FileFetcherSettings,
    channels: list[str],
    logs: list[str],
) -> list[str]:
    joined: list[str] = []
    if not channels:
        return joined
    existing = set([_norm_channel(x) for x in (cfg.force_sub_channels or []) if _norm_channel(x)])
    changed = False
    for raw in channels:
        channel = _norm_channel(raw)
        if not channel:
            continue
        try:
            await client.join_chat(channel)
            logs.append(f"Joined channel: {channel}")
            joined.append(channel)
        except Exception as e:
            text = str(e or "").lower()
            if "already" in text or "participant" in text:
                logs.append(f"Already joined: {channel}")
                joined.append(channel)
            else:
                logs.append(f"Join failed {channel}: {e}")
        if channel not in existing:
            existing.add(channel)
            changed = True
    if changed:
        cfg.force_sub_channels = sorted(existing)
        cfg.updated_at = datetime.now()
        await cfg.save()
    return joined


def _parse_tme_action(url: str) -> dict[str, Any]:
    raw = str(url or "").strip()
    if not raw:
        return {"kind": "none"}
    if raw.startswith("tg://"):
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query or "")
        domain = (qs.get("domain", [""])[0] or "").strip()
        start = (qs.get("start", [""])[0] or "").strip()
        if domain:
            return {"kind": "bot_start", "target": _norm_bot(domain), "start": start}
        return {"kind": "none"}
    if raw.startswith("t.me/"):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    netloc = str(parsed.netloc or "").lower()
    if ("t.me" not in netloc) and ("telegram.me" not in netloc):
        return {"kind": "none"}
    path = (parsed.path or "").strip("/")
    if not path:
        return {"kind": "none"}
    if path.startswith("+") or path.startswith("joinchat/"):
        return {"kind": "join", "target": raw}
    segs = [x for x in path.split("/") if x]
    if not segs:
        return {"kind": "none"}
    username = segs[0]
    if len(segs) >= 2 and re.fullmatch(r"\d+", segs[1]):
        return {"kind": "post", "target": _norm_bot(username), "message_id": int(segs[1])}
    start = (parse_qs(parsed.query or "").get("start", [""])[0] or "").strip()
    return {"kind": "bot_start", "target": _norm_bot(username), "start": start}


async def _forward_message(client, destination_bot: str, from_chat: int | str, message_id: int, logs: list[str]) -> int:
    try:
        await client.forward_messages(
            chat_id=destination_bot,
            from_chat_id=from_chat,
            message_ids=message_id,
        )
        logs.append(f"Forwarded message {message_id} -> {destination_bot}")
        return 1
    except Exception as e:
        logs.append(f"Forward failed (msg {message_id}): {e}")
        return 0


async def _collect_new_media_and_force_sub(
    client,
    destination_bot: str,
    watch_chats: list[int | str],
    since_ts: float,
    cfg: FileFetcherSettings,
    logs: list[str],
    seen_keys: set[str],
) -> tuple[int, list[str]]:
    forwarded = 0
    joined_total: list[str] = []
    chats = [x for x in watch_chats if x not in ("", None)]
    logged_chat_errors: set[str] = set()
    for _ in range(10):
        await asyncio.sleep(1.2)
        for chat in chats:
            try:
                async for msg in client.get_chat_history(chat, limit=35):
                    mts = _msg_ts(msg)
                    if mts < since_ts:
                        break
                    key = f"{getattr(getattr(msg, 'chat', None), 'id', '')}:{getattr(msg, 'id', '')}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    text = str(getattr(msg, "text", "") or getattr(msg, "caption", "") or "")
                    if _FORCE_SUB_RE.search(text):
                        channels = _extract_channels(text)
                        if channels:
                            joined = await _join_channels_and_save(client, cfg, channels, logs)
                            joined_total.extend(joined)
                    if _has_media(msg):
                        msg_chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
                        msg_id = int(getattr(msg, "id", 0) or 0)
                        if msg_chat_id and msg_id:
                            forwarded += await _forward_message(client, destination_bot, msg_chat_id, msg_id, logs)
            except Exception as e:
                key = str(chat)
                if key not in logged_chat_errors:
                    logged_chat_errors.add(key)
                    logs.append(f"Read chat failed [{chat}]: {e}")
                continue
    return forwarded, _dedupe_keep_order(joined_total)


async def _click_message_button(client, chat_id: int | str, message_id: int, action: dict[str, Any], logs: list[str]) -> bool:
    try:
        msg = await client.get_messages(chat_id=chat_id, message_ids=message_id)
        row = int(action.get("row", 0) or 0)
        col = int(action.get("col", 0) or 0)
        try:
            await msg.click(x=row, y=col)
            return True
        except Exception:
            btn_text = str(action.get("button_text", "") or "").strip()
            if btn_text:
                await msg.click(btn_text)
                return True
            raise
    except Exception as e:
        logs.append(f"Button click failed: {e}")
        return False


async def _resolve_start_actions_from_recent(
    client,
    *,
    chats: list[int | str],
    since_ts: float,
    cfg: FileFetcherSettings,
    logs: list[str],
) -> tuple[list[str], list[str]]:
    start_actions: list[tuple[str, str]] = []
    joined_total: list[str] = []
    for chat in _dedupe_chat_refs(chats):
        try:
            async for msg in client.get_chat_history(chat, limit=35):
                if _msg_ts(msg) < since_ts:
                    break
                text = str(getattr(msg, "text", "") or getattr(msg, "caption", "") or "")
                urls: list[str] = []
                urls.extend(_TME_URL_RE.findall(text))
                urls.extend(_extract_entity_urls(msg, text))
                urls.extend(_extract_button_urls(msg))
                urls = _dedupe_keep_order([x for x in urls if x])
                for url in urls:
                    parsed = _parse_tme_action(url)
                    kind = str(parsed.get("kind") or "")
                    if kind == "join":
                        joined = await _join_channels_and_save(client, cfg, [str(parsed.get("target") or "")], logs)
                        joined_total.extend(joined)
                        continue
                    if kind == "bot_start":
                        target = str(parsed.get("target") or "").strip()
                        start = str(parsed.get("start") or "").strip()
                        if _is_valid_bot_username(target):
                            start_actions.append((target, start))
        except Exception:
            continue

    started_targets: list[str] = []
    seen: set[str] = set()
    for target, start in start_actions:
        key = f"{target}|{start}"
        if key in seen:
            continue
        seen.add(key)
        try:
            if start:
                await client.send_message(target, f"/start {start}")
                logs.append(f"Sent /start payload to {target}")
            else:
                await client.send_message(target, "/start")
                logs.append(f"Sent /start to {target}")
            started_targets.append(target)
        except Exception as e:
            logs.append(f"/start send failed for {target}: {e}")

    return _dedupe_keep_order(started_targets), _dedupe_keep_order(joined_total)


@router.get("/file-fetcher")
async def file_fetcher_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/admin-login")
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not allowed")

    cfg = await _ensure_settings()
    base_ctx = await _admin_context_base(user)
    return templates.TemplateResponse(
        "file_fetcher.html",
        {
            **base_ctx,
            "user": user,
            "request": request,
            "cfg": cfg,
        },
    )


@router.post("/file-fetcher/settings")
async def file_fetcher_save_settings(
    request: Request,
    source_chat_id: str = Form(""),
    source_bots_text: str = Form(""),
    destination_bot: str = Form(""),
    force_sub_channels_text: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not allowed")

    chat_ref = _norm_chat_ref(source_chat_id)
    if chat_ref is None:
        return JSONResponse({"ok": False, "error": "Enter a valid Source Query Group/Chat ID."}, status_code=400)

    cfg = await _ensure_settings()
    source_bots = _parse_multiline_refs(source_bots_text, channel=False)
    destination = _norm_bot(destination_bot or cfg.destination_bot or "mysticmovies_bot").lstrip("@")
    force_channels = _parse_multiline_refs(force_sub_channels_text, channel=True)

    cfg.source_chat_id = str(chat_ref)
    cfg.source_bots = source_bots
    cfg.destination_bot = destination
    cfg.force_sub_channels = force_channels
    cfg.updated_at = datetime.now()
    await cfg.save()
    return JSONResponse({"ok": True, "message": "Settings saved."})


@router.post("/file-fetcher/search")
async def file_fetcher_search(
    request: Request,
    query: str = Form(""),
    source_chat_id: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        return JSONResponse({"ok": False, "error": "Not allowed."}, status_code=403)

    q = " ".join(str(query or "").split()).strip()
    if len(q) < 2:
        return JSONResponse({"ok": False, "error": "Enter a content name."}, status_code=400)

    cfg = await _ensure_settings()
    # Search uses explicit runtime source chat (from UI) first to avoid stale saved settings.
    runtime_source_raw = str(source_chat_id or "").strip()
    source_chat = _norm_chat_ref(runtime_source_raw) if runtime_source_raw else _norm_chat_ref(cfg.source_chat_id)
    if source_chat is None:
        return JSONResponse({"ok": False, "error": "Configure Source Query Group/Chat ID first."}, status_code=400)
    if isinstance(source_chat, str):
        sref = source_chat.strip().lower()
        if sref.startswith("@") and sref.endswith("bot"):
            return JSONResponse(
                {
                    "ok": False,
                    "error": "Source Query Group/Chat ID points to a bot. Set a group/chat id (e.g. -100...) or group username.",
                },
                status_code=400,
            )
    # Persist runtime override so next actions/page calls keep same target.
    if runtime_source_raw:
        cfg.source_chat_id = str(source_chat)
        cfg.updated_at = datetime.now()
        await cfg.save()
    source_bots = _dedupe_keep_order([_norm_bot(x) for x in (cfg.source_bots or []) if _norm_bot(x)])

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    logs: list[str] = []
    source_chat = await _prepare_source_chat(client, source_chat, logs)
    logs.append(f"Using source chat: {source_chat}")
    group_send_ok = False
    sent = None
    since_ts = _now_ts() - 90.0
    send_candidates = _chat_id_variants(source_chat)
    send_errors: list[str] = []

    async def _attempt_send(target: int | str) -> bool:
        nonlocal sent, since_ts, source_chat
        try:
            sent = await client.send_message(target, q)
            since_ts = max(0.0, _msg_ts(sent) - 2.0)
            source_chat = target
            logs.append(f"Sent query to {target}: {q}")
            return True
        except Exception as ex:
            send_errors.append(str(ex))
            logs.append(f"Send failed [{target}]: {ex}")
            return False

    for cand in send_candidates:
        if await _attempt_send(cand):
            group_send_ok = True
            break

    if (not group_send_ok) and isinstance(source_chat, int):
        if any("peer id invalid" in x.lower() for x in send_errors):
            warmed = await _warm_peer_cache_for_chat(client, source_chat, logs)
            retry_candidates = _chat_id_variants(int(warmed)) if isinstance(warmed, int) else send_candidates
            for cand in retry_candidates:
                if await _attempt_send(cand):
                    group_send_ok = True
                    break

    items: list[dict[str, Any]] = []
    pagers: list[dict[str, Any]] = []
    if group_send_ok:
        target_bot_count = max(1, len(source_bots))
        for attempt in range(1, 6):
            await asyncio.sleep(1.5)
            scan_items, scan_pagers = await _collect_from_group(
                client,
                source_chat=source_chat,
                query=q,
                since_ts=since_ts,
                source_bots_filter=source_bots,
                logs=logs,
                limit=180,
            )
            items = _dedupe_candidates([*items, *scan_items])
            pagers = _dedupe_pagers([*pagers, *scan_pagers])
            bot_seen = len(set([str(x.get("source_bot") or "") for x in items if str(x.get("source_bot") or "")]))
            logs.append(f"Scan {attempt}/5 -> files={len(items)} bots={bot_seen} pagers={len(pagers)}")
            if len(items) >= 1 and bot_seen >= target_bot_count:
                break
    else:
        err_hint = ""
        err_low = " ".join(logs).lower()
        if "peer id invalid" in err_low:
            err_hint = (
                " Telegram user-session cannot access this source group id. "
                "Use @groupusername or invite link in Source Query Group/Chat ID, "
                "and ensure SESSION_STRING account is a member."
            )
        return JSONResponse(
            {
                "ok": False,
                "error": "Failed to send query in source group. Check Source Query Group/Chat ID and user-session access." + err_hint,
                "logs": logs,
            },
            status_code=500,
        )

    _put_cache(
        _user_cache_key(user),
        items=items,
        pagers=pagers,
        query=q,
        source_chat=source_chat,
    )
    logs.append(f"Results: {len(items)} file choices | {len(pagers)} page controls")
    return JSONResponse({"ok": True, "items": items, "pagers": pagers, "logs": logs})


@router.post("/file-fetcher/page")
async def file_fetcher_page_next(
    request: Request,
    payload: dict = Body(default={}),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        return JSONResponse({"ok": False, "error": "Not allowed."}, status_code=403)

    pager_id = str((payload or {}).get("pager_id") or "").strip()
    if not pager_id:
        return JSONResponse({"ok": False, "error": "pager_id is required."}, status_code=400)

    cache = _get_cache(_user_cache_key(user))
    if not cache:
        return JSONResponse({"ok": False, "error": "Search cache expired. Search again."}, status_code=400)
    pagers = cache.get("pagers") if isinstance(cache.get("pagers"), list) else []
    pager_map = {str(x.get("id") or ""): x for x in pagers}
    pager = pager_map.get(pager_id)
    if not pager:
        return JSONResponse({"ok": False, "error": "Pager not found. Search again."}, status_code=400)

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    logs: list[str] = []
    clicked = False
    action_type = str(pager.get("action_type") or "")
    action = pager.get("action") if isinstance(pager.get("action"), dict) else {}
    chat_id = pager.get("chat_id")
    message_id = int(pager.get("message_id") or 0)
    source_bot = str(pager.get("source_bot") or "")
    source_chat = cache.get("source_chat")
    query = str(cache.get("query") or "")

    if action_type == "pager_callback":
        clicked = await _click_message_button(client, chat_id, message_id, action, logs)
        if clicked:
            logs.append(f"Clicked page button on {source_bot}: {action.get('button_text') or ''}")
    elif action_type == "pager_url":
        raw_url = str(action.get("url") or "").strip()
        parsed = _parse_tme_action(raw_url)
        if parsed.get("kind") == "bot_start" and parsed.get("target"):
            try:
                target = parsed.get("target")
                start = str(parsed.get("start") or "").strip()
                if start:
                    await client.send_message(target, f"/start {start}")
                else:
                    await client.send_message(target, "/start")
                clicked = True
                logs.append(f"Opened page link: {target}")
            except Exception as e:
                logs.append(f"Open page link failed: {e}")

    if not clicked:
        return JSONResponse({"ok": False, "error": "Failed to open next page.", "logs": logs}, status_code=400)

    await asyncio.sleep(2.3)
    new_items: list[dict[str, Any]] = []
    new_pagers: list[dict[str, Any]] = []
    try:
        updated_msg = await client.get_messages(chat_id=chat_id, message_ids=message_id)
        msg_items, msg_pagers = _extract_from_message(updated_msg, source_bot or _msg_sender_label(updated_msg))
        new_items.extend(msg_items)
        new_pagers.extend(msg_pagers)
    except Exception as e:
        logs.append(f"Failed reading updated page message: {e}")

    extra_items, extra_pagers = await _collect_from_group(
        client,
        source_chat=source_chat,
        query=query,
        since_ts=_now_ts() - 12.0,
        source_bots_filter=[source_bot] if source_bot else [],
        logs=logs,
        limit=80,
    )
    new_items.extend(extra_items)
    new_pagers.extend(extra_pagers)

    cached_items = cache.get("items") if isinstance(cache.get("items"), list) else []
    cached_pagers = cache.get("pagers") if isinstance(cache.get("pagers"), list) else []
    scope = str(pager.get("scope") or "")
    cached_pagers = [x for x in cached_pagers if str(x.get("scope") or "") != scope]
    merged_items = _dedupe_candidates([*cached_items, *new_items])
    merged_pagers = _dedupe_pagers([*cached_pagers, *new_pagers])

    _put_cache(
        _user_cache_key(user),
        items=merged_items,
        pagers=merged_pagers,
        query=query,
        source_chat=source_chat,
    )
    logs.append(f"Page updated: +{len(_dedupe_candidates(new_items))} new file choices")
    return JSONResponse({"ok": True, "items": merged_items, "pagers": merged_pagers, "logs": logs})


@router.post("/file-fetcher/page-all")
async def file_fetcher_page_next_all(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        return JSONResponse({"ok": False, "error": "Not allowed."}, status_code=403)

    cache = _get_cache(_user_cache_key(user))
    if not cache:
        return JSONResponse({"ok": False, "error": "Search cache expired. Search again."}, status_code=400)

    pagers = cache.get("pagers") if isinstance(cache.get("pagers"), list) else []
    if not pagers:
        return JSONResponse({"ok": False, "error": "No page controls available."}, status_code=400)

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    logs: list[str] = []
    source_chat = cache.get("source_chat")
    query = str(cache.get("query") or "")
    cfg = await _ensure_settings()
    source_bots_filter = _dedupe_keep_order([_norm_bot(x) for x in (cfg.source_bots or []) if _norm_bot(x)])

    # choose one pager per source bot:
    # prefer explicit "Next", else fallback to pager_url that looks like next/page start payload.
    next_pagers: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for p in pagers:
        bot = str(p.get("source_bot") or "").strip() or "unknown"
        grouped.setdefault(bot, []).append(p)

    for bot, rows in grouped.items():
        picked: dict[str, Any] | None = None
        for p in rows:
            if _is_next_button(str(p.get("button_text") or "")):
                picked = p
                break
        if not picked:
            for p in rows:
                if str(p.get("action_type") or "") != "pager_url":
                    continue
                action = p.get("action") if isinstance(p.get("action"), dict) else {}
                raw_url = str(action.get("url") or "").strip()
                parsed = _parse_tme_action(raw_url)
                if str(parsed.get("kind") or "") != "bot_start":
                    continue
                start = str(parsed.get("start") or "").strip().lower()
                if ("next" in start) or ("page" in start):
                    picked = p
                    break
        if not picked and rows:
            picked = rows[0]
        if picked:
            next_pagers.append(picked)

    clicked_count = 0
    for pager in next_pagers:
        action_type = str(pager.get("action_type") or "")
        action = pager.get("action") if isinstance(pager.get("action"), dict) else {}
        chat_id = pager.get("chat_id")
        message_id = int(pager.get("message_id") or 0)
        source_bot = str(pager.get("source_bot") or "")
        button_text = str(pager.get("button_text") or "").strip()

        clicked = False
        if action_type == "pager_callback":
            clicked = await _click_message_button(client, chat_id, message_id, action, logs)
        elif action_type == "pager_url":
            raw_url = str(action.get("url") or "").strip()
            parsed = _parse_tme_action(raw_url)
            if parsed.get("kind") == "bot_start" and parsed.get("target"):
                try:
                    target = parsed.get("target")
                    start = str(parsed.get("start") or "").strip()
                    if start:
                        await client.send_message(target, f"/start {start}")
                    else:
                        await client.send_message(target, "/start")
                    clicked = True
                except Exception as e:
                    logs.append(f"Next link open failed for {target}: {e}")
        if clicked:
            clicked_count += 1
            logs.append(f"Next clicked: {source_bot} [{button_text}]")
        else:
            logs.append(f"Next skipped/failed: {source_bot} [{button_text}]")

    if clicked_count <= 0:
        return JSONResponse({"ok": False, "error": "Failed to click any Next button.", "logs": logs}, status_code=400)

    await asyncio.sleep(2.4)
    scan_items, scan_pagers = await _collect_from_group(
        client,
        source_chat=source_chat,
        query=query,
        since_ts=_now_ts() - 14.0,
        source_bots_filter=source_bots_filter,
        logs=logs,
        limit=120,
    )
    cached_items = cache.get("items") if isinstance(cache.get("items"), list) else []
    merged_items = _dedupe_candidates([*cached_items, *scan_items])
    merged_pagers = _dedupe_pagers(scan_pagers)
    _put_cache(
        _user_cache_key(user),
        items=merged_items,
        pagers=merged_pagers,
        query=query,
        source_chat=source_chat,
    )
    logs.append(f"Next-All update: +{len(scan_items)} file rows")
    return JSONResponse({"ok": True, "items": merged_items, "pagers": merged_pagers, "logs": logs})


@router.post("/file-fetcher/fetch")
async def file_fetcher_fetch(
    request: Request,
    payload: dict = Body(default={}),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        return JSONResponse({"ok": False, "error": "Not allowed."}, status_code=403)

    raw_ids = payload.get("ids", [])
    selected_ids = [str(x).strip() for x in (raw_ids if isinstance(raw_ids, list) else []) if str(x).strip()]
    if not selected_ids:
        return JSONResponse({"ok": False, "error": "Select at least one item."}, status_code=400)

    cfg = await _ensure_settings()
    destination_bot = _norm_bot(cfg.destination_bot or "mysticmovies_bot")
    if not destination_bot:
        destination_bot = "@mysticmovies_bot"

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    cache = _get_cache(_user_cache_key(user))
    if not cache:
        return JSONResponse({"ok": False, "error": "Search cache expired. Search again."}, status_code=400)
    cached_items = cache.get("items") if isinstance(cache.get("items"), list) else []
    item_map = {str(x.get("id") or ""): x for x in cached_items}
    chosen = [item_map[x] for x in selected_ids if x in item_map]
    if not chosen:
        return JSONResponse({"ok": False, "error": "Selected rows are not available now. Search again."}, status_code=400)

    logs: list[str] = []
    forwarded_total = 0
    joined_total: list[str] = []
    seen_keys: set[str] = set()
    source_chat = cache.get("source_chat")

    # do not send /start to destination bot on each fetch; forward directly

    for item in chosen:
        action_type = str(item.get("action_type") or "")
        source_bot_label = str(item.get("source_bot") or "")
        source_bot = _norm_bot(source_bot_label)
        chat_id = item.get("chat_id")
        message_id = int(item.get("message_id") or 0)
        action = item.get("action") if isinstance(item.get("action"), dict) else {}
        logs.append(f"Fetching: {item.get('title') or 'Untitled'} [{source_bot_label or source_bot}]")

        if action_type == "direct_media":
            forwarded_total += await _forward_message(client, destination_bot, chat_id, message_id, logs)
            continue
        if action_type == "text_result":
            logs.append("This row is a text-only result without direct link/button. Use linked/button rows to fetch.")
            continue

        start_ts = _now_ts() - 1.5
        watch_chats: list[int | str] = []
        if source_chat not in (None, ""):
            watch_chats.append(source_chat)
        if chat_id not in (None, ""):
            watch_chats.append(chat_id)
        if source_bot and _is_valid_bot_username(source_bot):
            watch_chats.append(source_bot)

        clicked = False
        if action_type == "button_callback":
            clicked = await _click_message_button(client, chat_id, message_id, action, logs)
        elif action_type in {"button_url", "line_url"}:
            if action_type == "button_url":
                # Prefer real button click first (for bots that rely on callback/url interaction state).
                _ = await _click_message_button(client, chat_id, message_id, action, logs)
            raw_url = str(action.get("url") or "").strip()
            parsed = _parse_tme_action(raw_url)
            kind = str(parsed.get("kind") or "")
            if kind == "join":
                joined = await _join_channels_and_save(client, cfg, [str(parsed.get("target") or "")], logs)
                joined_total.extend(joined)
                clicked = bool(joined)
            elif kind == "bot_start":
                target = str(parsed.get("target") or "").strip()
                start_payload = str(parsed.get("start") or "").strip()
                if target:
                    try:
                        if start_payload:
                            await client.send_message(target, f"/start {start_payload}")
                            logs.append(f"Sent /start payload to {target}")
                        else:
                            await client.send_message(target, "/start")
                            logs.append(f"Sent /start to {target}")
                        watch_chats.append(target)
                        clicked = True
                    except Exception as e:
                        logs.append(f"Open link failed for {target}: {e}")
            elif kind == "post":
                target = parsed.get("target")
                post_message_id = int(parsed.get("message_id") or 0)
                if target and post_message_id:
                    forwarded_total += await _forward_message(client, destination_bot, target, post_message_id, logs)
                    clicked = True

        if not clicked:
            logs.append("Action not clickable for this row.")

        started_targets, joined_from_links = await _resolve_start_actions_from_recent(
            client,
            chats=watch_chats,
            since_ts=start_ts,
            cfg=cfg,
            logs=logs,
        )
        for t in started_targets:
            watch_chats.append(t)
        joined_total.extend(joined_from_links)

        forwarded_now, joined_now = await _collect_new_media_and_force_sub(
            client=client,
            destination_bot=destination_bot,
            watch_chats=_dedupe_chat_refs(watch_chats),
            since_ts=start_ts,
            cfg=cfg,
            logs=logs,
            seen_keys=seen_keys,
        )
        forwarded_total += int(forwarded_now or 0)
        joined_total.extend(joined_now)

        if int(forwarded_now or 0) == 0 and joined_now and action_type == "button_callback":
            logs.append("Retrying after join...")
            retry_ts = _now_ts() - 1.0
            retry_click = await _click_message_button(client, chat_id, message_id, action, logs)
            if retry_click:
                retry_started, retry_joined_from_links = await _resolve_start_actions_from_recent(
                    client,
                    chats=watch_chats,
                    since_ts=retry_ts,
                    cfg=cfg,
                    logs=logs,
                )
                for t in retry_started:
                    watch_chats.append(t)
                joined_total.extend(retry_joined_from_links)
                retry_forwarded, retry_joined = await _collect_new_media_and_force_sub(
                    client=client,
                    destination_bot=destination_bot,
                    watch_chats=_dedupe_chat_refs(watch_chats),
                    since_ts=retry_ts,
                    cfg=cfg,
                    logs=logs,
                    seen_keys=seen_keys,
                )
                forwarded_total += int(retry_forwarded or 0)
                joined_total.extend(retry_joined)

    joined_total = _dedupe_keep_order(joined_total)
    return JSONResponse(
        {
            "ok": True,
            "forwarded_count": int(forwarded_total or 0),
            "joined_channels": joined_total,
            "logs": logs,
        }
    )
