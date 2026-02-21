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
        return _norm_bot(segs[0])
    if text.startswith("@"):
        return text.lower()
    return _norm_bot(text)


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
        val = line.strip()
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


def _sender_matches_filter(msg, sender_label: str, filters: list[str]) -> bool:
    if not filters:
        return _msg_sender_is_bot(msg)
    if sender_label in filters:
        return True
    sender_low = sender_label.lower().lstrip("@")
    for fil in filters:
        check = str(fil or "").lower().lstrip("@")
        if check and (check == sender_low or check in sender_low):
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


def _message_line_for_url(text: str, url: str) -> str:
    lines = [x.strip() for x in str(text or "").splitlines() if x.strip()]
    for line in lines:
        if url in line:
            return line
    return lines[0] if lines else ""


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
                action = {
                    "row": r_idx,
                    "col": c_idx,
                    "button_text": btxt,
                    "url": burl,
                    "callback_data": bcb,
                }
                if _is_pager_button(btxt):
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

    urls = []
    urls.extend(_TME_URL_RE.findall(body_text))
    urls.extend(_extract_entity_urls(msg, body_text))
    urls = _dedupe_keep_order([x for x in urls if x])
    for url in urls:
        line = _message_line_for_url(body_text, url)
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
            if not _sender_matches_filter(msg, sender, source_bots_filter):
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
    if "t.me" not in (parsed.netloc or ""):
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
    for _ in range(7):
        await asyncio.sleep(1.5)
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
            except Exception:
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
):
    user = await get_current_user(request)
    if not _is_admin(user):
        return JSONResponse({"ok": False, "error": "Not allowed."}, status_code=403)

    q = " ".join(str(query or "").split()).strip()
    if len(q) < 2:
        return JSONResponse({"ok": False, "error": "Enter a content name."}, status_code=400)

    cfg = await _ensure_settings()
    source_chat = _norm_chat_ref(cfg.source_chat_id)
    if source_chat is None:
        return JSONResponse({"ok": False, "error": "Configure Source Query Group/Chat ID first."}, status_code=400)
    source_bots = _dedupe_keep_order([_norm_bot(x) for x in (cfg.source_bots or []) if _norm_bot(x)])

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    logs: list[str] = []
    try:
        sent = await client.send_message(source_chat, q)
        since_ts = max(0.0, _msg_ts(sent) - 2.0)
        logs.append(f"Sent query to {source_chat}: {q}")
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Failed to send query to source chat: {e}"}, status_code=500)

    await asyncio.sleep(2.8)
    items, pagers = await _collect_from_group(
        client,
        source_chat=source_chat,
        query=q,
        since_ts=since_ts,
        source_bots_filter=source_bots,
        logs=logs,
    )
    if not items:
        await asyncio.sleep(2.2)
        items, pagers = await _collect_from_group(
            client,
            source_chat=source_chat,
            query=q,
            since_ts=since_ts,
            source_bots_filter=source_bots,
            logs=logs,
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

    try:
        await client.send_message(destination_bot, "/start")
    except Exception:
        pass

    for item in chosen:
        action_type = str(item.get("action_type") or "")
        source_bot = _norm_bot(item.get("source_bot") or "")
        chat_id = item.get("chat_id")
        message_id = int(item.get("message_id") or 0)
        action = item.get("action") if isinstance(item.get("action"), dict) else {}
        logs.append(f"Fetching: {item.get('title') or 'Untitled'} [{source_bot}]")

        if action_type == "direct_media":
            forwarded_total += await _forward_message(client, destination_bot, chat_id, message_id, logs)
            continue

        start_ts = _now_ts() - 1.5
        watch_chats: list[int | str] = []
        if source_chat not in (None, ""):
            watch_chats.append(source_chat)
        if source_bot:
            watch_chats.append(source_bot)

        clicked = False
        if action_type == "button_callback":
            clicked = await _click_message_button(client, chat_id, message_id, action, logs)
        elif action_type in {"button_url", "line_url"}:
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
                        else:
                            await client.send_message(target, "/start")
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
