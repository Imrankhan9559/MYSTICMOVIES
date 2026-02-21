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

from app.db.models import FileFetcherSettings, User
from app.routes.admin import _admin_context_base, _is_admin
from app.routes.dashboard import get_current_user
from app.core.telegram_bot import start_telegram, user_client

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


def _dedupe_keep_order(rows: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        key = row.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row.strip())
    return out


def _parse_multiline_refs(raw: str, *, channel: bool = False) -> list[str]:
    parts = []
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
    return bool(
        getattr(msg, "document", None)
        or getattr(msg, "video", None)
        or getattr(msg, "audio", None)
    )


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


def _best_title(text: str, fallback: str = "") -> str:
    raw = " ".join(str(text or "").split()).strip()
    if not raw:
        return fallback
    line = raw.split("\n")[0].strip()
    line = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", line)
    line = re.sub(r"^\s*[-•]\s*", "", line).strip()
    if len(line) > 220:
        line = line[:220].rstrip()
    return line or fallback


def _candidate_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


async def _ensure_settings() -> FileFetcherSettings:
    row = await FileFetcherSettings.find_one(FileFetcherSettings.key == "main")
    if not row:
        row = FileFetcherSettings(
            key="main",
            source_bots=[],
            destination_bot="mysticmovies_bot",
            force_sub_channels=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        await row.insert()
    return row


def _user_cache_key(user: User) -> str:
    return f"user:{(user.phone_number or '').strip()}"


def _put_cache(user_key: str, items: list[dict[str, Any]]) -> None:
    _SEARCH_CACHE[user_key] = {"ts": _now_ts(), "items": items}
    # cheap cleanup
    cutoff = _now_ts() - _SEARCH_CACHE_TTL_SEC
    stale = [k for k, v in _SEARCH_CACHE.items() if float(v.get("ts", 0)) < cutoff]
    for key in stale:
        _SEARCH_CACHE.pop(key, None)


def _get_cache(user_key: str) -> list[dict[str, Any]]:
    row = _SEARCH_CACHE.get(user_key) or {}
    ts = float(row.get("ts", 0))
    if (_now_ts() - ts) > _SEARCH_CACHE_TTL_SEC:
        _SEARCH_CACHE.pop(user_key, None)
        return []
    items = row.get("items")
    return items if isinstance(items, list) else []


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


async def _collect_candidates_for_bot(
    client,
    bot: str,
    query: str,
    since_ts: float,
    logs: list[str],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    try:
        async for msg in client.get_chat_history(bot, limit=45):
            ts = _msg_ts(msg)
            if ts < since_ts:
                break
            if getattr(msg, "outgoing", False):
                continue

            chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
            message_id = int(getattr(msg, "id", 0) or 0)
            body_text = str(getattr(msg, "text", "") or getattr(msg, "caption", "") or "")
            media_name = _media_name(msg)
            item_text = media_name or body_text
            size_bytes, size_label = _extract_size(item_text)
            if not size_bytes and _has_media(msg):
                size_bytes = _media_size(msg)
                size_label = _format_size(size_bytes)

            if _has_media(msg):
                title = _best_title(media_name or body_text, fallback=f"File from {bot}")
                payload = {
                    "source_bot": bot,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "action_type": "direct_media",
                    "action": {},
                    "title": title,
                    "size_bytes": int(size_bytes or 0),
                    "size_label": size_label,
                }
                payload["id"] = _candidate_id(payload)
                results.append(payload)

            reply_markup = getattr(msg, "reply_markup", None)
            inline = getattr(reply_markup, "inline_keyboard", None) if reply_markup else None
            if inline:
                for r_idx, row in enumerate(inline):
                    for c_idx, btn in enumerate(row or []):
                        btxt = str(getattr(btn, "text", "") or "").strip()
                        burl = str(getattr(btn, "url", "") or "").strip()
                        bcb = str(getattr(btn, "callback_data", "") or "").strip()
                        if not burl and not bcb:
                            continue
                        item_title = _best_title(btxt or body_text, fallback=f"Button from {bot}")
                        b_size_bytes, b_size_label = _extract_size(f"{btxt} {body_text}")
                        action_type = "button_url" if burl else "button_callback"
                        payload = {
                            "source_bot": bot,
                            "chat_id": chat_id,
                            "message_id": message_id,
                            "action_type": action_type,
                            "action": {
                                "row": r_idx,
                                "col": c_idx,
                                "button_text": btxt,
                                "url": burl,
                                "callback_data": bcb,
                            },
                            "title": item_title,
                            "size_bytes": int(b_size_bytes or 0),
                            "size_label": b_size_label,
                        }
                        payload["id"] = _candidate_id(payload)
                        results.append(payload)

            if body_text:
                for url in _TME_URL_RE.findall(body_text):
                    line_title = _best_title(body_text, fallback=f"Link from {bot}")
                    line_size_bytes, line_size_label = _extract_size(body_text)
                    payload = {
                        "source_bot": bot,
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "action_type": "line_url",
                        "action": {"url": url},
                        "title": line_title,
                        "size_bytes": int(line_size_bytes or 0),
                        "size_label": line_size_label,
                    }
                    payload["id"] = _candidate_id(payload)
                    results.append(payload)
    except Exception as e:
        logs.append(f"{bot}: read failed ({e})")

    # keep query relevance first
    qlow = str(query or "").strip().lower()
    if qlow:
        results.sort(
            key=lambda x: (
                1 if qlow in str(x.get("title", "")).lower() else 0,
                int(x.get("size_bytes", 0) or 0),
            ),
            reverse=True,
        )
    return results


def _dedupe_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    out.sort(key=lambda x: (int(x.get("size_bytes", 0) or 0), str(x.get("title", ""))), reverse=True)
    return out


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
            logs.append(f"Joined force-sub channel: {channel}")
            joined.append(channel)
        except Exception as e:
            # Already joined or inaccessible; still keep in known list if parseable.
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


def _parse_tme_action(url: str) -> tuple[str, str]:
    # returns (target_chat, start_payload)
    raw = str(url or "").strip()
    if not raw:
        return "", ""
    if raw.startswith("tg://"):
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query or "")
        domain = (qs.get("domain", [""])[0] or "").strip()
        start = (qs.get("start", [""])[0] or "").strip()
        if domain:
            return _norm_bot(domain), start
        return "", ""
    parsed = urlparse(raw)
    if "t.me" not in (parsed.netloc or ""):
        return "", ""
    path = (parsed.path or "").strip("/")
    if not path:
        return "", ""
    # join links are not bot-start actions
    if path.startswith("+") or path.startswith("joinchat/"):
        return raw, ""
    username = path.split("/")[0]
    start = (parse_qs(parsed.query or "").get("start", [""])[0] or "").strip()
    return _norm_bot(username), start


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
    watch_chats: list[str],
    since_ts: float,
    cfg: FileFetcherSettings,
    logs: list[str],
    seen_keys: set[str],
) -> tuple[int, list[str]]:
    forwarded = 0
    joined_total: list[str] = []
    for _ in range(7):
        await asyncio.sleep(2)
        for chat in watch_chats:
            try:
                async for msg in client.get_chat_history(chat, limit=30):
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
    source_bots_text: str = Form(""),
    destination_bot: str = Form(""),
    force_sub_channels_text: str = Form(""),
):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not allowed")

    cfg = await _ensure_settings()
    source_bots = _parse_multiline_refs(source_bots_text, channel=False)
    destination = _norm_bot(destination_bot or cfg.destination_bot or "mysticmovies_bot").lstrip("@")
    force_channels = _parse_multiline_refs(force_sub_channels_text, channel=True)

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
    source_bots = _dedupe_keep_order([_norm_bot(x) for x in (cfg.source_bots or []) if _norm_bot(x)])
    if not source_bots:
        return JSONResponse({"ok": False, "error": "No source bots configured in settings."}, status_code=400)

    client, err = await _ensure_user_session_client()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)

    logs: list[str] = []
    sent_mark: dict[str, float] = {}
    for bot in source_bots:
        try:
            sent = await client.send_message(bot, q)
            sent_mark[bot] = max(0.0, _msg_ts(sent) - 2.0)
            logs.append(f"Sent query to {bot}: {q}")
        except Exception as e:
            logs.append(f"Send failed for {bot}: {e}")
            sent_mark[bot] = _now_ts() - 90.0

    await asyncio.sleep(3.0)
    all_items: list[dict[str, Any]] = []
    for bot in source_bots:
        all_items.extend(await _collect_candidates_for_bot(client, bot, q, sent_mark.get(bot, _now_ts() - 90.0), logs))

    items = _dedupe_candidates(all_items)
    _put_cache(_user_cache_key(user), items)
    logs.append(f"Total candidates: {len(items)}")

    return JSONResponse({"ok": True, "items": items, "logs": logs})


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

    cached_items = _get_cache(_user_cache_key(user))
    if not cached_items:
        return JSONResponse({"ok": False, "error": "Search cache expired. Search again."}, status_code=400)
    item_map = {str(x.get("id") or ""): x for x in cached_items}
    chosen = [item_map[x] for x in selected_ids if x in item_map]
    if not chosen:
        return JSONResponse({"ok": False, "error": "Selected items are no longer available. Search again."}, status_code=400)

    logs: list[str] = []
    forwarded_total = 0
    joined_total: list[str] = []
    seen_keys: set[str] = set()

    # Ensure destination bot chat exists.
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
        start_ts = _now_ts() - 1.5
        watch_chats = [source_bot] if source_bot else []

        logs.append(f"Processing: {item.get('title') or 'Untitled'} ({action_type})")
        if action_type == "direct_media":
            forwarded_total += await _forward_message(client, destination_bot, chat_id, message_id, logs)
            continue

        if action_type == "button_callback":
            try:
                msg = await client.get_messages(chat_id=chat_id, message_ids=message_id)
                row = int(action.get("row", 0) or 0)
                col = int(action.get("col", 0) or 0)
                try:
                    await msg.click(x=row, y=col)
                    logs.append(f"Clicked callback button r{row} c{col} in {source_bot}")
                except Exception:
                    btn_text = str(action.get("button_text") or "").strip()
                    if btn_text:
                        await msg.click(btn_text)
                        logs.append(f"Clicked callback button by text in {source_bot}")
                    else:
                        raise
            except Exception as e:
                logs.append(f"Callback click failed: {e}")

        if action_type in {"button_url", "line_url"}:
            raw_url = str(action.get("url") or "").strip()
            target, start_payload = _parse_tme_action(raw_url)
            if target.startswith("http://") or target.startswith("https://") or target.startswith("t.me/"):
                joined = await _join_channels_and_save(client, cfg, [target], logs)
                joined_total.extend(joined)
            elif target:
                try:
                    if start_payload:
                        await client.send_message(target, f"/start {start_payload}")
                    else:
                        await client.send_message(target, "/start")
                    logs.append(f"Opened {target} via start link.")
                    watch_chats.append(target)
                except Exception as e:
                    logs.append(f"Open-link action failed for {target}: {e}")

        forwarded, joined = await _collect_new_media_and_force_sub(
            client=client,
            destination_bot=destination_bot,
            watch_chats=_dedupe_keep_order([x for x in watch_chats if x]),
            since_ts=start_ts,
            cfg=cfg,
            logs=logs,
            seen_keys=seen_keys,
        )
        forwarded_total += int(forwarded or 0)
        joined_total.extend(joined)

    joined_total = _dedupe_keep_order(joined_total)
    return JSONResponse(
        {
            "ok": True,
            "forwarded_count": forwarded_total,
            "joined_channels": joined_total,
            "logs": logs,
        }
    )
