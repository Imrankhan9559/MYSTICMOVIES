import base64
import hashlib
import hmac
import json
import re
import time
import urllib.parse
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Request, Header, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, Response

from app.core.config import settings
from app.db.models import (
    AppBroadcast,
    AppDeviceSession,
    AppRelease,
    AppSettings,
    SiteSettings,
    TokenSetting,
)
from app.routes.content import (
    _build_catalog,
    _card_matches_query,
    _decorate_catalog_cards,
    _ensure_group_assets,
    _ensure_share_token,
    _group_slug,
    _is_admin,
    _normalize_filter_type,
    _normalize_sort_type,
    _quality_rank,
    _share_params,
    _sort_catalog_cards,
    _viewer_name,
)
from app.routes.dashboard import get_current_user

router = APIRouter(prefix="/app-api")


DEFAULT_HEADER_MENU = [
    {"label": "Home", "url": "/", "icon": "fas fa-house"},
    {"label": "Content", "url": "/content", "icon": "fas fa-film"},
    {"label": "Request Content", "url": "/request-content", "icon": "fas fa-inbox"},
]
DEFAULT_FOOTER_EXPLORE_LINKS = [
    {"label": "Movies Library", "url": "/content/f/movies"},
    {"label": "Web Series", "url": "/content/f/series"},
    {"label": "Latest Uploads", "url": "/content/f/all"},
]
DEFAULT_FOOTER_SUPPORT_LINKS = [
    {"label": "Request Content", "url": "/request-content"},
    {"label": "Report an Issue", "url": "/request-content"},
]
IMAGE_PROXY_ALLOWED_HOSTS = {
    "image.tmdb.org",
    "raw.githubusercontent.com",
    "mysticmovies.onrender.com",
}


def _now_ts() -> int:
    return int(time.time())


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * ((4 - len(text) % 4) % 4)
    return base64.urlsafe_b64decode((text + pad).encode("ascii"))


def _clean_version(value: str) -> tuple[int, ...]:
    parts = [int(p) for p in re.findall(r"\d+", (value or "").strip())]
    return tuple(parts[:4]) if parts else (0,)


def _version_lt(left: str, right: str) -> bool:
    a = _clean_version(left)
    b = _clean_version(right)
    max_len = max(len(a), len(b))
    a = a + (0,) * (max_len - len(a))
    b = b + (0,) * (max_len - len(b))
    return a < b


async def _app_settings() -> AppSettings:
    row = await AppSettings.find_one(AppSettings.key == "main")
    if not row:
        row = AppSettings(key="main", updated_at=datetime.now())
        await row.insert()
    # Keep username synced from env unless admin already set it.
    if not (row.telegram_bot_username or "").strip():
        row.telegram_bot_username = (getattr(settings, "BOT_USERNAME", "") or "").strip()
        await row.save()
    return row


def _clean_link_rows(value: Any, include_icon: bool = False) -> list[dict]:
    rows = value if isinstance(value, list) else []
    cleaned: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = (row.get("label") or "").strip()
        url = (row.get("url") or "").strip() or "#"
        icon = (row.get("icon") or "").strip()
        if not label:
            continue
        payload = {"label": label, "url": url}
        if include_icon:
            payload["icon"] = icon
        cleaned.append(payload)
    return cleaned


async def _site_settings() -> SiteSettings:
    row = await SiteSettings.find_one(SiteSettings.key == "main")
    if not row:
        row = SiteSettings(key="main")
        await row.insert()

    changed = False
    if not (getattr(row, "topbar_text", "") or "").strip():
        row.topbar_text = "Welcome to Mystic Movies"
        changed = True
    if getattr(row, "logo_path", None) is None:
        row.logo_path = ""
        changed = True

    header_menu = _clean_link_rows(getattr(row, "header_menu", []), include_icon=True)
    if not header_menu:
        row.header_menu = [x.copy() for x in DEFAULT_HEADER_MENU]
        changed = True
    else:
        row.header_menu = header_menu

    footer_explore_links = _clean_link_rows(getattr(row, "footer_explore_links", []), include_icon=False)
    if not footer_explore_links:
        row.footer_explore_links = [x.copy() for x in DEFAULT_FOOTER_EXPLORE_LINKS]
        changed = True
    else:
        row.footer_explore_links = footer_explore_links

    footer_support_links = _clean_link_rows(getattr(row, "footer_support_links", []), include_icon=False)
    if not footer_support_links:
        row.footer_support_links = [x.copy() for x in DEFAULT_FOOTER_SUPPORT_LINKS]
        changed = True
    else:
        row.footer_support_links = footer_support_links

    if changed:
        row.updated_at = datetime.now()
        await row.save()
    return row


def _absolute_url(request: Request, path_or_url: str) -> str:
    raw = (path_or_url or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    path = raw if raw.startswith("/") else f"/{raw}"
    return f"{str(request.base_url).rstrip('/')}{path}"


def _image_proxy_path(raw_url: str) -> str:
    source = (raw_url or "").strip()
    if not source:
        return ""
    encoded = urllib.parse.quote(source, safe="")
    return f"/app-api/image?src={encoded}"


def _app_image_url(request: Request, raw_url: str) -> str:
    source = (raw_url or "").strip()
    if not source:
        return ""
    parsed = urllib.parse.urlparse(source)
    host = (parsed.hostname or "").strip().lower()
    if host == "image.tmdb.org":
        return _absolute_url(request, _image_proxy_path(source))
    return _absolute_url(request, source)


async def _handshake_secret() -> str:
    row = await TokenSetting.find_one(TokenSetting.key == "app_handshake_secret")
    if row and row.value:
        return row.value
    secret = hashlib.sha256(uuid.uuid4().hex.encode("utf-8")).hexdigest()
    if row:
        row.value = secret
        row.updated_at = datetime.now()
        await row.save()
    else:
        await TokenSetting(key="app_handshake_secret", value=secret, created_at=datetime.now(), updated_at=datetime.now()).insert()
    return secret


def _sign_payload(payload: dict[str, Any], secret: str) -> str:
    body = _b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    sig = hmac.new(secret.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_b64url_encode(sig)}"


def _verify_payload(token: str, secret: str) -> dict[str, Any] | None:
    raw = (token or "").strip()
    if "." not in raw:
        return None
    body_part, sig_part = raw.split(".", 1)
    calc = hmac.new(secret.encode("utf-8"), body_part.encode("ascii"), hashlib.sha256).digest()
    try:
        sig = _b64url_decode(sig_part)
    except Exception:
        return None
    if not hmac.compare_digest(calc, sig):
        return None
    try:
        payload = json.loads(_b64url_decode(body_part).decode("utf-8"))
    except Exception:
        return None
    exp = int(payload.get("exp") or 0)
    if exp and _now_ts() > exp:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_handshake_token(request: Request, explicit: str = "", header_value: str = "") -> str:
    token = (explicit or "").strip()
    if token:
        return token
    token = (header_value or "").strip()
    if token:
        return token
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    token_q = (request.query_params.get("hs") or "").strip()
    return token_q


async def _link_token() -> str:
    row = await TokenSetting.find_one(TokenSetting.key == "link_token")
    if not row:
        row = TokenSetting(key="link_token", value=str(uuid.uuid4()), created_at=datetime.now(), updated_at=datetime.now())
        await row.insert()
    return row.value


def _deep_link(bot_username: str, share_token: str, link_token: str = "") -> str:
    user = (bot_username or "").strip().lstrip("@")
    if not user:
        return ""
    start = f"share_{share_token}"
    if link_token:
        start = f"{start}_t_{link_token}"
    return f"https://t.me/{user}?start={start}"


def _normalize_content_key(value: str) -> str:
    return (value or "").strip().lower()


def _find_catalog_group(catalog: list[dict], content_key: str) -> dict | None:
    key = _normalize_content_key(content_key)
    if not key:
        return None

    is_object_id = bool(re.fullmatch(r"[0-9a-fA-F]{24}", content_key or ""))
    if not is_object_id:
        for group in catalog:
            if (group.get("slug") or "").strip().lower() == key:
                return group
        for group in catalog:
            slug_guess = _group_slug(group.get("title", ""), group.get("year", "")).strip().lower()
            if slug_guess == key:
                return group

    for group in catalog:
        group_id = str(group.get("id") or "").strip()
        if group_id and group_id.lower() == key:
            return group
        for item in group.get("items") or []:
            item_id = str(item.get("id") or "").strip()
            if item_id and item_id.lower() == key:
                return group
    return None


@router.post("/handshake")
async def app_handshake(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    device_id = (payload.get("device_id") or "").strip()
    if not device_id:
        return JSONResponse({"ok": False, "error": "device_id is required"}, status_code=400)

    platform = (payload.get("platform") or "android").strip().lower()
    app_version = (payload.get("app_version") or "").strip()
    try:
        build_number = int(payload.get("build_number") or 0)
    except Exception:
        build_number = 0

    user = await get_current_user(request)
    user_phone = user.phone_number if user else (payload.get("user_phone") or "")
    user_name = ""
    if user:
        user_name = (user.requested_name or user.first_name or user.phone_number or "").strip()
    if not user_name:
        user_name = (payload.get("user_name") or "").strip()

    now = _now_ts()
    ttl_seconds = 3600
    secret = await _handshake_secret()
    signed = _sign_payload({
        "did": device_id,
        "plt": platform,
        "ver": app_version,
        "b": build_number,
        "iat": now,
        "exp": now + ttl_seconds,
    }, secret)

    row = await AppDeviceSession.find_one(AppDeviceSession.device_id == device_id)
    if row:
        row.platform = platform
        row.app_version = app_version
        row.build_number = build_number
        row.user_phone = user_phone or row.user_phone
        row.user_name = user_name or row.user_name
        row.handshake_token = signed
        row.handshake_expire_at = datetime.fromtimestamp(now + ttl_seconds)
        row.last_ping_at = datetime.now()
        row.updated_at = datetime.now()
        await row.save()
    else:
        await AppDeviceSession(
            device_id=device_id,
            platform=platform,
            app_version=app_version,
            build_number=build_number,
            user_phone=user_phone or None,
            user_name=user_name or None,
            handshake_token=signed,
            handshake_expire_at=datetime.fromtimestamp(now + ttl_seconds),
            last_ping_at=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ).insert()

    return {"ok": True, "handshake_token": signed, "expires_in": ttl_seconds}


@router.get("/bootstrap")
async def app_bootstrap(
    request: Request,
    hs: str = "",
    x_app_handshake: str = Header(default=""),
):
    token = _extract_handshake_token(request, explicit=hs, header_value=x_app_handshake)
    secret = await _handshake_secret()
    parsed = _verify_payload(token, secret)
    if not parsed:
        return JSONResponse({"ok": False, "error": "Invalid handshake"}, status_code=401)

    app_cfg = await _app_settings()
    site_cfg = await _site_settings()
    release_rows = await AppRelease.find(AppRelease.is_active == True).sort([("build_number", -1), ("created_at", -1)]).limit(1).to_list()
    latest_release = release_rows[0] if release_rows else None

    current_build = int(parsed.get("b") or 0)
    current_version = str(parsed.get("ver") or "")
    latest_build = int(getattr(app_cfg, "latest_build", 0) or 0)
    latest_version = (getattr(app_cfg, "latest_version", "") or "").strip()
    release_notes = (getattr(app_cfg, "latest_release_notes", "") or "").strip()
    update_mode = "none"

    if latest_release:
        if int(getattr(latest_release, "build_number", 0) or 0) > latest_build:
            latest_build = int(getattr(latest_release, "build_number", 0) or 0)
        if not latest_version:
            latest_version = (getattr(latest_release, "version", "") or "").strip()
        if not release_notes:
            release_notes = (getattr(latest_release, "release_notes", "") or "").strip()
        update_mode = (getattr(latest_release, "update_mode", "none") or "none").strip().lower()

    if getattr(app_cfg, "force_update", False):
        update_mode = "forced"
    elif getattr(app_cfg, "recommended_update", False) and update_mode != "forced":
        update_mode = "recommended"

    force_update_required = False
    if latest_build > 0 and current_build > 0 and current_build < latest_build and update_mode == "forced":
        force_update_required = True
    min_supported = (getattr(app_cfg, "min_supported_version", "") or "").strip()
    if min_supported and current_version and _version_lt(current_version, min_supported):
        force_update_required = True
        update_mode = "forced"

    recommend_update = (
        not force_update_required
        and latest_build > 0
        and current_build > 0
        and current_build < latest_build
        and update_mode in {"recommended", "forced"}
    )

    link_token = await _link_token()
    apk_token = (getattr(app_cfg, "latest_apk_share_token", "") or "").strip()
    update_url = ""
    if apk_token:
        update_url = f"/d/{apk_token}?t={urllib.parse.quote(link_token, safe='')}&U=AppUpdater"
    base_url = str(request.base_url).rstrip("/")
    update_url_absolute = f"{base_url}{update_url}" if update_url else ""

    active_broadcasts = await AppBroadcast.find(AppBroadcast.is_active == True).sort("-created_at").limit(12).to_list()
    notifications = []
    for row in active_broadcasts:
        notifications.append({
            "id": str(row.id),
            "title": (getattr(row, "title", "") or "").strip(),
            "message": (getattr(row, "message", "") or "").strip(),
            "type": (getattr(row, "type", "news") or "news").strip().lower(),
            "created_at": getattr(row, "created_at", datetime.now()).isoformat(),
        })

    bot_username = (getattr(app_cfg, "telegram_bot_username", "") or getattr(settings, "BOT_USERNAME", "") or "").strip().lstrip("@")
    site_name = (getattr(site_cfg, "site_name", "") or "").strip() or "mysticmovies"
    footer_text = (getattr(site_cfg, "footer_text", "") or "").strip() or "MysticMovies"
    topbar_text = (getattr(site_cfg, "topbar_text", "") or "").strip() or "Welcome to Mystic Movies"
    logo_path = (getattr(site_cfg, "logo_path", "") or "").strip()
    header_menu = _clean_link_rows(getattr(site_cfg, "header_menu", []), include_icon=True)
    if not header_menu:
        header_menu = [x.copy() for x in DEFAULT_HEADER_MENU]
    footer_explore = _clean_link_rows(getattr(site_cfg, "footer_explore_links", []), include_icon=False)
    if not footer_explore:
        footer_explore = [x.copy() for x in DEFAULT_FOOTER_EXPLORE_LINKS]
    footer_support = _clean_link_rows(getattr(site_cfg, "footer_support_links", []), include_icon=False)
    if not footer_support:
        footer_support = [x.copy() for x in DEFAULT_FOOTER_SUPPORT_LINKS]
    footer_about = (getattr(site_cfg, "footer_about_text", "") or "").strip()
    if not footer_about:
        footer_about = "Mystic Movies provides high-quality content for free. If a movie is missing, let us know."

    splash_raw = (getattr(app_cfg, "splash_image_url", "") or "").strip()
    loading_raw = (getattr(app_cfg, "loading_icon_url", "") or "").strip()

    return {
        "ok": True,
        "app": {
            "name": (getattr(app_cfg, "app_name", "") or "MysticMovies Android").strip(),
            "package_name": (getattr(app_cfg, "package_name", "") or "com.mysticmovies.app").strip(),
            "splash_image_url": _app_image_url(request, splash_raw),
            "loading_icon_url": _app_image_url(request, loading_raw),
            "splash_image_original": splash_raw,
            "loading_icon_original": loading_raw,
            "onboarding_message": (getattr(app_cfg, "onboarding_message", "") or "").strip(),
            "ads_message": (getattr(app_cfg, "ads_message", "") or "").strip(),
            "push_enabled": bool(getattr(app_cfg, "push_enabled", True)),
            "keepalive_on_launch": bool(getattr(app_cfg, "keepalive_on_launch", True)),
            "maintenance_mode": bool(getattr(app_cfg, "maintenance_mode", False)),
            "maintenance_message": (getattr(app_cfg, "maintenance_message", "") or "").strip(),
        },
        "update": {
            "mode": update_mode,
            "force_required": force_update_required,
            "recommend": recommend_update,
            "latest_version": latest_version,
            "latest_build": latest_build,
            "release_notes": release_notes,
            "update_popup_title": (getattr(app_cfg, "update_popup_title", "") or "Update Available").strip(),
            "update_popup_body": (getattr(app_cfg, "update_popup_body", "") or "A new app version is available.").strip(),
            "apk_download_url": update_url,
            "apk_download_url_absolute": update_url_absolute,
        },
        "notifications": notifications,
        "telegram": {
            "bot_username": bot_username,
        },
        "ui": {
            "site_name": site_name,
            "footer_text": footer_text,
            "topbar_text": topbar_text,
            "logo_url": _app_image_url(request, logo_path),
            "logo_original": logo_path,
            "header_menu": header_menu,
            "footer_explore_links": footer_explore,
            "footer_support_links": footer_support,
            "footer_about_text": footer_about,
        },
        "endpoints": {
            "ping": "/app-api/ping",
            "telegram_link": "/app-api/telegram-start/{share_token}",
            "image_proxy": "/app-api/image?src={url_encoded}",
        },
        "server_time": datetime.now().isoformat(),
    }


@router.post("/ping")
async def app_ping(
    request: Request,
    hs: str = "",
    x_app_handshake: str = Header(default=""),
):
    token = _extract_handshake_token(request, explicit=hs, header_value=x_app_handshake)
    secret = await _handshake_secret()
    parsed = _verify_payload(token, secret)
    if not parsed:
        return JSONResponse({"ok": False, "error": "Invalid handshake"}, status_code=401)

    device_id = (parsed.get("did") or "").strip()
    if not device_id:
        return JSONResponse({"ok": False, "error": "Invalid device"}, status_code=400)

    row = await AppDeviceSession.find_one(AppDeviceSession.device_id == device_id)
    if row:
        row.last_ping_at = datetime.now()
        row.updated_at = datetime.now()
        await row.save()
    return {"ok": True, "device_id": device_id, "alive": True, "server_time": datetime.now().isoformat()}


@router.get("/catalog")
async def app_catalog(
    request: Request,
    filter: str = "all",
    q: str = "",
    sort: str = "release_new",
    page: int = 1,
    per_page: int = 24,
):
    user = await get_current_user(request)
    is_admin = _is_admin(user)

    normalized_filter = _normalize_filter_type(filter)
    normalized_sort = _normalize_sort_type(sort)
    query = (q or "").strip()

    try:
        page_num = max(1, int(page or 1))
    except Exception:
        page_num = 1
    try:
        per_page_num = int(per_page or 24)
    except Exception:
        per_page_num = 24
    per_page_num = max(6, min(per_page_num, 60))

    cards = await _build_catalog(user, is_admin, limit=4000)

    if normalized_filter == "movies":
        cards = [card for card in cards if (card.get("type") or "").strip().lower() == "movie"]
    elif normalized_filter == "series":
        cards = [card for card in cards if (card.get("type") or "").strip().lower() == "series"]

    if query:
        cards = [card for card in cards if _card_matches_query(card, query)]

    cards = _sort_catalog_cards(cards, normalized_sort)
    cards = _decorate_catalog_cards(cards)

    total_items = len(cards)
    total_pages = max(1, (total_items + per_page_num - 1) // per_page_num)
    page_num = max(1, min(page_num, total_pages))
    start = (page_num - 1) * per_page_num
    end = start + per_page_num
    paged = cards[start:end]

    base_url = str(request.base_url).rstrip("/")
    items = []
    for card in paged:
        slug = (card.get("slug") or "").strip()
        if not slug:
            slug = _group_slug(card.get("title", ""), card.get("year", "")).strip()
        detail_path = f"/content/details/{slug}" if slug else ""
        poster_raw = (card.get("poster") or "").strip()
        backdrop_raw = (card.get("backdrop") or "").strip()
        items.append({
            "id": str(card.get("id") or ""),
            "slug": slug,
            "title": card.get("title") or "",
            "year": card.get("year") or "",
            "type": card.get("type") or "",
            "poster": _app_image_url(request, poster_raw),
            "backdrop": _app_image_url(request, backdrop_raw),
            "poster_original": poster_raw,
            "backdrop_original": backdrop_raw,
            "description": card.get("description") or "",
            "release_date": card.get("release_date") or "",
            "quality_row": card.get("quality_row") or [],
            "season_text": card.get("season_text") or "",
            "detail_path": detail_path,
            "detail_url": f"{base_url}{detail_path}" if detail_path else "",
        })

    slider = []
    for card in cards[:8]:
        slug = (card.get("slug") or "").strip()
        if not slug:
            slug = _group_slug(card.get("title", ""), card.get("year", "")).strip()
        detail_path = f"/content/details/{slug}" if slug else ""
        image_raw = (card.get("backdrop") or "").strip() or (card.get("poster") or "").strip()
        if not image_raw:
            continue
        slider.append({
            "title": card.get("title") or "",
            "subtitle": f"{(card.get('year') or '').strip()} | {(card.get('type') or '').strip().upper()}".strip(" |"),
            "image": _app_image_url(request, image_raw),
            "image_original": image_raw,
            "detail_path": detail_path,
            "detail_url": f"{base_url}{detail_path}" if detail_path else "",
        })

    return {
        "ok": True,
        "filter": normalized_filter,
        "sort": normalized_sort,
        "query": query,
        "items": items,
        "slider": slider,
        "pagination": {
            "page": page_num,
            "per_page": per_page_num,
            "total_items": total_items,
            "total_pages": total_pages,
            "has_next": page_num < total_pages,
            "has_prev": page_num > 1,
        },
        "server_time": datetime.now().isoformat(),
    }


@router.get("/content/{content_key}")
async def app_content_detail(
    request: Request,
    content_key: str,
):
    user = await get_current_user(request)
    is_admin = _is_admin(user)

    catalog = await _build_catalog(user, is_admin, limit=4500)
    group = _find_catalog_group(catalog, content_key)
    if not group:
        return JSONResponse({"ok": False, "error": "Content not found"}, status_code=404)

    group = await _ensure_group_assets(group)

    link_token = await _link_token()
    viewer_name = (_viewer_name(user) or "Mystic User").strip()
    share_query_payload = _share_params(link_token, viewer_name)
    query = f"?{share_query_payload}" if share_query_payload else ""
    app_cfg = await _app_settings()
    bot_username = (getattr(app_cfg, "telegram_bot_username", "") or getattr(settings, "BOT_USERNAME", "") or "").strip().lstrip("@")
    encoded_link_token = urllib.parse.quote(link_token, safe="")

    movie_links = []
    if (group.get("type") or "").strip().lower() == "movie":
        movie_qualities = sorted(
            (group.get("qualities") or {}).items(),
            key=lambda row: (-_quality_rank(row[0]), row[0]),
        )
        for quality, row in movie_qualities:
            file_id = str((row or {}).get("file_id") or "").strip()
            if not file_id:
                continue
            token = await _ensure_share_token(file_id)
            if not token:
                continue
            view_url = f"/s/{token}{query}"
            stream_url = f"/s/stream/{token}{query}"
            download_url = f"/d/{token}{query}"
            telegram_url = f"/t/{token}{query}"
            watch_together_url = f"/w/{token}{query}"
            telegram_start_url = f"/app-api/telegram-start/{token}?t={encoded_link_token}"
            movie_links.append({
                "label": quality,
                "size": int((row or {}).get("size") or 0),
                "view_url": view_url,
                "stream_url": stream_url,
                "download_url": download_url,
                "telegram_url": telegram_url,
                "telegram_start_url": telegram_start_url,
                "telegram_deep_link": _deep_link(bot_username, token, link_token) if bot_username else "",
                "watch_together_url": watch_together_url,
            })

    series_links = []
    if (group.get("type") or "").strip().lower() == "series":
        seasons = group.get("seasons") or {}
        for season_no, episodes in sorted(seasons.items(), key=lambda row: int(row[0])):
            qualities = set()
            preview_link = ""
            preview_stream_link = ""
            preview_telegram_start = ""
            total_episodes = 0
            for episode_no, variants in sorted((episodes or {}).items(), key=lambda row: int(row[0])):
                total_episodes += 1
                for quality, row in sorted((variants or {}).items(), key=lambda v: (-_quality_rank(v[0]), v[0])):
                    qualities.add(quality)
                    if preview_link:
                        continue
                    file_id = str((row or {}).get("file_id") or "").strip()
                    if not file_id:
                        continue
                    token = await _ensure_share_token(file_id)
                    if not token:
                        continue
                    preview_link = f"/s/{token}{query}"
                    preview_stream_link = f"/s/stream/{token}{query}"
                    preview_telegram_start = f"/app-api/telegram-start/{token}?t={encoded_link_token}"
            series_links.append({
                "season": int(season_no),
                "episode_count": total_episodes,
                "qualities": sorted(qualities, key=lambda value: (-_quality_rank(value), value)),
                "preview_view_url": preview_link,
                "preview_stream_url": preview_stream_link if preview_link else "",
                "preview_telegram_start_url": preview_telegram_start if preview_link else "",
            })

    slug = (group.get("slug") or "").strip()
    if not slug:
        slug = _group_slug(group.get("title", ""), group.get("year", "")).strip()
    detail_path = f"/content/details/{slug}" if slug else ""
    base_url = str(request.base_url).rstrip("/")

    return {
        "ok": True,
        "item": {
            "id": str(group.get("id") or ""),
            "slug": slug,
            "title": group.get("title") or "",
            "year": group.get("year") or "",
            "type": group.get("type") or "",
            "poster": _app_image_url(request, group.get("poster") or ""),
            "backdrop": _app_image_url(request, group.get("backdrop") or ""),
            "poster_original": group.get("poster") or "",
            "backdrop_original": group.get("backdrop") or "",
            "description": group.get("description") or "",
            "release_date": group.get("release_date") or "",
            "genres": group.get("genres") or [],
            "actors": group.get("actors") or [],
            "director": group.get("director") or "",
            "trailer_url": group.get("trailer_url") or "",
            "trailer_key": group.get("trailer_key") or "",
        },
        "movie_links": movie_links,
        "series_links": series_links,
        "detail_path": detail_path,
        "detail_url": f"{base_url}{detail_path}" if detail_path else "",
        "viewer_name": viewer_name,
        "link_token": link_token,
        "server_time": datetime.now().isoformat(),
    }


@router.get("/image")
async def app_image_proxy(src: str = "", url: str = ""):
    target = (src or url or "").strip()
    if not target:
        return JSONResponse({"ok": False, "error": "Missing src"}, status_code=400)

    parsed = urllib.parse.urlparse(target)
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"https", "http"}:
        return JSONResponse({"ok": False, "error": "Invalid image URL scheme"}, status_code=400)
    if host not in IMAGE_PROXY_ALLOWED_HOSTS:
        return JSONResponse({"ok": False, "error": "Host not allowed"}, status_code=400)

    request_headers = {
        "User-Agent": "MysticMovies-AppProxy/1.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    req = urllib.request.Request(target, headers=request_headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            status_code = int(getattr(resp, "status", 200) or 200)
            if status_code >= 400:
                return JSONResponse({"ok": False, "error": f"Upstream error {status_code}"}, status_code=status_code)
            body = resp.read()
            content_type = resp.headers.get("Content-Type", "image/jpeg")
            return Response(
                content=body,
                media_type=content_type,
                headers={
                    "Cache-Control": "public, max-age=21600",
                },
            )
    except urllib.error.HTTPError as exc:
        return JSONResponse({"ok": False, "error": f"Image fetch failed ({exc.code})"}, status_code=exc.code)
    except Exception:
        return JSONResponse({"ok": False, "error": "Image fetch failed"}, status_code=502)


@router.get("/telegram-start/{share_token}")
async def app_telegram_start(
    request: Request,
    share_token: str,
    t: str = "",
    redirect: int = 0,
):
    token = (share_token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Invalid share token")
    app_cfg = await _app_settings()
    bot_username = (getattr(app_cfg, "telegram_bot_username", "") or getattr(settings, "BOT_USERNAME", "") or "").strip().lstrip("@")
    if not bot_username:
        raise HTTPException(status_code=400, detail="BOT_USERNAME not configured")
    deep_link = _deep_link(bot_username, token, (t or "").strip())
    if not deep_link:
        raise HTTPException(status_code=400, detail="Could not build telegram link")
    if int(redirect or 0) == 1:
        return RedirectResponse(deep_link)
    return {"ok": True, "deep_link": deep_link}
