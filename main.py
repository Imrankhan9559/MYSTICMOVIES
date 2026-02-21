import os
import asyncio
import logging
import time
import re
import uvicorn
from fastapi import FastAPI, Response, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

# Set loop policy as early as possible before importing modules that create
# asyncio-aware clients/locks (Pyrogram/Telethon), to avoid loop mismatch issues.
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

# Pyrogram sync wrapper expects a current main-thread loop at import time.
# Ensure one exists under uvloop policy before importing telegram modules.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from app.core.config import settings
from app.core.telegram_bot import start_telegram, stop_telegram
from app.core.telethon_storage import get_client as get_telethon_client, stop_client as stop_telethon_client
from app.db.models import init_db
from app.routes import auth, content, dashboard, stream, admin, share, app_client, advance_mass_content, file_fetcher

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


_REQUEST_TIMING_ENABLED = _env_flag("REQUEST_TIMING_ENABLED", True)
_REQUEST_TIMING_SLOW_MS = max(1.0, _env_float("REQUEST_TIMING_SLOW_MS", 1200.0))
_REQUEST_TIMING_LOG_ALL = _env_flag("REQUEST_TIMING_LOG_ALL", False)
_REQUEST_TIMING_FLUSH_SEC = max(5.0, _env_float("REQUEST_TIMING_FLUSH_SEC", 60.0))
_REQUEST_TIMING_TOP_N = max(1, _env_int("REQUEST_TIMING_TOP_N", 8))
_REQUEST_TIMING_EXCLUDE_PATHS = {
    p.strip() for p in (os.getenv("REQUEST_TIMING_EXCLUDE_PATHS", "/favicon.ico,/static,/healthz").split(",")) if p.strip()
}
_REQUEST_TIMING_METRICS: dict[str, dict[str, float | int]] = {}
_REQUEST_TIMING_LAST_FLUSH_TS = 0.0


def _path_timing_excluded(path: str) -> bool:
    path = (path or "").strip()
    if not path:
        return True
    for prefix in _REQUEST_TIMING_EXCLUDE_PATHS:
        if path == prefix or path.startswith(prefix):
            return True
    return False


def _normalize_path_for_metrics(path: str) -> str:
    cleaned = (path or "").strip() or "/"
    # Collapse high-cardinality identifiers in routes for useful aggregation.
    cleaned = re.sub(r"/[0-9a-fA-F]{24}(?=/|$)", "/:oid", cleaned)
    cleaned = re.sub(
        r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?=/|$)",
        "/:uuid",
        cleaned,
    )
    cleaned = re.sub(r"/\d+(?=/|$)", "/:n", cleaned)
    return cleaned


def _record_request_metric(path: str, elapsed_ms: float, is_slow: bool) -> None:
    key = _normalize_path_for_metrics(path)
    row = _REQUEST_TIMING_METRICS.setdefault(
        key,
        {"count": 0, "total_ms": 0.0, "max_ms": 0.0, "slow_count": 0},
    )
    row["count"] = int(row["count"]) + 1
    row["total_ms"] = float(row["total_ms"]) + float(elapsed_ms)
    if elapsed_ms > float(row["max_ms"]):
        row["max_ms"] = float(elapsed_ms)
    if is_slow:
        row["slow_count"] = int(row["slow_count"]) + 1


def _flush_request_metrics_if_due() -> None:
    global _REQUEST_TIMING_LAST_FLUSH_TS
    if not _REQUEST_TIMING_ENABLED:
        return
    now = time.monotonic()
    if (now - _REQUEST_TIMING_LAST_FLUSH_TS) < _REQUEST_TIMING_FLUSH_SEC:
        return
    _REQUEST_TIMING_LAST_FLUSH_TS = now
    if not _REQUEST_TIMING_METRICS:
        return

    rows = []
    for path, info in _REQUEST_TIMING_METRICS.items():
        count = int(info.get("count") or 0)
        if count <= 0:
            continue
        total_ms = float(info.get("total_ms") or 0.0)
        max_ms = float(info.get("max_ms") or 0.0)
        slow_count = int(info.get("slow_count") or 0)
        avg_ms = (total_ms / count) if count else 0.0
        rows.append((path, count, avg_ms, max_ms, slow_count))
    if not rows:
        return

    rows.sort(key=lambda item: (item[2], item[3], item[1]), reverse=True)
    top = rows[:_REQUEST_TIMING_TOP_N]
    summary = " | ".join(
        f"{path} count={count} avg={avg_ms:.1f}ms max={max_ms:.1f}ms slow={slow_count}"
        for path, count, avg_ms, max_ms, slow_count in top
    )
    logger.info("HTTP timing top endpoints: %s", summary)
    _REQUEST_TIMING_METRICS.clear()


async def _init_db_with_retry() -> None:
    attempts = max(1, _env_int("DB_INIT_ATTEMPTS", 5))
    delay_sec = max(0.5, _env_float("DB_INIT_DELAY_SEC", 2.0))
    for attempt in range(1, attempts + 1):
        try:
            await init_db()
            if attempt > 1:
                logger.info("MongoDB connected on attempt %s/%s", attempt, attempts)
            return
        except Exception as exc:
            if attempt >= attempts:
                logger.exception("MongoDB init failed after %s attempt(s).", attempts)
                raise
            logger.warning(
                "MongoDB init attempt %s/%s failed: %s. Retrying in %.1fs.",
                attempt,
                attempts,
                exc,
                delay_sec,
            )
            await asyncio.sleep(delay_sec)


async def _safe_shutdown(name: str, fn: Callable[[], Awaitable[None]], timeout_sec: float = 15.0) -> None:
    try:
        await asyncio.wait_for(fn(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        logger.warning("%s shutdown timed out after %.1fs", name, timeout_sec)
    except Exception:
        logger.exception("%s shutdown failed", name)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await _init_db_with_retry()

    telegram_required = _env_flag("TELEGRAM_STARTUP_REQUIRED", False)
    telethon_required = _env_flag("TELETHON_STARTUP_REQUIRED", False)

    try:
        await start_telegram()
    except Exception:
        logger.exception("Telegram startup failed.")
        if telegram_required:
            raise

    try:
        await get_telethon_client()
    except Exception:
        logger.exception("Telethon startup failed.")
        if telethon_required:
            await _safe_shutdown("Telegram", stop_telegram)
            await _safe_shutdown("Telethon", stop_telethon_client)
            raise

    try:
        yield
    finally:
        await _safe_shutdown("Telegram", stop_telegram)
        await _safe_shutdown("Telethon", stop_telethon_client)

app = FastAPI(title="MORGANXMYSTIC", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    if not _REQUEST_TIMING_ENABLED:
        return await call_next(request)

    method = (request.method or "").upper()
    path = request.url.path or "/"
    if _path_timing_excluded(path):
        return await call_next(request)

    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        is_slow = elapsed_ms >= _REQUEST_TIMING_SLOW_MS
        _record_request_metric(path, elapsed_ms, is_slow=True)
        logger.exception("HTTP %s %s failed after %.1fms", method, path, elapsed_ms)
        _flush_request_metrics_if_due()
        raise

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    is_slow = elapsed_ms >= _REQUEST_TIMING_SLOW_MS
    _record_request_metric(path, elapsed_ms, is_slow=is_slow)
    response.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"

    if _REQUEST_TIMING_LOG_ALL or is_slow or int(response.status_code) >= 500:
        logger.warning(
            "HTTP %s %s -> %s in %.1fms",
            method,
            path,
            int(response.status_code),
            elapsed_ms,
        )

    _flush_request_metrics_if_due()
    return response

# --- FIX: Auto-Create Static Directory ---
# This prevents the "RuntimeError: Directory 'app/static' does not exist" on Koyeb
static_dir = "app/static"
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

# Mount Static Files (CSS/JS)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# Fix for annoying 404 Favicon errors in browser console
@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    icon_path = os.path.join("app", "templates", "fav-icon.png")
    if os.path.exists(icon_path):
        return FileResponse(icon_path)
    return Response(status_code=204)


@app.head("/", include_in_schema=False)
async def root_head_ok():
    # Some uptime probes/scanners use HEAD; keep it cheap and avoid noisy 405 logs.
    return Response(status_code=200)

# Include all Routes
app.include_router(auth.router)
app.include_router(content.router)
app.include_router(dashboard.router)
app.include_router(stream.router)
app.include_router(admin.router)
app.include_router(advance_mass_content.router)
app.include_router(file_fetcher.router)
app.include_router(share.router)
app.include_router(app_client.router)

if __name__ == "__main__":
    # Render sets PORT; default to 8000 for local dev.
    port = int(os.getenv("PORT", "8000"))
    # Enable auto-reload only when explicitly requested.
    reload = os.getenv("RELOAD", "").lower() in ("1", "true", "yes")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
