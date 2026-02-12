import os
import asyncio
import logging
import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from app.core.config import settings
from app.core.telegram_bot import start_telegram, stop_telegram
from app.core.telethon_storage import get_client as get_telethon_client, stop_client as stop_telethon_client
from app.db.models import init_db
from app.routes import auth, content, dashboard, stream, admin, share

# Prefer uvloop for faster asyncio if available
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

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

# Include all Routes
app.include_router(auth.router)
app.include_router(content.router)
app.include_router(dashboard.router)
app.include_router(stream.router)
app.include_router(admin.router)
app.include_router(share.router)

if __name__ == "__main__":
    # Render sets PORT; default to 8000 for local dev.
    port = int(os.getenv("PORT", "8000"))
    # Enable auto-reload only when explicitly requested.
    reload = os.getenv("RELOAD", "").lower() in ("1", "true", "yes")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
