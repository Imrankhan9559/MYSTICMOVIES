import os
import uvicorn
from fastapi import FastAPI, Response
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.telegram_bot import start_telegram, stop_telegram
from app.core.telethon_storage import get_client as get_telethon_client, stop_client as stop_telethon_client
from app.db.models import init_db
from app.routes import auth, dashboard, stream, admin, share

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to DB and Start Telegram Client
    await init_db()
    await start_telegram()
    await get_telethon_client()
    yield
    # Shutdown: Stop Telegram Client
    await stop_telegram()
    await stop_telethon_client()

app = FastAPI(title="MORGANXMYSTIC Storage", lifespan=lifespan)

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
    return Response(status_code=204)

# Include all Routes
app.include_router(auth.router)
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
