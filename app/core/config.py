from typing import Optional # Add this import
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    BOT_POOL_TOKENS: str = ""
    SESSION_STRING: str = ""
    MONGO_URI: str
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    STORAGE_CHANNEL_ID: int | None = None
    STORAGE_CHANNEL_USERNAME: str = ""
    STORAGE_CHANNEL_INVITE: str = ""
    STORAGE_CHANNEL_TITLE: str = ""
    BOT_USERNAME: str = ""

    # Cache / speed settings
    CACHE_ENABLED: bool = True
    CACHE_DIR: str = ""
    CACHE_MAX_GB: int = 20
    CACHE_HLS: bool = True
    CACHE_PARALLEL_CHUNKS: bool = True
    CACHE_MAX_WORKERS: int = 2
    CACHE_CHUNK_MB: int = 4
    CACHE_WARM_DELAY_SEC: int = 6
    
    # CHANGED: Replaced ADMIN_EMAIL with ADMIN_PHONE
    ADMIN_PHONE: str 

    class Config:
        env_file = ".env"

settings = Settings()
