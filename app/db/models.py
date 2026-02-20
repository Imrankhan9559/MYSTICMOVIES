import os
from typing import Optional, List, Dict, Any
from beanie import Document, init_beanie
from pydantic import BaseModel, Field, ConfigDict
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from app.core.config import settings

class User(Document):
    phone_number: str = Field(unique=True)
    session_string: str
    first_name: Optional[str] = None
    telegram_user_id: Optional[int] = None
    role: str = "user"  # user | admin
    role_requested: Optional[str] = None
    status: str = "approved"  # pending | approved | blocked
    requested_name: Optional[str] = None
    requested_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "users"
        indexes = [
            [("status", 1), ("requested_at", -1)],
            [("status", 1), ("created_at", -1)],
            [("role", 1), ("status", 1)],
        ]

class FilePart(BaseModel):
    telegram_file_id: str
    message_id: int  # <--- CRITICAL: Stores the message ID to refresh the link later
    chat_id: Optional[int | str] = None
    part_number: int
    size: int


class ContentFileRef(BaseModel):
    file_id: str
    name: str = ""
    quality: str = "HD"
    season: Optional[int] = None
    episode: Optional[int] = None
    episode_title: Optional[str] = None
    size: int = 0
    mime_type: Optional[str] = None


class FileSystemItem(Document):
    name: str
    is_folder: bool
    parent_id: Optional[str] = None
    owner_phone: str 
    created_at: datetime = datetime.now()
    source: str = "upload"  # upload | bot | admin
    catalog_status: str = "draft"  # draft | suggested | published | used
    catalog_type: Optional[str] = None  # movie | series
    title: Optional[str] = None
    series_title: Optional[str] = None
    year: Optional[str] = None
    quality: Optional[str] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    episode_title: Optional[str] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    description: Optional[str] = None
    release_date: Optional[str] = None
    genres: List[str] = []
    actors: List[str] = []
    director: Optional[str] = None
    trailer_url: Optional[str] = None
    trailer_key: Optional[str] = None
    cast_profiles: List[dict] = []
    tmdb_id: Optional[int] = None
    
    share_token: Optional[str] = None
    collaborators: List[str] = [] 
    
    size: int = 0
    mime_type: Optional[str] = None
    parts: List[FilePart] = [] 
    
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "filesystem"
        indexes = [
            [("parent_id", 1), ("is_folder", 1), ("name", 1)],
            [("owner_phone", 1), ("parent_id", 1), ("is_folder", 1)],
            [("source", 1), ("catalog_status", 1), ("created_at", -1)],
            [("catalog_status", 1), ("catalog_type", 1), ("title", 1), ("year", 1)],
            [("catalog_status", 1), ("catalog_type", 1), ("series_title", 1), ("season", 1), ("episode", 1)],
            [("share_token", 1)],
            [("name", 1)],
        ]


class ContentItem(Document):
    slug: str
    title: str
    search_title: str = ""
    content_type: str = "movie"  # movie | series
    status: str = "published"    # published | archived

    year: Optional[str] = None
    release_date: Optional[str] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    description: Optional[str] = None
    genres: List[str] = []
    actors: List[str] = []
    director: Optional[str] = None
    trailer_url: Optional[str] = None
    trailer_key: Optional[str] = None
    cast_profiles: List[dict] = []
    tmdb_id: Optional[int] = None

    owner_phone: str = ""
    collaborators: List[str] = []
    file_ids: List[str] = []
    files: List[ContentFileRef] = []
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()

    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "content"
        indexes = [
            [("status", 1), ("updated_at", -1)],
            [("status", 1), ("search_title", 1)],
            [("slug", 1), ("content_type", 1)],
            [("owner_phone", 1), ("status", 1)],
            [("status", 1), ("owner_phone", 1), ("updated_at", -1)],
            [("status", 1), ("collaborators", 1), ("updated_at", -1)],
            [("status", 1), ("content_type", 1), ("release_date", -1)],
            [("status", 1), ("title", 1)],
            [("status", 1), ("content_type", 1), ("title", 1), ("year", 1)],
            [("file_ids", 1)],
            [("files.file_id", 1)],
            [("tmdb_id", 1)],
        ]


class SharedCollection(Document):
    token: str = Field(unique=True)
    item_ids: List[str]
    owner_phone: str
    name: Optional[str] = "Shared Bundle"
    created_at: datetime = datetime.now()
    class Settings:
        name = "shared_collections"
        indexes = [
            [("owner_phone", 1), ("created_at", -1)],
        ]

class TokenSetting(Document):
    key: str = Field(unique=True)
    value: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    class Settings:
        name = "token_settings"

class PlaybackProgress(Document):
    user_key: str
    user_type: str  # public | user
    item_id: str
    collection_token: Optional[str] = None
    position: float = 0.0
    duration: float = 0.0
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "playback_progress"
        indexes = [
            [("user_type", 1), ("user_key", 1), ("updated_at", -1)],
            [("item_id", 1), ("updated_at", -1)],
            [("collection_token", 1), ("updated_at", -1)],
        ]


class WatchParty(Document):
    token: str
    room_code: Optional[str] = None
    host_name: str
    host_last_seen: datetime = datetime.now()
    item_id: Optional[str] = None
    position: float = 0.0
    is_playing: bool = True
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watch_parties"
        indexes = [
            [("token", 1)],
            [("room_code", 1)],
            [("updated_at", -1)],
        ]


class WatchPartyMember(Document):
    token: str
    user_name: str
    last_seen: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watch_party_members"
        indexes = [
            [("token", 1), ("last_seen", -1)],
        ]


class WatchPartyMessage(Document):
    token: str
    user_name: str
    text: str
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watch_party_messages"
        indexes = [
            [("token", 1), ("created_at", -1)],
        ]


class SiteSettings(Document):
    key: str = Field(unique=True)
    site_name: str = "mysticmovies"
    accent_color: str = "#facc15"
    bg_color: str = "#070b12"
    card_color: str = "#111827"
    hero_title: str = "Watch Movies & Series"
    hero_subtitle: str = "Stream, download, and send to Telegram in one place."
    hero_cta_link: str = "/content"
    hero_cta_text: str = "Browse Content"
    footer_text: str = "MysticMovies"
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "site_settings"


class HomeSlider(Document):
    title: str = ""
    subtitle: str = ""
    button_text: str = "Watch Now"
    link_url: str = "/content"
    image_url: str = ""
    content_slug: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "home_sliders"
        indexes = [
            [("is_active", 1), ("sort_order", 1), ("created_at", -1)],
        ]


class WatchlistEntry(Document):
    user_phone: str
    item_id: str
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watchlist_entries"
        indexes = [
            [("user_phone", 1), ("created_at", -1)],
            [("user_phone", 1), ("item_id", 1)],
        ]


class ContentRequest(Document):
    user_phone: str
    user_name: Optional[str] = None
    title: str
    request_type: str = "movie"  # movie | series
    note: Optional[str] = None
    status: str = "pending"      # pending | fulfilled | rejected
    fulfilled_content_id: Optional[str] = None
    fulfilled_content_title: Optional[str] = None
    fulfilled_content_type: Optional[str] = None
    fulfilled_content_path: Optional[str] = None
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "content_requests"
        indexes = [
            [("status", 1), ("updated_at", -1)],
            [("user_phone", 1), ("created_at", -1)],
        ]


class UserActivityEvent(Document):
    user_key: str = ""
    user_phone: Optional[str] = None
    user_name: Optional[str] = None
    user_type: str = "guest"  # user | public | guest
    action: str
    item_id: Optional[str] = None
    content_title: Optional[str] = None
    token: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "user_activity_events"
        indexes = [
            [("user_key", 1), ("created_at", -1)],
            [("action", 1), ("created_at", -1)],
            [("item_id", 1), ("created_at", -1)],
        ]


class AppSettings(Document):
    key: str = Field(unique=True)
    app_name: str = "MysticMovies Android"
    package_name: str = "com.mysticmovies.app"
    splash_image_url: str = ""
    loading_icon_url: str = ""
    onboarding_message: str = "Welcome to MysticMovies App"
    ads_message: str = ""
    update_popup_title: str = "Update Available"
    update_popup_body: str = "A new app version is available."
    latest_version: str = ""
    latest_build: int = 0
    latest_release_notes: str = ""
    latest_apk_item_id: Optional[str] = None
    latest_apk_share_token: Optional[str] = None
    latest_apk_size: int = 0
    recommended_update: bool = False
    force_update: bool = False
    min_supported_version: str = ""
    maintenance_mode: bool = False
    maintenance_message: str = ""
    push_enabled: bool = True
    keepalive_on_launch: bool = True
    telegram_bot_username: str = ""
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "app_settings"


class AppRelease(Document):
    version: str = ""
    build_number: int = 0
    release_notes: str = ""
    apk_item_id: Optional[str] = None
    apk_share_token: Optional[str] = None
    apk_size: int = 0
    update_mode: str = "none"  # none | recommended | forced
    is_active: bool = True
    created_by: Optional[str] = None
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "app_releases"
        indexes = [
            [("is_active", 1), ("build_number", -1)],
            [("update_mode", 1), ("created_at", -1)],
        ]


class AppBroadcast(Document):
    title: str = ""
    message: str = ""
    type: str = "news"  # news | ad | feature | maintenance
    is_active: bool = True
    created_by: Optional[str] = None
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "app_broadcasts"
        indexes = [
            [("is_active", 1), ("created_at", -1)],
            [("type", 1), ("created_at", -1)],
        ]


class AppDeviceSession(Document):
    device_id: str
    platform: str = "android"
    app_version: str = ""
    build_number: int = 0
    user_phone: Optional[str] = None
    user_name: Optional[str] = None
    handshake_token: str = ""
    handshake_expire_at: Optional[datetime] = None
    last_ping_at: datetime = datetime.now()
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "app_devices"
        indexes = [
            [("device_id", 1), ("updated_at", -1)],
            [("handshake_token", 1)],
            [("last_ping_at", -1)],
            [("user_phone", 1), ("updated_at", -1)],
        ]


class MassContentState(Document):
    key: str = Field(unique=True)
    title: str
    normalized_title: str
    content_type: str = "movie"  # movie | series
    year: Optional[str] = None

    panel: str = "processing"  # processing | tmdb_not_found | file_not_found | incomplete | complete
    tmdb_status: str = "pending"  # pending | found | not_found
    file_status: str = "pending"  # pending | missing | incomplete | complete
    upload_ready: bool = False
    uploaded: bool = False
    uploaded_at: Optional[datetime] = None

    source_inputs: List[str] = []
    tmdb_id: Optional[int] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    description: Optional[str] = None
    release_date: Optional[str] = None
    genres: List[str] = []
    actors: List[str] = []
    director: Optional[str] = None
    trailer_url: Optional[str] = None
    trailer_key: Optional[str] = None
    cast_profiles: List[dict] = []

    seasons: List[Dict[str, Any]] = []
    matched_files: List[Dict[str, Any]] = []
    live_notes: List[Dict[str, Any]] = []
    missing_items: List[Dict[str, Any]] = []
    upload_plan: List[Dict[str, Any]] = []
    last_error: Optional[str] = None

    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')

    class Settings:
        name = "mass_content_states"
        indexes = [
            [("updated_at", -1)],
            [("panel", 1), ("updated_at", -1)],
            [("upload_state", 1), ("updated_at", -1)],
            [("uploaded", 1), ("uploaded_at", -1)],
            [("tmdb_status", 1), ("updated_at", -1)],
            [("normalized_title", 1), ("content_type", 1)],
        ]

async def init_db():
    client = AsyncIOMotorClient(
        settings.MONGO_URI,
        serverSelectionTimeoutMS=int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000")),
        connectTimeoutMS=int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "10000")),
        socketTimeoutMS=int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "45000")),
        maxPoolSize=int(os.getenv("MONGO_MAX_POOL_SIZE", "80")),
        minPoolSize=int(os.getenv("MONGO_MIN_POOL_SIZE", "5")),
        retryWrites=True,
    )
    await init_beanie(
        database=client.morgan_db,
        document_models=[
            User,
            FileSystemItem,
            ContentItem,
            SharedCollection,
            TokenSetting,
            PlaybackProgress,
            WatchParty,
            WatchPartyMember,
            WatchPartyMessage,
            SiteSettings,
            HomeSlider,
            WatchlistEntry,
            ContentRequest,
            UserActivityEvent,
            AppSettings,
            AppRelease,
            AppBroadcast,
            AppDeviceSession,
            MassContentState,
        ],
    )
