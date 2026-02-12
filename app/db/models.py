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


class SharedCollection(Document):
    token: str = Field(unique=True)
    item_ids: List[str]
    owner_phone: str
    name: Optional[str] = "Shared Bundle"
    created_at: datetime = datetime.now()
    class Settings:
        name = "shared_collections"

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


class WatchPartyMember(Document):
    token: str
    user_name: str
    last_seen: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watch_party_members"


class WatchPartyMessage(Document):
    token: str
    user_name: str
    text: str
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watch_party_messages"


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


class WatchlistEntry(Document):
    user_phone: str
    item_id: str
    created_at: datetime = datetime.now()
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "watchlist_entries"


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

async def init_db():
    client = AsyncIOMotorClient(settings.MONGO_URI)
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
        ],
    )
