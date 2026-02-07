from typing import Optional, List
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

class FileSystemItem(Document):
    name: str
    is_folder: bool
    parent_id: Optional[str] = None
    owner_phone: str 
    created_at: datetime = datetime.now()
    source: str = "upload"  # upload | bot | admin
    
    share_token: Optional[str] = None
    collaborators: List[str] = [] 
    
    size: int = 0
    mime_type: Optional[str] = None
    parts: List[FilePart] = [] 
    
    model_config = ConfigDict(extra='allow')
    class Settings:
        name = "filesystem"

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

async def init_db():
    client = AsyncIOMotorClient(settings.MONGO_URI)
    await init_beanie(database=client.morgan_db, document_models=[User, FileSystemItem, SharedCollection, TokenSetting, PlaybackProgress, WatchParty, WatchPartyMember, WatchPartyMessage])
