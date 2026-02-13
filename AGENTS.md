# MYSTICMOVIES Agent Notes

Project-specific operating notes for backend, web, Telegram, admin, and Android app work.

## 1) System Architecture
- Backend framework: FastAPI (`main.py`)
- Templating: Jinja2 (`app/templates`)
- Database: MongoDB + Beanie models (`app/db/models.py`)
- Telegram integrations:
  - Pyrogram bot/user pool: `app/core/telegram_bot.py`
  - Telethon storage operations: `app/core/telethon_storage.py`
- Streaming/share routes:
  - Internal player stream: `app/routes/stream.py`
  - Public share/view/download/watch-together: `app/routes/share.py`
- Content and catalog routes: `app/routes/content.py`
- Admin panel routes: `app/routes/admin.py`
- Native app API routes: `app/routes/app_client.py` with prefix `/app-api`

## 2) Run Environment
- Required `.env` keys are read via `app/core/config.py`
- Critical runtime keys:
  - `API_ID`, `API_HASH`, `BOT_TOKEN`, `SESSION_STRING`
  - `MONGO_URI`, `ADMIN_PHONE`
  - `STORAGE_CHANNEL_ID` or `STORAGE_CHANNEL_USERNAME` (fallback `me`)
- Start server:
  - `python main.py`
  - Runs uvicorn, uses `PORT` env if set

## 3) Core Data Collections
- `users`: user identity, role, status, approval
- `filesystem`: physical file/folder tree + Telegram message/file references
- `content`: normalized content docs (`ContentItem`) synced from published `filesystem` items
- `shared_collections`: share bundles
- `token_settings`: link token, handshake secret, etc.
- `playback_progress`: progress for public/user playback
- `watch_parties`, `watch_party_members`, `watch_party_messages`
- `site_settings`: header/footer + UI branding
- `home_sliders`: homepage slider entries
- `watchlist_entries`
- `content_requests`
- `user_activity_events`
- App control collections:
  - `app_settings`
  - `app_releases`
  - `app_broadcasts`
  - `app_devices`

## 4) Access and Roles
- Admin checks generally accept either:
  - `role == "admin"`, or
  - phone matches `ADMIN_PHONE`
- User status gating:
  - approved users should have `status == "approved"`

## 5) Content Pipeline
- Publish flow is built around `FileSystemItem.catalog_status == "published"` items.
- `sync_content_catalog()` in `app/core/content_store.py` builds/refreshes `ContentItem` docs.
- Catalog pages and app APIs derive cards/groups from published catalog data.

## 6) Public Share/Streaming Routes
- Main share routes in `app/routes/share.py`:
  - `/s/{token}`: shared view
  - `/d/{token}`: shared download
  - `/t/{token}`: Telegram redirect/deep flow
  - `/w/{token}`: watch-together
  - `/s/stream/{token}` and `/s/stream/file/{item_id}`: stream bytes
- Stream alignment and parallel logic in `app/routes/stream.py`

## 7) App API (`/app-api`) Contract
Implemented in `app/routes/app_client.py`.

- `POST /app-api/handshake`
  - Inputs: device/app metadata
  - Output: signed handshake token
- `GET /app-api/bootstrap`
  - Requires handshake token (`X-App-Handshake` or bearer/query)
  - Returns:
    - app settings (splash/loading/onboarding/ads/maintenance/push/keepalive)
    - update policy (recommended/forced, notes, APK URL)
    - active broadcasts
    - Telegram bot metadata
    - UI config from `site_settings` (header/footer/topbar/logo/menu)
    - endpoint hints
- `POST /app-api/ping`
  - Refreshes last-seen for app device session
- `GET /app-api/catalog`
  - Filters: `all|movies|series`
  - Supports query/sort/pagination
  - Returns cards + slider + pagination
- `GET /app-api/content/{content_key}`
  - Returns full content item details
  - Returns movie/series link objects with app-friendly fields:
    - `stream_url`, `download_url`, `telegram_start_url`, `watch_together_url`
- `GET /app-api/image?src=...`
  - Server-side image proxy for allowed hosts (stabilizes poster loading)
- `GET /app-api/telegram-start/{share_token}`
  - Returns Telegram deep link payload for app

## 8) Admin Panel Areas
Primary routes in `app/routes/admin.py`.

- Header/Footer management:
  - `/header-footer-settings`
  - `POST /admin/header-footer/save`
- Slider management:
  - `/manage-slider`
  - create/update/delete/reorder endpoints
- App management:
  - `/app-management`
  - `POST /admin/app-management/save`
  - `POST /admin/app-management/release`
  - `POST /admin/app-management/notify`
  - broadcast toggle/delete endpoints

### App Management Functions
- Core app config:
  - app name, package name
  - splash image URL, loading icon URL
  - onboarding message, ads message
  - update popup title/body
  - latest version/build/release notes
  - min supported version
  - recommended update / force update
  - maintenance mode/message
  - push enabled
  - keepalive on launch
  - telegram bot username
- APK release flow:
  - APK files are auto-normalized to `APPS` folder
  - latest APK mapping + share token support
  - release entries with update mode (`none|recommended|forced`)
- Broadcasts:
  - save ad/news/feature/maintenance messages
  - activate/deactivate/delete
- Device telemetry:
  - recent handshakes and last ping timestamps

## 9) Android App (`android-app`) Functions
Native Android app (not full website wrapper).

### Implemented Activities
- `MainActivity`
  - handshake + bootstrap + ping flow
  - dynamic topbar/header/footer text/logo from bootstrap UI config
  - update prompt handling (recommended/forced)
  - catalog list, filters, search, hero card
  - open detail page
  - open app downloads screen
- `ContentDetailActivity`
  - fetches `/app-api/content/{key}`
  - shows title/meta/description/poster/trailer
  - quality rows and season rows
  - buttons:
    - Watch: opens in native player
    - Download: uses `DownloadManager`, no browser redirect
    - Telegram: opens Telegram deep link flow
    - Watch Together: in-app web shell
- `PlayerActivity`
  - Media3 ExoPlayer playback for remote stream and local downloaded files
  - resume position persistence
- `DownloadsActivity`
  - lists files from app external downloads dir
  - one-tap play in native player
- `LoginActivity`
  - in-app WebView shell for `/login` and external web pages (trailer/watch-together/login flow)

### Android Runtime State
- Shared app state in `AppRuntime.kt` (`AppRuntimeState`)
  - API base URL
  - handshake token
  - app settings
  - UI config
  - update config
  - notifications/ads

### Android Networking
- `ApiHttp.kt`
  - OkHttp client with custom DNS fallback
  - base URL candidate strategy
  - URL helpers

## 10) Android Build and Release
- Open project folder: `android-app/`
- Debug build:
  - `.\gradlew.bat assembleDebug`
- Release signing:
  - copy `android-templates/key.properties.example` -> `android-app/key.properties`
  - configure keystore values
  - run `assembleRelease`

### OneDrive/Windows Lock Handling
- Use `android-app/fix-build-lock.ps1`
  - stops Gradle/Kotlin daemons
  - clears read-only attributes
  - retries deletion of locked `app/build`
  - rebuilds with `--no-daemon`
- `gradle.properties` tuned for lock-prone environments:
  - disabled VFS watch/caching/parallel
  - Kotlin incremental + classpath snapshot disabled

## 11) Performance Rules
- Do not load full folder trees into memory for every UI action.
- Use DB filtering + pagination (`skip`/`limit`) for large lists.
- Avoid unnecessary storage sync calls on high-frequency routes.
- Keep share and stream routes token-based and direct.

## 12) Common Pitfalls
- Always normalize `chat_id` with `normalize_chat_id`.
- Telegram storage access can fail if peer access/session is invalid.
- For ID filters in Beanie `In(...)`, cast to `PydanticObjectId` when needed.
- When processing selected folders, avoid duplicate descendant operations.
- On Windows + OneDrive, locked Kotlin snapshot/build folders are common; use lock script.

## 13) Practical Debug Checklist
- Catalog empty in app:
  - test `GET /app-api/catalog` directly
  - verify handshake/bootstrap success
  - verify `BuildConfig.API_BASE_URL`
- Posters broken:
  - check `/app-api/image?src=...`
  - verify allowed host and outbound connectivity
- Telegram button not opening:
  - verify `BOT_USERNAME` / app setting `telegram_bot_username`
  - test `/app-api/telegram-start/{share_token}`
- Download not visible:
  - check app external downloads directory in `DownloadsActivity`
- Update flow not triggering:
  - verify `app_settings` + `app_releases` flags/build numbers
