# MYSTICMOVIES Agent Notes

These are project-specific guidelines for working in this repo.

## Architecture
- FastAPI app in `main.py` with Jinja templates in `app/templates`.
- MongoDB via Beanie (`app/db/models.py`).
- Telegram integration:
  - Pyrogram for bot/user clients (`app/core/telegram_bot.py`).
  - Telethon for storage channel operations and streaming (`app/core/telethon_storage.py`).
- Streaming endpoints live in `app/routes/stream.py` and `app/routes/share.py`.

## Environment
- `.env` is required. Key settings in `app/core/config.py`:
  - `API_ID`, `API_HASH`, `BOT_TOKEN`, `SESSION_STRING`, `MONGO_URI`, `ADMIN_PHONE`.
  - Storage channel: `STORAGE_CHANNEL_ID` or `STORAGE_CHANNEL_USERNAME`. If unset, uses `me`.
- Start server: `python main.py` (runs `uvicorn` with reload).

## Data Model Notes
- `FileSystemItem` represents both files and folders.
- `FilePart` stores Telegram message/file IDs (`message_id`, `telegram_file_id`, `chat_id`).
- When querying by ids from the UI, cast strings to `PydanticObjectId` before `In(...)`.

## Performance Rules
- Avoid loading all items into Python for search or listing.
- Use DB queries with filters, `skip`, and `limit`.
- Large folders should be paged; `ITEMS_PAGE_SIZE` in `app/routes/dashboard.py`.
- Storage sync is expensive; do not trigger it on every folder open.

## File Operations
- Rename:
  - Fast rename = DB-only (no Telegram reupload).
  - Storage rename = reupload + delete old message (slow).
- Move/Copy:
  - Single-item routes: `/item/move`, `/item/copy`.
  - Multi-select routes: `/item/move/bundle`, `/item/copy/bundle`.

## UI/Template Notes
- Main UI: `app/templates/dashboard.html`.
- Search supports scope (`all` vs `folder`) and suggestions.
- Folder explorer modal uses `/folders/list` and `/folder/create_json`.

## Sharing
- Share routes in `app/routes/share.py`:
  - `/s/{token}` view, `/d/{token}` download, `/t/{token}` Telegram deep link.
- Bundles and folders expand to underlying files server-side.

## Common Pitfalls
- Telegram storage operations require valid `chat_id` access.
- When copying/moving selected folders, skip descendants to avoid duplicates.
- Always normalize `chat_id` with `normalize_chat_id`.
