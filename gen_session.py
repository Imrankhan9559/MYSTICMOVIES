# gen_session.py
from pyrogram import Client
import asyncio
import os

# !!! IMPORTANT: REPLACE THESE WITH YOUR REAL VALUES !!!
# Get them from https://my.telegram.org
api_id = 29350587            # Change this to your API ID (Integer)
api_hash = "e01359937e4d33ddb00263167a55b9af"   # Change this to your API HASH (String)

async def create_session():
    # We changed ":memory:" to "temp_session" to fix the Windows error
    async with Client("temp_session", api_id=api_id, api_hash=api_hash) as app:
        print("\nProcessing...")
        session_str = await app.export_session_string()
        print("\n\nHERE IS YOUR SESSION STRING (COPY IT CAREFULLY):\n")
        print(session_str)
        print("\n\nKeep this string safe! Do not share it.")

if __name__ == "__main__":
    # Remove old session file if it exists to avoid conflicts
    if os.path.exists("temp_session.session"):
        os.remove("temp_session.session")

    # Fix for Windows Event Loop issues
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except AttributeError:
        pass
    
    asyncio.run(create_session())