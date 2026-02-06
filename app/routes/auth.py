import traceback
from datetime import datetime
from fastapi import APIRouter, Request, Form, Response
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, RedirectResponse
from pyrogram import Client, errors
from app.core.config import settings
from app.db.models import User

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# In-memory storage for temporary login steps (Production apps should use Redis)
temp_auth_data = {} 

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

async def _check_login_allowed(phone: str):
    user = await User.find_one(User.phone_number == phone)
    if not user:
        return None, "Account not found. Request access first."
    if user.status == "pending":
        return None, "Your account is in process. Please wait for admin approval."
    if user.status == "blocked":
        return None, "Your account has been blocked. Contact admin."
    return user, None

@router.get("/login")
async def login_page(request: Request):
    """Renders the login page."""
    return templates.TemplateResponse("login.html", {"request": request, "step": "phone"})

@router.get("/register")
async def register_page(request: Request):
    """Renders the account request page."""
    return templates.TemplateResponse("register.html", {"request": request})

@router.get("/logout")
async def logout(response: Response):
    """Logs the user out by clearing the cookie."""
    response = RedirectResponse(url="/login")
    response.delete_cookie("user_phone")
    return response

@router.post("/auth/send_code")
async def send_code(phone: str = Form(...)):
    """Step 1: Connect to Telegram and send OTP."""
    try:
        phone = _normalize_phone(phone)
        _, error = await _check_login_allowed(phone)
        if error:
            return JSONResponse({"error": error}, status_code=400)

        # Create a temporary client just for this auth flow
        client = Client(
            name=f"auth_{phone}",
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            in_memory=True
        )
        await client.connect()
        
        # Send Code
        sent_code = await client.send_code(phone)
        
        # Store phone_code_hash temporarily
        temp_auth_data[phone] = {
            "phone_code_hash": sent_code.phone_code_hash,
            "client": client # Keep connection open
        }
        
        return JSONResponse({"status": "success", "message": "Code sent"})
        
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=400)

@router.post("/auth/verify_code")
async def verify_code(response: Response, phone: str = Form(...), code: str = Form(...)):
    """Step 2: Verify OTP and Login."""
    phone = _normalize_phone(phone)
    _, error = await _check_login_allowed(phone)
    if error:
        return JSONResponse({"error": error}, status_code=400)

    if phone not in temp_auth_data:
        return JSONResponse({"error": "Session expired. Try again."}, status_code=400)
    
    data = temp_auth_data[phone]
    client = data["client"]
    phone_code_hash = data["phone_code_hash"]

    try:
        # Attempt Sign In
        user_info = await client.sign_in(phone, phone_code_hash, code)
        
        # If successful, export session string
        session_string = await client.export_session_string()
        await client.disconnect()
        del temp_auth_data[phone] # Cleanup

        # Save/Update User in DB
        await save_user_to_db(phone, session_string, user_info)

        # --- SET COOKIE (IFRAME COMPATIBLE) ---
        response = JSONResponse({"status": "success"})
        # Lax keeps user logged in on normal navigation while still protecting cross-site.
        response.set_cookie(
            key="user_phone",
            value=phone,
            httponly=True,
            samesite='lax',
            secure=True
        )
        return response

    except errors.SessionPasswordNeeded:
        # 2FA Required
        return JSONResponse({"status": "2fa_required"})
        
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@router.post("/auth/verify_password")
async def verify_password(response: Response, phone: str = Form(...), password: str = Form(...)):
    """Step 3 (Optional): Verify 2FA Password."""
    phone = _normalize_phone(phone)
    _, error = await _check_login_allowed(phone)
    if error:
        return JSONResponse({"error": error}, status_code=400)

    if phone not in temp_auth_data:
        return JSONResponse({"error": "Session expired."}, status_code=400)

    data = temp_auth_data[phone]
    client = data["client"]

    try:
        user_info = await client.check_password(password)
        
        session_string = await client.export_session_string()
        await client.disconnect()
        del temp_auth_data[phone]

        # Save/Update User
        await save_user_to_db(phone, session_string, user_info)

        # --- SET COOKIE (IFRAME COMPATIBLE) ---
        response = JSONResponse({"status": "success"})
        response.set_cookie(
            key="user_phone",
            value=phone,
            httponly=True,
            samesite='lax',
            secure=True
        )
        return response

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@router.post("/register/send_code")
async def register_send_code(name: str = Form(...), phone: str = Form(...)):
    """Request access: Step 1: send OTP."""
    try:
        phone = _normalize_phone(phone)
        existing = await User.find_one(User.phone_number == phone)
        if existing:
            if existing.status == "approved":
                return JSONResponse({"error": "Account already exists. Please login."}, status_code=400)
            if existing.status == "pending":
                return JSONResponse({"error": "Your account is already in process. Please wait for admin approval."}, status_code=400)
            if existing.status == "blocked":
                return JSONResponse({"error": "Your account has been blocked. Contact admin."}, status_code=400)

        client = Client(
            name=f"register_{phone}",
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            in_memory=True
        )
        await client.connect()
        sent_code = await client.send_code(phone)

        temp_auth_data[phone] = {
            "phone_code_hash": sent_code.phone_code_hash,
            "client": client,
            "requested_name": name.strip()
        }

        return JSONResponse({"status": "success", "message": "Code sent"})
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=400)

@router.post("/register/verify_code")
async def register_verify_code(phone: str = Form(...), code: str = Form(...)):
    """Request access: Step 2: verify OTP."""
    phone = _normalize_phone(phone)
    if phone not in temp_auth_data:
        return JSONResponse({"error": "Session expired. Try again."}, status_code=400)

    data = temp_auth_data[phone]
    client = data["client"]
    phone_code_hash = data["phone_code_hash"]
    requested_name = data.get("requested_name") or "User"

    try:
        user_info = await client.sign_in(phone, phone_code_hash, code)
        session_string = await client.export_session_string()
        await client.disconnect()
        del temp_auth_data[phone]

        await save_pending_user(phone, session_string, user_info, requested_name)
        return JSONResponse({"status": "pending", "message": "Verified. Waiting for admin approval."})

    except errors.SessionPasswordNeeded:
        return JSONResponse({"status": "2fa_required"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@router.post("/register/verify_password")
async def register_verify_password(phone: str = Form(...), password: str = Form(...)):
    """Request access: Step 3: verify 2FA."""
    phone = _normalize_phone(phone)
    if phone not in temp_auth_data:
        return JSONResponse({"error": "Session expired."}, status_code=400)

    data = temp_auth_data[phone]
    client = data["client"]
    requested_name = data.get("requested_name") or "User"

    try:
        user_info = await client.check_password(password)
        session_string = await client.export_session_string()
        await client.disconnect()
        del temp_auth_data[phone]

        await save_pending_user(phone, session_string, user_info, requested_name)
        return JSONResponse({"status": "pending", "message": "Verified. Waiting for admin approval."})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

async def save_user_to_db(phone, session_string, user_info):
    """Helper to save approved user data to MongoDB."""
    existing_user = await User.find_one(User.phone_number == phone)
    if not existing_user:
        raise ValueError("Account not found. Request access first.")
    if existing_user.status != "approved":
        raise ValueError("Account pending approval.")

    first_name = user_info.first_name if hasattr(user_info, 'first_name') else "User"
    existing_user.session_string = session_string
    existing_user.first_name = first_name
    existing_user.telegram_user_id = getattr(user_info, "id", None)
    await existing_user.save()

async def save_pending_user(phone, session_string, user_info, requested_name: str):
    """Save account request as pending."""
    existing_user = await User.find_one(User.phone_number == phone)
    first_name = user_info.first_name if hasattr(user_info, 'first_name') else "User"
    now = datetime.now()
    if existing_user:
        existing_user.session_string = session_string
        existing_user.first_name = first_name
        existing_user.telegram_user_id = getattr(user_info, "id", None)
        existing_user.status = "pending"
        existing_user.requested_name = requested_name
        existing_user.requested_at = now
        await existing_user.save()
    else:
        new_user = User(
            phone_number=phone,
            session_string=session_string,
            first_name=first_name,
            telegram_user_id=getattr(user_info, "id", None),
            status="pending",
            requested_name=requested_name,
            requested_at=now
        )
        await new_user.insert()
