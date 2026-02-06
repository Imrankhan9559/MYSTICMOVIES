from datetime import datetime
import uuid
from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from app.db.models import User, FileSystemItem, PlaybackProgress, TokenSetting
from app.routes.dashboard import get_current_user, _cast_ids
from app.core.config import settings

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def _normalize_phone(phone: str) -> str:
    return phone.replace(" ", "")

def _is_admin(user: User | None) -> bool:
    if not user: return False
    return _normalize_phone(user.phone_number) == _normalize_phone(getattr(settings, "ADMIN_PHONE", ""))

@router.get("/admin")
async def admin_panel(request: Request):
    user = await get_current_user(request)
    if not user: return RedirectResponse("/login")
    
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Not authorized.")

    total_users = await User.count()
    total_files = await FileSystemItem.find(FileSystemItem.is_folder == False).count()
    all_users = await User.find_all().to_list()
    pending_users = await User.find(User.status == "pending").sort("-requested_at").to_list()

    recent_progress = await PlaybackProgress.find_all().sort("-updated_at").limit(50).to_list()
    # Map item_id -> name for display
    item_ids = list({p.item_id for p in recent_progress if getattr(p, "item_id", None)})
    items_map = {}
    if item_ids:
        items = await FileSystemItem.find(In(FileSystemItem.id, _cast_ids(item_ids))).to_list()
        items_map = {str(i.id): i.name for i in items}
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    link_token = token_doc.value if token_doc else ""

    return templates.TemplateResponse("admin.html", {
        "request": request, "total_users": total_users, "total_files": total_files, 
        "users": all_users, "user_email": user.phone_number, "pending_users": pending_users,
        "recent_progress": recent_progress, "is_admin": True, "user": user, "link_token": link_token, "items_map": items_map
    })

@router.post("/admin/token/regenerate")
async def regenerate_link_token(request: Request):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    token_doc = await TokenSetting.find_one(TokenSetting.key == "link_token")
    new_val = str(uuid.uuid4())
    if token_doc:
        token_doc.value = new_val
        token_doc.updated_at = datetime.now()
        await token_doc.save()
    else:
        token_doc = TokenSetting(key="link_token", value=new_val)
        await token_doc.insert()
    return RedirectResponse("/admin", status_code=303)

@router.post("/admin/delete_user")
async def delete_user(request: Request, user_phone: str = Form(...)):
    """Deletes a user from the DB"""
    user = await get_current_user(request)
    # Re-verify admin
    if not _is_admin(user):
        raise HTTPException(403)
    
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        await target.delete()
        # Optional: Delete their files too
        await FileSystemItem.find(FileSystemItem.owner_phone == user_phone).delete()
    
    return RedirectResponse("/admin", status_code=303)

@router.post("/admin/approve_user")
async def approve_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "approved"
        target.approved_at = datetime.now()
        await target.save()
    return RedirectResponse("/admin", status_code=303)

@router.post("/admin/block_user")
async def block_user(request: Request, user_phone: str = Form(...)):
    user = await get_current_user(request)
    if not _is_admin(user):
        raise HTTPException(403)
    target = await User.find_one(User.phone_number == user_phone)
    if target:
        target.status = "blocked"
        await target.save()
    return RedirectResponse("/admin", status_code=303)
