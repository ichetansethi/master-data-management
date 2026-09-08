from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db, get_current_user
from app.models.users import Users
from app.security import create_access_token, verify_password
from app.dependencies import require_role

router = APIRouter()

@router.get("/auth/me")
async def read_current_user(current_user: Users = Depends(get_current_user)):
    return {
        "id": str(current_user.id),
        "username": current_user.username,
        "email": current_user.email,
        "role_id": str(current_user.role_id),
    }

@router.post("/auth/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Users).where(Users.username == form_data.username))
    user = result.scalar_one_or_none()
    if user is None or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token({"sub": str(user.id)})
    return {"access_token": token, "token_type": "bearer"}

@router.get("/auth/admin-only")
async def admin_only_route(current_user: Users = Depends(require_role(["ADMIN"]))):
    return {"message": f"Welcome, {current_user.username} — you have ADMIN access."}