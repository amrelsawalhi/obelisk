import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Depends, Request, Response, status
from pydantic import BaseModel, Field

from app.config import settings
# The token primitive and cookie name live in core/security.py so the websocket
# and dependency layers can use them without importing this router. Re-exported
# here because callers (and tests) have always imported them from this module.
from app.core.security import (  # noqa: F401
    COOKIE_NAME,
    DEFAULT_EXPIRATION_SECONDS,
    create_token,
    verify_token,
)

router = APIRouter(prefix="/api/auth", tags=["Auth"])

# Credentials are read from settings per request, so `.env` counts and a changed value
# takes effect without a reimport. Module-level constants used to freeze `os.environ` at
# import time, which silently ignored `.env` entirely.


# Request / Response Schemas
class LoginRequest(BaseModel):
    username: str = Field(..., description="Admin username")
    password: str = Field(..., description="Admin password")


class UserSession(BaseModel):
    username: str
    role: str
    authenticated: bool


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserSession


def get_current_user(request: Request) -> Dict[str, Any]:
    """Extracts session from HttpOnly cookie or Authorization Bearer header."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ", 1)[1]

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required."
        )

    return verify_token(token)


@router.post("/login", response_model=LoginResponse)
async def login(req: LoginRequest, response: Response):
    """
    Authenticates user and issues signed JWT in HttpOnly cookie and JSON body.
    """
    if (
        req.username != settings.OBELISK_ADMIN_USER
        or req.password != settings.OBELISK_ADMIN_PASSWORD
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials. Access denied."
        )

    token = create_token({"sub": req.username, "role": "admin"})

    # Set secure HttpOnly cookie for browser sessions
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=DEFAULT_EXPIRATION_SECONDS,
        httponly=True,
        samesite="lax",
        secure=False, # Set to False to permit LAN HTTP access on internal network
        path="/",
    )

    return LoginResponse(
        access_token=token,
        token_type="bearer",
        user=UserSession(username=req.username, role="admin", authenticated=True),
    )


@router.post("/logout")
async def logout(response: Response):
    """Clears HttpOnly session cookie."""
    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"status": "logged_out"}


@router.get("/me", response_model=UserSession)
async def get_session(request: Request):
    """Checks current session validity."""
    try:
        user_data = get_current_user(request)
        return UserSession(
            username=user_data.get("sub", "admin"),
            role=user_data.get("role", "admin"),
            authenticated=True,
        )
    except HTTPException:
        return UserSession(username="anonymous", role="guest", authenticated=False)
