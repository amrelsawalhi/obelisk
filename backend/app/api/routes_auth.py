import hmac
import os
import threading
import time
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Depends, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.user import User
from app.core.password import hash_password, verify_password
# The token primitive and cookie name live in core/security.py so the websocket
# and dependency layers can use them without importing this router. Re-exported
# here because callers (and tests) have always imported them from this module.
from app.core.security import (  # noqa: F401
    COOKIE_NAME,
    DEFAULT_EXPIRATION_SECONDS,
    auth_enabled,
    create_token,
    verify_token,
)

router = APIRouter(prefix="/api/auth", tags=["Auth"])


# Request / Response Schemas
class SetupRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Initial admin username")
    password: str = Field(..., min_length=8, max_length=128, description="Initial admin password (min 8 chars)")


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


# Per-address login throttling. In-memory is enough for a single-node install: the
# state is only meant to slow down password spraying, not to survive a restart.
_LOGIN_ATTEMPTS_LOCK = threading.Lock()
_LOGIN_ATTEMPTS: Dict[str, list[float]] = {}
_LOGIN_LOCKED_UNTIL: Dict[str, float] = {}


def _login_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _prune_login_attempts(window: float = 600.0) -> None:
    cutoff = time.time() - window
    with _LOGIN_ATTEMPTS_LOCK:
        for key in list(_LOGIN_ATTEMPTS):
            _LOGIN_ATTEMPTS[key] = [stamp for stamp in _LOGIN_ATTEMPTS[key] if stamp > cutoff]
            if not _LOGIN_ATTEMPTS[key]:
                _LOGIN_ATTEMPTS.pop(key, None)
        for key in list(_LOGIN_LOCKED_UNTIL):
            if _LOGIN_LOCKED_UNTIL[key] < time.time():
                _LOGIN_LOCKED_UNTIL.pop(key, None)


async def _resolve_session(db: Any):
    if db is not None and hasattr(db, "execute"):
        return db, False
    from app.database import AsyncSessionLocal
    return AsyncSessionLocal(), True


@router.post("/setup", response_model=LoginResponse)
async def initial_setup(
    req: SetupRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Initial admin setup. Allowed ONLY when the users table is completely empty."""
    response.headers["Cache-Control"] = "no-store"

    session, must_close = await _resolve_session(db)
    try:
        user_count = await session.scalar(select(func.count()).select_from(User))
        if user_count and user_count > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Initial setup has already been completed. Please sign in.",
            )

        clean_user = req.username.strip()
        if not clean_user or len(clean_user) < 3:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username must be at least 3 characters long.",
            )
        if len(req.password) < 8:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must be at least 8 characters long.",
            )

        user = User(
            username=clean_user,
            password_hash=hash_password(req.password),
            role="admin",
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
    finally:
        if must_close:
            await session.close()

    token = create_token(
        {"sub": user.username, "role": user.role},
        expires_seconds=int(settings.SESSION_TTL_SECONDS),
    )

    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=int(settings.SESSION_TTL_SECONDS),
        httponly=True,
        samesite="lax",
        secure=bool(settings.SESSION_SECURE_COOKIE),
        path="/",
    )

    return LoginResponse(
        access_token=token,
        token_type="bearer",
        user=UserSession(username=user.username, role=user.role, authenticated=True),
    )


@router.post("/login", response_model=LoginResponse)
async def login(
    req: LoginRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Authenticates user against the database user store and issues signed session."""
    response.headers["Cache-Control"] = "no-store"
    key = _login_key(request)
    max_attempts = max(1, int(settings.AUTH_MAX_FAILED_ATTEMPTS))
    lockout_seconds = max(1, int(settings.AUTH_LOCKOUT_SECONDS))

    _prune_login_attempts()
    with _LOGIN_ATTEMPTS_LOCK:
        locked_until = _LOGIN_LOCKED_UNTIL.get(key, 0.0)
    if locked_until > time.time():
        wait = max(1, int(locked_until - time.time()))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed attempts. Try again in {wait} seconds.",
            headers={"Retry-After": str(wait)},
        )

    # 1. Lookup user in database
    session, must_close = await _resolve_session(db)
    try:
        stmt = select(User).where(User.username == req.username.strip())
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
    finally:
        if must_close:
            await session.close()

    authenticated = False
    user_role = "admin"
    username = req.username.strip()

    if user:
        authenticated = verify_password(req.password, user.password_hash)
        user_role = user.role
        username = user.username
    elif (
        # Fallback for headless environments or before setup if matching configured env credentials
        (settings.ALLOW_DEFAULT_CREDENTIALS or settings.OBELISK_ADMIN_PASSWORD not in ("admin", "obelisk2026"))
        and settings.OBELISK_ADMIN_USER
        and settings.OBELISK_ADMIN_PASSWORD
        and hmac.compare_digest(req.username.encode("utf-8"), settings.OBELISK_ADMIN_USER.encode("utf-8"))
        and hmac.compare_digest(req.password.encode("utf-8"), settings.OBELISK_ADMIN_PASSWORD.encode("utf-8"))
    ):
        authenticated = True
        user_role = "admin"
        username = settings.OBELISK_ADMIN_USER

    if not authenticated:
        with _LOGIN_ATTEMPTS_LOCK:
            stamps = _LOGIN_ATTEMPTS.setdefault(key, [])
            stamps.append(time.time())
            if len(stamps) >= max_attempts:
                _LOGIN_LOCKED_UNTIL[key] = time.time() + lockout_seconds
                _LOGIN_ATTEMPTS.pop(key, None)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials. Access denied.",
        )

    with _LOGIN_ATTEMPTS_LOCK:
        _LOGIN_ATTEMPTS.pop(key, None)
        _LOGIN_LOCKED_UNTIL.pop(key, None)

    token = create_token(
        {"sub": username, "role": user_role},
        expires_seconds=int(settings.SESSION_TTL_SECONDS),
    )

    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=int(settings.SESSION_TTL_SECONDS),
        httponly=True,
        samesite="lax",
        secure=bool(settings.SESSION_SECURE_COOKIE),
        path="/",
    )

    return LoginResponse(
        access_token=token,
        token_type="bearer",
        user=UserSession(username=username, role=user_role, authenticated=True),
    )


@router.post("/logout")
async def logout(response: Response):
    """Clears HttpOnly session cookie."""
    response.headers["Cache-Control"] = "no-store"
    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"status": "logged_out"}


@router.get("/me", response_model=UserSession)
async def get_session(request: Request, response: Response):
    """Checks current session validity."""
    response.headers["Cache-Control"] = "no-store"
    try:
        user_data = get_current_user(request)
        return UserSession(
            username=user_data.get("sub", "admin"),
            role=user_data.get("role", "admin"),
            authenticated=True,
        )
    except HTTPException:
        return UserSession(username="anonymous", role="guest", authenticated=False)


@router.get("/status")
async def auth_status(db: AsyncSession = Depends(get_db)):
    """Public: reports authentication status and whether initial setup is required."""
    session, must_close = await _resolve_session(db)
    try:
        user_count = await session.scalar(select(func.count()).select_from(User))
    except Exception:
        user_count = 0
    finally:
        if must_close:
            await session.close()

    return {
        "auth_enabled": auth_enabled(),
        "needs_setup": (user_count or 0) == 0,
        "session_ttl_seconds": int(
            getattr(settings, "SESSION_TTL_SECONDS", DEFAULT_EXPIRATION_SECONDS)
        ),
        "terminal_enabled": bool(getattr(settings, "TERMINAL_ENABLED", False)),
    }


@router.get("/verify")
async def verify_session(request: Request):
    """For reverse proxies (Caddy ``forward_auth``): 204 only when the session is valid."""
    if not auth_enabled():
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"X-Obelisk-User": "local", "X-Obelisk-Role": "admin"},
        )
    try:
        user_data = get_current_user(request)
    except HTTPException:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )
    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers={
            "X-Obelisk-User": str(user_data.get("sub", "")),
            "X-Obelisk-Role": str(user_data.get("role", "")),
            "Cache-Control": "no-store",
        },
    )
