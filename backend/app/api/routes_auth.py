import hmac
import hashlib
import json
import base64
import time
import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Depends, Request, Response, status
from pydantic import BaseModel, Field

from app.config import settings

router = APIRouter(prefix="/api/auth", tags=["Auth"])

# Session Configuration
COOKIE_NAME = "obelisk_session"
DEFAULT_EXPIRATION_SECONDS = 86400 * 7 # 7 days
ADMIN_USERNAME = os.environ.get("OBELISK_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("OBELISK_ADMIN_PASSWORD", "obelisk2026")


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64_decode(data_str: str) -> bytes:
    padding = "=" * (4 - (len(data_str) % 4)) if len(data_str) % 4 != 0 else ""
    return base64.urlsafe_b64decode(data_str + padding)


def create_token(payload: Dict[str, Any], expires_seconds: int = DEFAULT_EXPIRATION_SECONDS) -> str:
    """Signs a standard HS256 JWT using settings.OBELISK_MASTER_KEY."""
    header = {"alg": "HS256", "typ": "JWT"}
    payload_copy = payload.copy()
    now = int(time.time())
    payload_copy["iat"] = now
    payload_copy["exp"] = now + expires_seconds

    header_b64 = _b64_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64_encode(json.dumps(payload_copy, separators=(",", ":")).encode("utf-8"))

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    secret_bytes = settings.OBELISK_MASTER_KEY.encode("utf-8")
    signature = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
    sig_b64 = _b64_encode(signature)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


def verify_token(token: str) -> Dict[str, Any]:
    """Validates signature and expiration of an HS256 JWT."""
    parts = token.strip().split(".")
    if len(parts) != 3:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed session token."
        )

    header_b64, payload_b64, sig_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    secret_bytes = settings.OBELISK_MASTER_KEY.encode("utf-8")
    expected_sig = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
    actual_sig = _b64_decode(sig_b64)

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid session token signature."
        )

    try:
        payload = json.loads(_b64_decode(payload_b64).decode("utf-8"))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid session payload."
        )

    now = int(time.time())
    if payload.get("exp", 0) < now:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session has expired."
        )

    return payload


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
    if req.username != ADMIN_USERNAME or req.password != ADMIN_PASSWORD:
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
