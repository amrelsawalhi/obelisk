"""
Variables & Secrets API Routes:
Provides persistent key-value configuration and AES-256 encrypted secrets
for Marimo notebooks, dbt pipelines, and ingestion workflows.
"""

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.variable import Variable
from app.core.vault import vault

router = APIRouter(prefix="/api/variables", tags=["Variables"])


class CreateVariableRequest(BaseModel):
    key: str = Field(..., description="Unique alphanumeric identifier (e.g. SLACK_WEBHOOK_URL)")
    value: str = Field(..., description="Plaintext or secret string value")
    is_secret: bool = Field(default=False, description="If True, encrypted with AES-256 Fernet vault")
    description: Optional[str] = Field(default="", description="Optional description of usage")


class UpdateVariableRequest(BaseModel):
    value: Optional[str] = None
    is_secret: Optional[bool] = None
    description: Optional[str] = None


@router.get("")
async def list_variables():
    """
    Returns all registered variables.
    Secret variables have their values masked as '••••••••••••' for security.
    Auto-seeds default enterprise variables if empty.
    """
    async with AsyncSessionLocal() as session:
        stmt = select(Variable).order_by(Variable.key.asc())
        res = await session.execute(stmt)
        vars_list = res.scalars().all()

        if not vars_list:
            default_seeds = [
                Variable(
                    key="ENV",
                    value="development",
                    is_secret=False,
                    description="Active runtime environment mode (development / staging / production)",
                ),
                Variable(
                    key="DEFAULT_CURRENCY",
                    value="USD",
                    is_secret=False,
                    description="Base currency for analytical financial data marts",
                ),
                Variable(
                    key="BATCH_SIZE",
                    value="1000",
                    is_secret=False,
                    description="Default batch chunk size for pipeline data extraction",
                ),
            ]
            for v in default_seeds:
                session.add(v)
            await session.commit()
            stmt = select(Variable).order_by(Variable.key.asc())
            res = await session.execute(stmt)
            vars_list = res.scalars().all()

        return [
            {
                "id": v.id,
                "key": v.key,
                "value": "••••••••••••" if v.is_secret else v.value,
                "is_secret": v.is_secret,
                "description": v.description,
                "created_at": v.created_at.isoformat() if v.created_at else None,
                "updated_at": v.updated_at.isoformat() if v.updated_at else None,
            }
            for v in vars_list
        ]


@router.post("")
async def set_variable(req: CreateVariableRequest):
    """
    Creates or updates a variable.
    If is_secret is True, encrypts the value using AES-256 Fernet vault.
    """
    clean_key = req.key.strip().upper().replace(" ", "_")
    if not clean_key:
        raise HTTPException(status_code=400, detail="Variable key cannot be empty")

    async with AsyncSessionLocal() as session:
        stmt = select(Variable).where(Variable.key == clean_key)
        res = await session.execute(stmt)
        existing = res.scalars().first()

        stored_value = vault.encrypt(req.value) if req.is_secret else req.value

        if existing:
            existing.value = stored_value
            existing.is_secret = req.is_secret
            existing.description = req.description or ""
            await session.commit()
            await session.refresh(existing)
            return {
                "id": existing.id,
                "key": existing.key,
                "value": "••••••••••••" if existing.is_secret else existing.value,
                "is_secret": existing.is_secret,
                "description": existing.description,
                "updated_at": existing.updated_at.isoformat() if existing.updated_at else None,
            }

        new_v = Variable(
            key=clean_key,
            value=stored_value,
            is_secret=req.is_secret,
            description=req.description or "",
        )
        session.add(new_v)
        await session.commit()
        await session.refresh(new_v)
        return {
            "id": new_v.id,
            "key": new_v.key,
            "value": "••••••••••••" if new_v.is_secret else new_v.value,
            "is_secret": new_v.is_secret,
            "description": new_v.description,
            "created_at": new_v.created_at.isoformat() if new_v.created_at else None,
        }


@router.get("/{key}/reveal")
async def reveal_variable(key: str):
    """Decrypts and returns the raw value of a secret variable for authorized UI inspection."""
    clean_key = key.strip().upper()
    async with AsyncSessionLocal() as session:
        stmt = select(Variable).where(Variable.key == clean_key)
        res = await session.execute(stmt)
        v = res.scalars().first()
        if not v:
            raise HTTPException(status_code=404, detail=f"Variable '{key}' not found")

        decrypted_val = vault.decrypt(v.value) if v.is_secret else v.value
        return {
            "key": v.key,
            "value": decrypted_val,
            "is_secret": v.is_secret,
        }


@router.get("/sdk/{key}")
async def get_sdk_variable(key: str):
    """Internal SDK endpoint: Returns raw decrypted value for notebook and pipeline runtime execution."""
    clean_key = key.strip().upper()
    async with AsyncSessionLocal() as session:
        stmt = select(Variable).where(Variable.key == clean_key)
        res = await session.execute(stmt)
        v = res.scalars().first()
        if not v:
            raise HTTPException(status_code=404, detail=f"Variable '{key}' not found")

        raw_val = vault.decrypt(v.value) if v.is_secret else v.value
        return {"key": v.key, "value": raw_val}


@router.delete("/{var_id}")
async def delete_variable(var_id: str):
    """Deletes a variable by ID."""
    async with AsyncSessionLocal() as session:
        stmt = select(Variable).where(Variable.id == var_id)
        res = await session.execute(stmt)
        v = res.scalars().first()
        if not v:
            raise HTTPException(status_code=404, detail="Variable not found")

        await session.delete(v)
        await session.commit()
        return {"status": "deleted", "id": var_id}
