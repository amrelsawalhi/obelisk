"""
Layers API.

Exposes the nested layer tree that replaced the hardcoded medallion tiers. Layers
are nodes of a materialised path (see :mod:`app.core.layers`); this router only
manages the tree — moving data when a layer is renamed belongs to the write path,
so a rename is refused while the subtree still holds objects.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select, update

from app.core.layers import (
    InvalidLayerPath,
    join_layer_path,
    parent_layer_path,
    resolve_layer,
    validate_layer_path,
)
from app.database import AsyncSessionLocal
from app.models.catalog_object import CatalogObject
from app.models.layer import Layer

router = APIRouter(prefix="/api/layers", tags=["Layers"])


class CreateLayerRequest(BaseModel):
    path: str = Field(..., description="Dot-separated path, e.g. 'raw' or 'marts.sales'")
    description: Optional[str] = None
    color: Optional[str] = Field(default=None, description="Optional UI accent, e.g. '#22c55e'")
    icon: Optional[str] = Field(default=None, description="Optional icon key")
    position: Optional[int] = Field(default=None, description="Sort order among siblings")


class UpdateLayerRequest(BaseModel):
    name: Optional[str] = Field(
        default=None, description="Renames the last segment of the path"
    )
    description: Optional[str] = None
    color: Optional[str] = None
    icon: Optional[str] = None
    position: Optional[int] = None
    is_default: Optional[bool] = Field(
        default=None, description="Marks this layer as the default ingest target"
    )


def _subtree_clause(path: str):
    """Matches a layer and every descendant of it (materialised-path prefix)."""
    return (Layer.path == path) | (Layer.path.like(f"{path}.%"))


async def _object_counts(session) -> dict[str, int]:
    """Object counts per exact layer path, from the catalog (metadata only)."""
    rows = await session.execute(
        select(CatalogObject.layer_path, func.count(CatalogObject.id)).group_by(
            CatalogObject.layer_path
        )
    )
    return {lp: count for lp, count in rows.all() if lp}


def _serialize(layer: Layer, subtree_objects: int) -> dict:
    return {
        "path": layer.path,
        "name": layer.name,
        "parent_path": layer.parent_path,
        "depth": layer.depth,
        "position": layer.position,
        "description": layer.description,
        "color": layer.color,
        "icon": layer.icon,
        "is_default": bool(layer.is_default),
        "object_count": subtree_objects,
        "created_at": layer.created_at.isoformat() if layer.created_at else None,
        "updated_at": layer.updated_at.isoformat() if layer.updated_at else None,
    }


@router.get("")
async def list_layers():
    """Returns every layer, ordered so a client can render the tree directly."""
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(Layer).order_by(Layer.parent_path.asc(), Layer.position.asc(), Layer.name.asc())
            )
        ).scalars().all()
        counts = await _object_counts(session)

        result = []
        for layer in rows:
            subtree = sum(
                n for lp, n in counts.items() if lp == layer.path or lp.startswith(f"{layer.path}.")
            )
            result.append(_serialize(layer, subtree))
        return result


@router.get("/resolve")
async def resolve_layer_path(path: str):
    """Shows the physical shapes a logical layer maps to (files, schema, dbt)."""
    try:
        loc = resolve_layer(path)
    except InvalidLayerPath as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "path": loc.path,
        "depth": loc.depth,
        "filesystem_dir": loc.filesystem_dir.as_posix(),
        "warehouse_schema": loc.warehouse_schema,
        "dbt_folder": loc.dbt_folder,
    }


async def _subtree_object_count(session, path: str) -> int:
    """Objects directly in ``path`` plus everything below it."""
    stmt = select(func.count(CatalogObject.id)).where(
        (CatalogObject.layer_path == path) | (CatalogObject.layer_path.like(f"{path}.%"))
    )
    return (await session.execute(stmt)).scalar() or 0


def _directory_holds_files(path: str) -> bool:
    """True when the layer's directory already contains data on disk.

    Renaming or deleting such a layer would orphan real Parquet files, so the API
    refuses instead of leaving the catalog pointing at a directory whose name no
    longer matches its contents.
    """
    try:
        directory = resolve_layer(path).filesystem_dir
        if not directory.is_dir():
            return False
        return any(entry.is_file() for entry in directory.rglob("*"))
    except (InvalidLayerPath, OSError):
        return False


@router.post("", status_code=201)
async def create_layer(req: CreateLayerRequest):
    """Adds a layer. Parents must exist, so the tree is always consistent."""
    try:
        path = validate_layer_path(req.path)
    except InvalidLayerPath as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    parent = parent_layer_path(path)
    async with AsyncSessionLocal() as session:
        if await session.get(Layer, path) is not None:
            raise HTTPException(status_code=409, detail=f"Layer {path!r} already exists.")
        if parent and await session.get(Layer, parent) is None:
            raise HTTPException(
                status_code=400,
                detail=f"Parent layer {parent!r} does not exist. Create it first.",
            )

        position = req.position
        if position is None:
            sibling_max = (
                await session.execute(
                    select(func.max(Layer.position)).where(
                        Layer.parent_path == parent
                        if parent
                        else Layer.parent_path.is_(None)
                    )
                )
            ).scalar()
            position = (sibling_max or 0) + 1

        defaults = (
            await session.execute(
                select(func.count()).select_from(Layer).where(Layer.is_default.is_(True))
            )
        ).scalar() or 0

        layer = Layer(
            path=path,
            name=path.rsplit(".", 1)[-1],
            parent_path=parent,
            position=position,
            description=req.description,
            color=req.color,
            icon=req.icon,
            is_default=defaults == 0,
        )
        session.add(layer)
        await session.commit()
        await session.refresh(layer)
        return _serialize(layer, 0)


@router.patch("/{path}")
async def update_layer(path: str, req: UpdateLayerRequest):
    """Updates a layer's metadata, or renames its last segment when it is empty."""
    try:
        current = validate_layer_path(path)
    except InvalidLayerPath as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    async with AsyncSessionLocal() as session:
        layer = await session.get(Layer, current)
        if layer is None:
            raise HTTPException(status_code=404, detail=f"Layer {current!r} not found.")

        new_path = current
        if req.name is not None and req.name != layer.name:
            try:
                new_path = join_layer_path(layer.parent_path, req.name)
            except InvalidLayerPath as exc:
                raise HTTPException(status_code=400, detail=str(exc))

            if await session.get(Layer, new_path) is not None:
                raise HTTPException(
                    status_code=409, detail=f"Layer {new_path!r} already exists."
                )
            objects = await _subtree_object_count(session, current)
            if objects:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Layer {current!r} still holds {objects} catalogued object(s). "
                        "Move or drop them before renaming the layer."
                    ),
                )
            if _directory_holds_files(current):
                raise HTTPException(
                    status_code=409,
                    detail=f"Layer {current!r} still has files on disk; refusing to rename.",
                )

            # Rewrite the whole subtree: paths are materialised, so every
            # descendant's path and parent_path has to follow the new prefix.
            subtree = (
                await session.execute(select(Layer).where(_subtree_clause(current)))
            ).scalars().all()
            for node in subtree:
                moved = new_path + node.path[len(current):]
                await session.execute(
                    update(Layer)
                    .where(Layer.path == node.path)
                    .values(path=moved, parent_path=parent_layer_path(moved))
                    .execution_options(synchronize_session=False)
                )
            session.expunge_all()
            layer = await session.get(Layer, new_path)
            await session.refresh(layer)

        for field in ("description", "color", "icon", "position"):
            value = getattr(req, field)
            if value is not None:
                setattr(layer, field, value)

        if req.is_default:
            await session.execute(
                update(Layer)
                .where(Layer.path != new_path)
                .values(is_default=False)
                .execution_options(synchronize_session=False)
            )
            layer.is_default = True

        await session.commit()
        await session.refresh(layer)
        return _serialize(layer, await _subtree_object_count(session, new_path))


@router.delete("/{path}")
async def delete_layer(path: str):
    """Deletes an empty layer, promoting another one if it was the default."""
    try:
        current = validate_layer_path(path)
    except InvalidLayerPath as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    async with AsyncSessionLocal() as session:
        layer = await session.get(Layer, current)
        if layer is None:
            raise HTTPException(status_code=404, detail=f"Layer {current!r} not found.")

        children = (
            await session.execute(
                select(func.count()).select_from(Layer).where(Layer.parent_path == current)
            )
        ).scalar() or 0
        if children:
            raise HTTPException(
                status_code=409,
                detail=f"Layer {current!r} has {children} child layer(s); delete them first.",
            )

        objects = await _subtree_object_count(session, current)
        if objects:
            raise HTTPException(
                status_code=409,
                detail=f"Layer {current!r} still holds {objects} catalogued object(s).",
            )
        if _directory_holds_files(current):
            raise HTTPException(
                status_code=409, detail=f"Layer {current!r} still has files on disk."
            )

        total = (
            await session.execute(select(func.count()).select_from(Layer))
        ).scalar() or 0
        if total <= 1:
            raise HTTPException(
                status_code=409, detail="Cannot delete the only remaining layer."
            )

        was_default = bool(layer.is_default)
        await session.delete(layer)
        await session.flush()

        promoted = None
        if was_default:
            promoted = (
                await session.execute(select(Layer).order_by(Layer.path.asc()).limit(1))
            ).scalars().first()
            if promoted is not None:
                promoted.is_default = True

        await session.commit()
        return {
            "deleted": current,
            "default_layer": promoted.path if promoted is not None else None,
        }

