import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import settings

router = APIRouter(prefix="/api/files", tags=["files"])

IGNORED_NAMES = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".venv",
    "node_modules",
    ".DS_Store",
    ".idea",
    ".vscode",
    "target",
    "logs",
}


MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB safety limit for web editor


def _resolve_safe_path(rel_path: str) -> tuple[Path, str]:
    """
    Resolves relative path inside git_repo and ensures no directory traversal.
    Returns (absolute_path, normalized_relative_posix_path).
    """
    clean_rel = rel_path.replace("\\", "/").strip("/").strip()
    root = settings.git_repo_path.resolve()

    if not clean_rel:
        return root, ""

    target = (root / clean_rel).resolve()

    # Security check: must reside inside root
    try:
        target.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied: path is outside git_repo")

    norm_rel = target.relative_to(root).as_posix()
    return target, norm_rel


def _build_tree(directory: Path, root: Path) -> list[dict]:
    """
    Recursively builds a directory tree.
    """
    items = []
    try:
        entries = sorted(list(directory.iterdir()), key=lambda e: (not e.is_dir(), e.name.lower()))
    except (PermissionError, FileNotFoundError):
        return items

    for entry in entries:
        if entry.name in IGNORED_NAMES or entry.name.endswith(".pyc"):
            continue

        rel_posix = entry.relative_to(root).as_posix()
        try:
            stat = entry.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
            size = stat.st_size
        except OSError:
            mtime = None
            size = 0

        if entry.is_dir():
            items.append({
                "name": entry.name,
                "path": rel_posix,
                "type": "directory",
                "modified_at": mtime,
                "children": _build_tree(entry, root),
            })
        else:
            ext = entry.suffix.lower()
            items.append({
                "name": entry.name,
                "path": rel_posix,
                "type": "file",
                "extension": ext,
                "size": size,
                "modified_at": mtime,
            })
    return items


class SaveFileRequest(BaseModel):
    path: str = Field(..., description="Relative path within git_repo")
    content: str = Field(..., description="File content to save")


class CreateFileRequest(BaseModel):
    path: str = Field(..., description="Relative path within git_repo")
    type: Literal["file", "directory"] = Field("file", description="Whether to create a file or directory")


class RenameFileRequest(BaseModel):
    old_path: str = Field(..., description="Existing relative path")
    new_path: str = Field(..., description="New relative path")


class DeleteFileRequest(BaseModel):
    path: str = Field(..., description="Relative path within git_repo")


@router.get("/tree")
def get_file_tree():
    """
    Returns the full file and directory tree of git_repo/.
    """
    root = settings.git_repo_path.resolve()
    root.mkdir(parents=True, exist_ok=True)
    tree = _build_tree(root, root)
    return {
        "root": "git_repo",
        "absolute_path": root.as_posix(),
        "children": tree,
    }


@router.get("/content")
def get_file_content(path: str = Query(..., description="Relative path within git_repo")):
    """
    Reads the content of a text file inside git_repo/.
    """
    target, norm_rel = _resolve_safe_path(path)

    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {norm_rel}")

    if target.is_dir():
        raise HTTPException(status_code=400, detail="Cannot read content of a directory")

    stat = target.stat()
    if stat.st_size > MAX_FILE_SIZE_BYTES:
        raise HTTPException(status_code=400, detail=f"File too large ({stat.st_size} bytes). Max supported size is 5MB.")

    try:
        with open(target, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(target, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read file as text: {str(e)}")

    return {
        "path": norm_rel,
        "name": target.name,
        "extension": target.suffix.lower(),
        "size": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "content": content,
    }


@router.post("/save")
def save_file(req: SaveFileRequest):
    """
    Saves content to an existing or new file in git_repo/.
    """
    target, norm_rel = _resolve_safe_path(req.path)

    if target.is_dir():
        raise HTTPException(status_code=400, detail="Target path is a directory, not a file")

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "w", encoding="utf-8") as f:
        f.write(req.content)

    stat = target.stat()
    return {
        "status": "saved",
        "path": norm_rel,
        "size": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


@router.post("/create")
def create_file_or_dir(req: CreateFileRequest):
    """
    Creates a new file or directory inside git_repo/.
    """
    target, norm_rel = _resolve_safe_path(req.path)

    if not norm_rel:
        raise HTTPException(status_code=400, detail="Cannot create root git_repo")

    if target.exists():
        raise HTTPException(status_code=409, detail=f"Path already exists: {norm_rel}")

    if req.type == "directory":
        target.mkdir(parents=True, exist_ok=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()

    return {
        "status": "created",
        "path": norm_rel,
        "type": req.type,
    }


@router.post("/rename")
def rename_file(req: RenameFileRequest):
    """
    Renames or moves a file/directory inside git_repo/.
    """
    old_target, old_rel = _resolve_safe_path(req.old_path)
    new_target, new_rel = _resolve_safe_path(req.new_path)

    if not old_rel:
        raise HTTPException(status_code=400, detail="Cannot rename root git_repo")

    if not old_target.exists():
        raise HTTPException(status_code=404, detail=f"Source path not found: {old_rel}")

    if new_target.exists():
        raise HTTPException(status_code=409, detail=f"Destination already exists: {new_rel}")

    new_target.parent.mkdir(parents=True, exist_ok=True)
    old_target.rename(new_target)

    return {
        "status": "renamed",
        "old_path": old_rel,
        "new_path": new_rel,
    }


@router.post("/delete")
def delete_file(req: DeleteFileRequest):
    """
    Deletes a file or directory inside git_repo/.
    """
    target, norm_rel = _resolve_safe_path(req.path)

    if not norm_rel:
        raise HTTPException(status_code=400, detail="Cannot delete root git_repo")

    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Path not found: {norm_rel}")

    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()

    return {
        "status": "deleted",
        "path": norm_rel,
    }
