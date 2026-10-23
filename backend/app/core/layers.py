"""Layer resolution — the seam between a layer *name* and physical storage.

A layer is a path, not an enum. ``marts.sales`` is legal, and no module knows the
words bronze/silver/gold: a workspace that wants medallions creates those three
layers, and one that wants a flat tree keeps the seeded ``raw``.

Three physical backends need three shapes, so :func:`resolve_layer` derives all of
them at once:

==========  =======================================  ====================
backend     shape of ``marts.sales``                value
==========  =======================================  ====================
files       one directory below the lakehouse root  ``<root>/marts.sales``
warehouse   one flat schema (schemas cannot nest)   ``marts__sales``
dbt         one folder under ``models/``            ``models/marts__sales``
==========  =======================================  ====================

``.`` separates path segments; ``__`` separates them after flattening. A single
segment therefore may not contain ``__``, otherwise ``a.b`` and ``a__b`` would
flatten to the same warehouse schema and silently merge two different layers.

Resolution is pure: it touches the filesystem only to normalise a path, never to
create anything. Callers decide when a directory comes into existence.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from app.config import settings

LAYER_SEPARATOR = "."
FLATTEN_SEPARATOR = "__"
FQN_ROOT = "lakehouse"
MAX_DEPTH = 8
MAX_PATH_LENGTH = 255

# A segment starts with a letter or digit so that no segment can begin with a
# shell/SQL metacharacter, and contains no '.' (the separator) or '__' (the
# flattening separator).
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class InvalidLayerPath(ValueError):
    """Raised when a layer path cannot be represented unambiguously."""


def validate_layer_path(path: str) -> str:
    """Returns ``path`` stripped, or raises :class:`InvalidLayerPath`.

    The rules exist so one path maps to exactly one physical destination, and so
    a path can never escape the lakehouse root.
    """
    if path is None or not str(path).strip():
        raise InvalidLayerPath("Layer path is empty.")
    clean = str(path).strip()
    if len(clean) > MAX_PATH_LENGTH:
        raise InvalidLayerPath(f"Layer path is longer than {MAX_PATH_LENGTH} characters.")
    if "/" in clean or "\\" in clean or ".." in clean:
        raise InvalidLayerPath(f"Layer path {clean!r} contains an illegal path segment.")
    if clean.startswith(LAYER_SEPARATOR) or clean.endswith(LAYER_SEPARATOR):
        raise InvalidLayerPath(f"Layer path {clean!r} may not start or end with '.'.")

    segments = clean.split(LAYER_SEPARATOR)
    if len(segments) > MAX_DEPTH:
        raise InvalidLayerPath(f"Layer path {clean!r} is deeper than {MAX_DEPTH} levels.")
    for segment in segments:
        if not segment:
            raise InvalidLayerPath(f"Layer path {clean!r} contains an empty segment.")
        if FLATTEN_SEPARATOR in segment:
            raise InvalidLayerPath(
                f"Layer segment {segment!r} may not contain {FLATTEN_SEPARATOR!r}: it would "
                "collide with the flattened form of a nested layer."
            )
        if not _SEGMENT_RE.match(segment):
            raise InvalidLayerPath(
                f"Layer segment {segment!r} may only contain letters, digits, '_' and '-', "
                "and must start with a letter or digit."
            )
    return clean


def split_layer_path(path: str) -> tuple[str, ...]:
    """Validated path as its segments: ``'marts.sales'`` -> ``('marts', 'sales')``."""
    return tuple(validate_layer_path(path).split(LAYER_SEPARATOR))


_UNSAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_-]+")


def safe_layer_path(value: str) -> str:
    """Best-effort layer path for values that predate layer validation.

    The ingest engine accepts a free-form ``target_folder``, and rows written
    before layers existed can hold anything. The catalog must never drop such a
    row because its folder name is not a legal path, so this normalises what it
    can and falls back to the landing layer.
    """
    try:
        return validate_layer_path(value)
    except InvalidLayerPath:
        cleaned = _UNSAFE_CHARS_RE.sub("_", str(value or "")).strip("_")
        return cleaned or "raw"


def parent_layer_path(path: str) -> Optional[str]:
    """``'marts.sales'`` -> ``'marts'``; ``None`` for a root layer."""
    segments = split_layer_path(path)
    return LAYER_SEPARATOR.join(segments[:-1]) if len(segments) > 1 else None


def join_layer_path(parent: Optional[str], name: str) -> str:
    """Builds and validates a child path from one segment name.

    ``name`` must be a single segment: letting it contain ``'.'`` would silently
    turn a rename into a re-parent.
    """
    if name is None or LAYER_SEPARATOR in str(name):
        raise InvalidLayerPath(f"Layer name {name!r} must be a single segment (no '.').")
    if not parent:
        return validate_layer_path(name)
    return validate_layer_path(f"{parent}{LAYER_SEPARATOR}{name}")


def flatten_layer_path(path: str) -> str:
    """``'marts.sales'`` -> ``'marts__sales'`` (warehouse schema, dbt folder)."""
    return FLATTEN_SEPARATOR.join(split_layer_path(path))


def layer_fqn(layer_path: str, name: str) -> str:
    """Fully-qualified catalogue name of an object inside a layer."""
    return f"{FQN_ROOT}{LAYER_SEPARATOR}{validate_layer_path(layer_path)}{LAYER_SEPARATOR}{name}"


@dataclass(frozen=True)
class LayerLocation:
    """Every physical shape one logical layer takes."""

    path: str
    depth: int
    filesystem_dir: Path
    warehouse_schema: str
    dbt_folder: str

    def table_fqn(self, name: str) -> str:
        return layer_fqn(self.path, name)


def resolve_layer(path: str, *, root: Path | str | None = None) -> LayerLocation:
    """Maps a logical layer path onto its physical locations.

    ``root`` defaults to the configured lakehouse root; tests pass a temporary
    directory so they never depend on the developer's machine.
    """
    clean = validate_layer_path(path)
    base = Path(root) if root is not None else settings.lakehouse_root_path

    # The layer path is used verbatim as one directory name, which keeps every
    # row already written by the ingest engine resolvable. Splitting dotted paths
    # into nested directories would be a data migration, not a refactor.
    base_resolved = base.resolve()
    directory = (base_resolved / clean).resolve()
    if directory != base_resolved and base_resolved not in directory.parents:
        raise InvalidLayerPath(f"Layer path {clean!r} escapes the lakehouse root.")

    flattened = flatten_layer_path(clean)
    return LayerLocation(
        path=clean,
        depth=len(split_layer_path(clean)),
        filesystem_dir=directory,
        warehouse_schema=flattened,
        dbt_folder=f"models/{flattened}",
    )

