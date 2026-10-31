"""The SQLite durability modes Obelisk allows, validated once.

Two SQLite files back the platform: the control-plane database (``app.database``)
and the DuckLake metadata catalog (``app.core.catalog``). Both must honour the same
operator-selected journal and synchronous modes, and neither may interpolate an
unvalidated environment value into a PRAGMA. Validation lives here so the two
callers cannot drift apart — the catalog used to apply the journal mode but never
the synchronous mode, which is exactly the kind of gap one shared validator prevents.

This module deliberately imports nothing from ``app.core`` at import time beyond the
settings object: it is imported by ``app.database`` while the engine is being built.
"""
from __future__ import annotations

from app.config import settings

JOURNAL_MODES = frozenset({"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"})
SYNCHRONOUS_MODES = frozenset({"OFF", "NORMAL", "FULL", "EXTRA"})


def pragma_values() -> tuple[str, str]:
    """The validated ``(journal_mode, synchronous)`` this process must use.

    Raises ``RuntimeError`` naming the offending setting rather than letting an
    invalid value reach a PRAGMA, where SQLite would either ignore it or fail with
    an opaque error much further from the cause.
    """
    journal_mode = str(settings.SQLITE_JOURNAL_MODE or "WAL").strip().upper()
    synchronous = str(settings.SQLITE_SYNCHRONOUS or "NORMAL").strip().upper()
    if journal_mode not in JOURNAL_MODES:
        raise RuntimeError(
            f"OBELISK_SQLITE_JOURNAL_MODE={journal_mode!r} is invalid; expected one of "
            f"{sorted(JOURNAL_MODES)}."
        )
    if synchronous not in SYNCHRONOUS_MODES:
        raise RuntimeError(
            f"OBELISK_SQLITE_SYNCHRONOUS={synchronous!r} is invalid; expected one of "
            f"{sorted(SYNCHRONOUS_MODES)}."
        )
    return journal_mode, synchronous
