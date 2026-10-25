"""Lakehouse housekeeping: compaction, snapshot expiry, and a backup that can be restored.

Two jobs, both safe to run by hand and meant to be scheduled:

  maintain  merges small files, expires old snapshots, and (optionally) deletes the files
            those snapshots left behind. Every step is reported and `--dry-run` says what
            would change without changing it.
  backup    copies the lakehouse folder (data *and* its catalog, which live together) and
            takes a consistent SQLite snapshot of the control plane.

Run:  backend\\.venv\\Scripts\\python.exe backend\\scripts\\lakehouse_maintenance.py maintain --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT / "backend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "backend"))

from app.config import settings  # noqa: E402
from app.core.catalog import SOURCE, lakehouse_connection  # noqa: E402


def _maintain(args: argparse.Namespace) -> int:
    conn = lakehouse_connection()
    try:
        # 1. Merge small files: this is what stops a table becoming thousands of fragments
        #    after a year of daily loads. Bounded, so one run cannot rewrite the lake.
        if args.merge:
            merged = conn.execute(
                f"CALL ducklake_merge_adjacent_files('{SOURCE}', "
                f"max_compacted_files => {args.max_files});"
            ).fetchall()
            print(f"merge_adjacent_files: {len(merged)} table(s) touched")

        # 2. Expire snapshots older than the retention window. Time travel beyond the
        #    window is gone on purpose: keeping every version forever is how a lake fills.
        older_than = datetime.now(timezone.utc) - timedelta(days=args.keep_days)
        expired = conn.execute(
            f"CALL ducklake_expire_snapshots('{SOURCE}', "
            f"older_than => TIMESTAMPTZ '{older_than.isoformat()}'{', dry_run => true' if args.dry_run else ''});"
        ).fetchall()
        print(f"expire_snapshots: {len(expired)} snapshot(s) {('would be' if args.dry_run else '')} expired")

        # 3. Remove the files those expired snapshots left behind. Dry run by default
        #    unless --delete-files is given: deleting bytes is the one irreversible step.
        if args.delete_files:
            removed = conn.execute(
                f"CALL ducklake_cleanup_old_files('{SOURCE}', "
                f"older_than => TIMESTAMPTZ '{older_than.isoformat()}', "
                f"dry_run => {'true' if args.dry_run else 'false'});"
            ).fetchall()
            print(f"cleanup_old_files: {len(removed)} file(s) {('would be' if args.dry_run else '')} removed")
        else:
            print("cleanup_old_files: skipped (pass --delete-files to remove expired files)")

        remaining = len(conn.execute(f"FROM ducklake_snapshots('{SOURCE}');").fetchall())
        print(f"snapshots remaining: {remaining}")
    finally:
        conn.close()
    return 0


def _backup(args: argparse.Namespace) -> int:
    import shutil

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = Path(args.destination) / f"obelisk-backup-{stamp}"
    target.mkdir(parents=True, exist_ok=False)

    # The lakehouse folder holds the data *and* the catalog that describes it, so copying
    # one tree captures both — that is why the catalog lives inside it.
    lakehouse = settings.lakehouse_root_path
    if lakehouse.is_dir():
        shutil.copytree(lakehouse, target / "lakehouse")
        print(f"lakehouse copied -> {target / 'lakehouse'}")
    else:
        print(f"lakehouse not found at {lakehouse}; skipped")

    # SQLite cannot be copied safely while the app is writing. `.backup` takes a
    # consistent snapshot through the database itself.
    db_path = Path(settings.sqlite_db_path)
    if db_path.is_file():
        with sqlite3.connect(db_path) as source, sqlite3.connect(target / "obelisk.db") as dest:
            source.backup(dest)
        print(f"control plane snapshotted -> {target / 'obelisk.db'}")
    else:
        print(f"control plane not found at {db_path}; skipped")

    print(f"\nbackup complete: {target}")
    print("Restore: stop Obelisk, replace data/lakehouse and data/obelisk.db, start again.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    maintain = sub.add_parser("maintain", help="compact, expire snapshots, clean up files")
    maintain.add_argument("--keep-days", type=int, default=14, help="snapshots older than this expire")
    maintain.add_argument("--max-files", type=int, default=50, help="cap on files rewritten in one run")
    maintain.add_argument("--merge", action="store_true", default=True)
    maintain.add_argument("--no-merge", dest="merge", action="store_false")
    maintain.add_argument("--delete-files", action="store_true", help="also delete expired files")
    maintain.add_argument("--dry-run", action="store_true", help="report without changing anything")
    maintain.set_defaults(func=_maintain)

    backup = sub.add_parser("backup", help="copy the lakehouse and snapshot the control plane")
    backup.add_argument("--destination", default=str(PROJECT_ROOT / "backups"))
    backup.set_defaults(func=_backup)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
