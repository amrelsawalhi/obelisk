"""Seeds every external source the end-to-end test reads from.

Writes deterministic data into three places:

* PostgreSQL  ``public.e2e_customers``, ``public.e2e_orders``
* MongoDB     ``sourcedb.e2e_sessions``, ``sourcedb.e2e_clickstream``
* Lakehouse   ``bronze/e2e_products.parquet``

Credentials are read from the running workspace's own vault (``data/obelisk.db``
plus ``OBELISK_MASTER_KEY``), never hardcoded here, so the script keeps working when
the operator's credentials change.

    python backend/scripts/e2e/seed_sources.py --scale medium

Scales exist so CI stays fast while a real run is large enough to be meaningful:

    scale    orders     customers   events      sessions
    small    20,000     2,000       50,000      10,000
    medium   200,000    10,000      500,000     100,000
    large    2,000,000  50,000      5,000,000   500,000
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.core.vault import decrypt  # noqa: E402

SCALES = {
    "small": {"orders": 20_000, "customers": 2_000, "events": 50_000,
              "sessions": 10_000, "products": 500},
    "medium": {"orders": 200_000, "customers": 10_000, "events": 500_000,
               "sessions": 100_000, "products": 5_000},
    "large": {"orders": 2_000_000, "customers": 50_000, "events": 5_000_000,
              "sessions": 500_000, "products": 20_000},
}

SEED = 20261025
REGIONS = ("emea", "amer", "apac", "latam")
SEGMENTS = ("standard", "silver", "gold", "platinum")
STATUSES = ("placed", "shipped", "delivered", "returned")
DEVICES = ("web", "ios", "android")
EVENT_TYPES = ("view", "add_to_cart", "checkout", "purchase")
PAGES = ("/", "/plp", "/pdp", "/cart", "/checkout")
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def connection(name: str) -> dict:
    """Decrypted connection row, read from the same vault the backend uses."""
    db = ROOT / "data" / "obelisk.db"
    con = sqlite3.connect(db)
    try:
        row = con.execute(
            "select host, port, database, username, encrypted_credentials, options "
            "from connections where name = ?", (name,)
        ).fetchone()
    finally:
        con.close()
    if not row:
        raise SystemExit(f"Connection {name!r} does not exist in {db}.")
    host, port, database, username, encrypted, options = row
    return {
        "host": host, "port": port, "database": database, "username": username,
        "password": decrypt(encrypted), "options": json.loads(options or "{}"),
    }


def customer_row(idx: int) -> tuple:
    return (
        idx,
        f"Customer {idx:06d}",
        REGIONS[(idx - 1) % len(REGIONS)],
        SEGMENTS[(idx - 1) % len(SEGMENTS)],
        date(2024, 1, 1) + timedelta(days=(idx * 7) % 700),
        idx % 11 != 0,
    )


def order_row(rng, idx: int, customers: int) -> tuple:
    customer_id = rng.randint(1, customers)
    placed = BASE + timedelta(minutes=rng.randint(0, 525_600))
    return (
        idx,
        customer_id,
        placed,
        REGIONS[(customer_id - 1) % len(REGIONS)],
        STATUSES[rng.randrange(len(STATUSES))],
        round(rng.uniform(5.0, 900.0), 2),
        placed + timedelta(minutes=rng.randint(0, 2880)),
    )
def seed_postgres(scale: dict) -> dict:
    import psycopg2
    from psycopg2.extras import execute_values

    cfg = connection("pg_enterprise_warehouse")
    started = time.perf_counter()
    with psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"], connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.e2e_orders")
            cur.execute("DROP TABLE IF EXISTS public.e2e_customers")
            cur.execute(
                "CREATE TABLE public.e2e_customers ("
                "customer_id integer PRIMARY KEY, name text NOT NULL, region text NOT NULL, "
                "segment text NOT NULL, signup_date date NOT NULL, is_active boolean NOT NULL)"
            )
            cur.execute(
                "CREATE TABLE public.e2e_orders ("
                "order_id bigint PRIMARY KEY, customer_id integer NOT NULL, "
                "order_ts timestamptz NOT NULL, region text NOT NULL, status text NOT NULL, "
                "amount numeric(12,2) NOT NULL, updated_at timestamptz NOT NULL)"
            )
            execute_values(
                cur,
                "INSERT INTO public.e2e_customers "
                "(customer_id, name, region, segment, signup_date, is_active) VALUES %s",
                (customer_row(i) for i in range(1, scale["customers"] + 1)),
                page_size=5_000,
            )
            rng = random.Random(SEED)
            execute_values(
                cur,
                "INSERT INTO public.e2e_orders "
                "(order_id, customer_id, order_ts, region, status, amount, updated_at) VALUES %s",
                (order_row(rng, i, scale["customers"]) for i in range(1, scale["orders"] + 1)),
                page_size=10_000,
            )
            cur.execute(
                "SELECT count(*), coalesce(round(sum(amount)::numeric, 2), 0) FROM public.e2e_orders"
            )
            orders, total = cur.fetchone()
            cur.execute("SELECT count(*) FROM public.e2e_customers")
            customers = cur.fetchone()[0]

    elapsed = time.perf_counter() - started
    print(f"  postgres   customers={customers:,} orders={orders:,} "
          f"amount={float(total):,.2f} ({elapsed:.1f}s)", flush=True)
    return {
        "customers": customers,
        "orders": orders,
        "orders_amount": float(total),
        "seconds": round(elapsed, 2),
    }


def seed_mongo(scale: dict) -> dict:
    from pymongo import MongoClient
    from urllib.parse import quote_plus

    cfg = connection("mongo_telemetry_source")
    auth_db = cfg["options"].get("auth_db", "admin")
    uri = (
        f"mongodb://{quote_plus(cfg['username'])}:{quote_plus(cfg['password'])}"
        f"@{cfg['host']}:{cfg['port']}/?authSource={auth_db}"
    )
    started = time.perf_counter()
    client = MongoClient(uri, serverSelectionTimeoutMS=8_000)
    try:
        db = client[cfg["database"]]
        db.e2e_sessions.drop()
        db.e2e_clickstream.drop()

        rng = random.Random(SEED + 1)
        batch = []
        for i in range(1, scale["sessions"] + 1):
            batch.append({
                "session_id": f"s{i:08d}",
                "customer_id": rng.randint(1, scale["customers"]),
                "device": DEVICES[rng.randrange(len(DEVICES))],
                "started_at": BASE + timedelta(seconds=rng.randint(0, 31_536_000)),
                "duration_s": rng.randint(1, 3_600),
            })
            if len(batch) >= 5_000:
                db.e2e_sessions.insert_many(batch, ordered=False)
                batch.clear()
        if batch:
            db.e2e_sessions.insert_many(batch, ordered=False)

        rng = random.Random(SEED + 2)
        batch = []
        for i in range(1, scale["events"] + 1):
            kind = EVENT_TYPES[rng.randrange(len(EVENT_TYPES))]
            batch.append({
                "event_id": f"ev{i:09d}",
                "session_id": f"s{rng.randint(1, scale['sessions']):08d}",
                "customer_id": rng.randint(1, scale["customers"]),
                "event_type": kind,
                "page": PAGES[rng.randrange(len(PAGES))],
                "ts": BASE + timedelta(seconds=rng.randint(0, 31_536_000)),
                "revenue": round(rng.uniform(5.0, 400.0), 2) if kind == "purchase" else 0.0,
            })
            if len(batch) >= 5_000:
                db.e2e_clickstream.insert_many(batch, ordered=False)
                batch.clear()
        if batch:
            db.e2e_clickstream.insert_many(batch, ordered=False)

        events = db.e2e_clickstream.count_documents({})
        sessions = db.e2e_sessions.count_documents({})
        purchases = db.e2e_clickstream.count_documents({"event_type": "purchase"})
    finally:
        client.close()

    elapsed = time.perf_counter() - started
    print(f"  mongo      sessions={sessions:,} events={events:,} purchases={purchases:,} "
          f"({elapsed:.1f}s)", flush=True)
    return {
        "sessions": sessions,
        "events": events,
        "purchases": purchases,
        "seconds": round(elapsed, 2),
    }


def seed_lakehouse(scale: dict) -> dict:
    import polars as pl

    from app.sdk.obelisk import save

    started = time.perf_counter()
    n = scale["products"]
    frame = pl.DataFrame({
        "product_id": list(range(1, n + 1)),
        "sku": [f"SKU-{i:06d}" for i in range(1, n + 1)],
        "category": [("apparel", "beauty", "home", "tech")[(i - 1) % 4] for i in range(1, n + 1)],
        "unit_price": [round(5.0 + (i % 97) * 1.35, 2) for i in range(1, n + 1)],
    })
    result = save(frame, "bronze.e2e_products")
    elapsed = time.perf_counter() - started
    print(f"  lakehouse  products={result.get('num_rows', n):,} -> {result.get('file_path', '?')} "
          f"({elapsed:.1f}s)", flush=True)
    return {"products": result.get("num_rows", n), "seconds": round(elapsed, 2)}
def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the end-to-end test sources.")
    parser.add_argument("--scale", choices=sorted(SCALES), default="medium")
    parser.add_argument(
        "--only", choices=("postgres", "mongo", "lakehouse"), default=None,
        help="Seed just one source (default: all three).",
    )
    args = parser.parse_args()

    scale = SCALES[args.scale]
    print(f"seeding scale={args.scale} {scale}", flush=True)
    started = time.perf_counter()
    manifest = {
        "scale": args.scale,
        "seed": SEED,
        "sizes": scale,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    if args.only in (None, "postgres"):
        manifest["postgres"] = seed_postgres(scale)
    if args.only in (None, "mongo"):
        manifest["mongo"] = seed_mongo(scale)
    if args.only in (None, "lakehouse"):
        manifest["lakehouse"] = seed_lakehouse(scale)
    manifest["total_seconds"] = round(time.perf_counter() - started, 2)

    out = ROOT / "data" / "e2e_manifest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"  manifest   {out} ({manifest['total_seconds']}s total)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())