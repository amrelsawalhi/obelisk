import os
import sqlite3
import uuid
import pytest
import psutil
from app.config import settings
from app.core.resource_advisor import advisor, ResourceAdvisor
from app.core.vault import Vault, encrypt, decrypt
from app.database import AsyncSessionLocal
from app.models.connection import Connection

def test_resource_advisor_hardware_specs():
    """
    Asserts live host probing:
    total_ram_gb matches psutil.virtual_memory().total within 1% and is > 0.
    """
    specs = advisor.get_host_specs()
    vm_total = psutil.virtual_memory().total
    expected_total_gb = vm_total / (1024 ** 3)

    assert specs["total_ram_gb"] > 0
    assert abs(specs["total_ram_gb"] - expected_total_gb) / expected_total_gb < 0.01
    assert specs["cpu_count"] > 0
    assert specs["available_ram_gb"] > 0
    assert specs["max_safe_allocation_bytes"] == int(vm_total * 0.85)
    assert specs["max_safe_allocation_gb"] <= specs["total_ram_gb"] * 0.86

def test_resource_advisor_recommendations():
    """Validates dynamic memory sizing for catalog services."""
    services = ["postgres", "superset", "clickhouse", "redpanda", "gitea"]
    specs = advisor.get_host_specs()

    for svc in services:
        rec = advisor.recommend_allocation(svc)
        assert rec["service_name"] == svc
        assert rec["recommended_ram_mb"] > 0
        assert rec["recommended_ram_bytes"] == rec["recommended_ram_mb"] * 1024 * 1024
        assert rec["recommended_cpus"] >= 1.0
        # Ensure allocation does not exceed safe cap
        assert rec["recommended_ram_bytes"] <= specs["max_safe_allocation_bytes"]

def test_duckdb_memory_limit_recommendation():
    """Checks dynamic DuckDB limit string calculation."""
    limit_str = advisor.get_duckdb_memory_limit()
    assert limit_str.endswith("MiB")
    mib_val = int(limit_str.removesuffix("MiB"))
    assert mib_val >= 1


def test_duckdb_memory_limit_is_shared_between_concurrent_slots():
    """Four heavy queries must not each claim the memory meant for the whole host."""
    single_slot = advisor.get_duckdb_memory_limit(1)
    four_slots = advisor.get_duckdb_memory_limit(4)
    assert single_slot.endswith("MiB")
    assert four_slots.endswith("MiB")
    assert int(four_slots.removesuffix("MiB")) <= int(single_slot.removesuffix("MiB"))
    # A nonsensical slot count is clamped to 1 rather than dividing by zero.
    assert advisor.get_duckdb_memory_limit(0) == single_slot


def test_the_memory_budget_cannot_be_multiplied_past_the_host():
    """The sum of every slot must stay inside the 50% DuckDB share, even on a small host."""
    specs = advisor.get_host_specs()
    safe_mib = int(specs["max_safe_allocation_bytes"]) // (1024 ** 2)
    for slots in (1, 2, 4, 8):
        per_slot = advisor.get_duckdb_memory_limit_mb(slots)
        assert per_slot >= 1
        assert per_slot * slots <= safe_mib // 2 + slots, (
            "rounding may leave a few MiB unused, but may never over-allocate"
        )


def test_the_spill_budget_never_promises_more_than_the_disk_has():
    """``DUCKDB_TEMP_SIZE_LIMIT=50GB`` on a 40 GB volume was a promise DuckDB could not keep.

    The budget is the configured ceiling or half the free space, whichever is smaller.
    """
    budget = advisor.get_duckdb_temp_limit(".", "50GB")

    assert budget.endswith("MiB")
    assert int(budget.removesuffix("MiB")) >= 1024, "a spill budget under 1 GiB is useless"


def test_a_low_configured_ceiling_still_wins():
    """The setting is a ceiling, not a floor: a small limit is honoured on a large disk."""
    assert advisor.get_duckdb_temp_limit(".", "1MB") == "1MiB"


def test_an_unparseable_limit_falls_back_to_the_default_ceiling():
    """A typo must not remove the cap entirely."""
    budget = advisor.get_duckdb_temp_limit(".", "fifty gigabytes")

    assert budget.endswith("MiB")
    assert int(budget.removesuffix("MiB")) >= 1024

def test_vault_encryption_and_decryption():
    """Verifies AES-128 Fernet encryption and decryption."""
    secret = "super_confidential_db_token_999"
    ciphertext = encrypt(secret)
    
    assert ciphertext != secret
    assert len(ciphertext) > 20
    assert decrypt(ciphertext) == secret

@pytest.mark.asyncio
async def test_vault_ciphertext_proof_in_sqlite():
    """
    Section 5 B Vault Ciphertext Proof:
    Insert connection record with encrypted credentials, open a raw sqlite3 cursor
    directly against data/obelisk.db and run SELECT encrypted_credentials FROM connections.
    Assert that the raw plaintext password substring is NOT in the raw database column,
    and that vault.decrypt(raw_row[0]) == 'my_secret_pw'.
    """
    plaintext_password = "my_secret_pw"
    encrypted_pw = encrypt(plaintext_password)
    unique_conn_name = f"test_conn_{uuid.uuid4().hex[:8]}"

    # Save via SQLAlchemy async session
    async with AsyncSessionLocal() as session:
        conn_model = Connection(
            name=unique_conn_name,
            source_type="postgres",
            host="localhost",
            port=5432,
            database="warehouse",
            username="analyst",
            encrypted_credentials=encrypted_pw,
        )
        session.add(conn_model)
        await session.commit()

    # Physical verification via raw SQLite cursor
    db_path = settings.sqlite_db_path
    assert os.path.exists(db_path), f"Database file {db_path} does not exist"

    raw_conn = sqlite3.connect(db_path)
    try:
        cursor = raw_conn.cursor()
        cursor.execute(
            "SELECT encrypted_credentials FROM connections WHERE name = ?",
            (unique_conn_name,)
        )
        raw_row = cursor.fetchone()
        assert raw_row is not None, "Connection record was not found in raw SQLite database"
        
        raw_ciphertext = raw_row[0]
        # Physical Proof 1: plaintext password must NOT appear in raw disk storage
        assert plaintext_password not in raw_ciphertext, (
            "SECURITY FAULT: Raw plaintext password was found unencrypted in SQLite column!"
        )
        # Physical Proof 2: ciphertext decrypts to exact original plaintext
        decrypted = decrypt(raw_ciphertext)
        assert decrypted == plaintext_password, (
            f"Decryption mismatch: expected '{plaintext_password}', got '{decrypted}'"
        )
    finally:
        raw_conn.close()

def test_sqlite_wal_proof():
    """
    Section 5 B SQLite WAL Proof:
    Query PRAGMA journal_mode; directly on data/obelisk.db and assert the result is 'wal'.
    """
    db_path = settings.sqlite_db_path
    assert os.path.exists(db_path), f"Database file {db_path} does not exist"

    raw_conn = sqlite3.connect(db_path)
    try:
        cursor = raw_conn.cursor()
        cursor.execute("PRAGMA journal_mode;")
        journal_mode = cursor.fetchone()[0]
        assert journal_mode.lower() == "wal", (
            f"SQLite WAL proof failed: expected 'wal', got '{journal_mode}'"
        )
    finally:
        raw_conn.close()

def test_storage_uri_normalization(tmp_path):
    """Verifies STORAGE_URI resolution: local paths resolve, remote schemes fail.

    Remote schemes used to be returned verbatim from duckdb_storage_path while
    every writer silently wrote to local disk (audit B1). They are now rejected
    at resolution time instead of being passed through to Path()/os.replace().

    The probe directory is a temporary one: ``file:///data/lakehouse/bronze`` is not
    an absolute path on Windows, so resolving it creates a directory inside the
    checkout — which is exactly how empty folders kept appearing in the developer's
    lakehouse after a test run.
    """
    original = settings.STORAGE_URI
    try:
        # Local file:// scheme resolution with forward slashes
        settings.STORAGE_URI = (tmp_path / "lakehouse" / "bronze").as_uri()
        storage_path = settings.duckdb_storage_path
        assert storage_path.endswith("lakehouse/bronze")
        assert "\\" not in storage_path
        assert os.path.exists(storage_path)

        # Remote S3 scheme is refused, not passed through
        settings.STORAGE_URI = "s3://enterprise-lake/bronze"
        with pytest.raises(RuntimeError, match="not implemented yet"):
            _ = settings.duckdb_storage_path

        # Remote Azure scheme is refused too
        settings.STORAGE_URI = "abfss://lake@account.dfs.core.windows.net/bronze"
        with pytest.raises(RuntimeError, match="not implemented yet"):
            _ = settings.duckdb_storage_path
    finally:
        settings.STORAGE_URI = original
