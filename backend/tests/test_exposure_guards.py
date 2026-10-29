"""The two deliberate exposure decisions: eager SDK reads, and an open API in prod.

Neither of these is a crash. They are the places where a convenience default turns into
a way to lose control of the instance, so the behaviour is pinned here.
"""
from __future__ import annotations

import os
import uuid

import pytest

from app.config import settings
from app.core.security import (
    UNAUTHENTICATED_OPT_IN_ENV,
    assert_deployment_is_safe,
    unauthenticated_terminal_allowed,
)
from app.engines.duckdb_engine import duckdb_engine
from app.sdk import obelisk as sdk


@pytest.fixture()
def three_rows():
    """A lakehouse table with three rows, removed afterwards."""
    schema = f"zz{uuid.uuid4().hex[:6]}"
    conn = duckdb_engine.get_connection()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema};")
    conn.execute(
        f"CREATE TABLE lakehouse.{schema}.t AS SELECT * FROM (VALUES (1), (2), (3)) v(n)"
    )
    try:
        yield f"lakehouse.{schema}.t"
    finally:
        conn.execute(f"DROP SCHEMA lakehouse.{schema} CASCADE;")
        conn.close()


def test_query_refuses_to_materialise_past_its_cap(three_rows) -> None:
    previous = settings.SDK_QUERY_ROW_LIMIT
    settings.SDK_QUERY_ROW_LIMIT = 2
    try:
        with pytest.raises(RuntimeError, match="iter_batches"):
            sdk.query(f"SELECT * FROM {three_rows}")
        assert sdk.query(f"SELECT * FROM {three_rows} LIMIT 2").height == 2
    finally:
        settings.SDK_QUERY_ROW_LIMIT = previous


def test_query_arrow_refuses_to_materialise_past_its_cap(three_rows) -> None:
    previous = settings.SDK_QUERY_ROW_LIMIT
    settings.SDK_QUERY_ROW_LIMIT = 2
    try:
        with pytest.raises(RuntimeError, match="lazy_query"):
            sdk.query_arrow(f"SELECT * FROM {three_rows}")
        assert sdk.query_arrow(f"SELECT * FROM {three_rows} LIMIT 2").num_rows == 2
    finally:
        settings.SDK_QUERY_ROW_LIMIT = previous


def test_a_zero_cap_is_an_explicit_opt_out(three_rows) -> None:
    previous = settings.SDK_QUERY_ROW_LIMIT
    settings.SDK_QUERY_ROW_LIMIT = 0
    try:
        assert sdk.query(f"SELECT * FROM {three_rows}").height == 3
    finally:
        settings.SDK_QUERY_ROW_LIMIT = previous


def _set_deployment(environment: str, auth: bool, host: str) -> tuple:
    previous = (settings.ENVIRONMENT, settings.AUTH_ENABLED, settings.HOST)
    settings.ENVIRONMENT = environment
    settings.AUTH_ENABLED = auth
    settings.HOST = host
    return previous


def test_production_refuses_an_unauthenticated_public_api() -> None:
    previous = _set_deployment("production", False, "0.0.0.0")
    status = os.environ.pop(UNAUTHENTICATED_OPT_IN_ENV, None)
    try:
        with pytest.raises(RuntimeError, match="OBELISK_AUTH_ENABLED"):
            assert_deployment_is_safe()

        settings.HOST = "127.0.0.1"
        assert_deployment_is_safe()  # a private bind is allowed

        settings.HOST = "0.0.0.0"
        settings.AUTH_ENABLED = True
        assert_deployment_is_safe()  # authentication is the other way to be safe
    finally:
        settings.ENVIRONMENT, settings.AUTH_ENABLED, settings.HOST = previous
        if status is not None:
            os.environ[UNAUTHENTICATED_OPT_IN_ENV] = status


def test_an_unauthenticated_production_deployment_needs_an_explicit_opt_in() -> None:
    previous = _set_deployment("production", False, "0.0.0.0")
    os.environ[UNAUTHENTICATED_OPT_IN_ENV] = "1"
    try:
        assert_deployment_is_safe()
    finally:
        settings.ENVIRONMENT, settings.AUTH_ENABLED, settings.HOST = previous
        os.environ.pop(UNAUTHENTICATED_OPT_IN_ENV, None)


def test_a_local_install_is_not_gated() -> None:
    previous = _set_deployment("development", False, "0.0.0.0")
    try:
        assert_deployment_is_safe()
    finally:
        settings.ENVIRONMENT, settings.AUTH_ENABLED, settings.HOST = previous


def test_an_empty_or_any_address_host_is_not_loopback() -> None:
    """An unset HOST means "all interfaces", which must not slip past the guard."""
    previous = _set_deployment("production", False, "")
    status = os.environ.pop(UNAUTHENTICATED_OPT_IN_ENV, None)
    try:
        with pytest.raises(RuntimeError, match="OBELISK_AUTH_ENABLED"):
            assert_deployment_is_safe()
        settings.HOST = "::"
        with pytest.raises(RuntimeError, match="OBELISK_AUTH_ENABLED"):
            assert_deployment_is_safe()
    finally:
        settings.ENVIRONMENT, settings.AUTH_ENABLED, settings.HOST = previous
        if status is not None:
            os.environ[UNAUTHENTICATED_OPT_IN_ENV] = status


def test_the_unauthenticated_terminal_refuses_a_proxy_it_does_not_trust() -> None:
    previous = settings.TRUSTED_PROXIES
    settings.TRUSTED_PROXIES = ""
    try:
        assert unauthenticated_terminal_allowed("127.0.0.1", {}) is True
        assert unauthenticated_terminal_allowed("10.1.2.3", {}) is False
        # A loopback peer claiming to forward someone is not believed without config:
        # a local proxy makes every connection look loopback.
        assert (
            unauthenticated_terminal_allowed(
                "127.0.0.1", {"X-Forwarded-For": "127.0.0.1"}
            )
            is False
        )
    finally:
        settings.TRUSTED_PROXIES = previous


def test_a_trusted_proxy_must_still_forward_a_loopback_client() -> None:
    previous = settings.TRUSTED_PROXIES
    settings.TRUSTED_PROXIES = "10.0.0.2"
    try:
        assert (
            unauthenticated_terminal_allowed(
                "10.0.0.2", {"X-Forwarded-For": "127.0.0.1"}
            )
            is True
        )
        assert (
            unauthenticated_terminal_allowed(
                "10.0.0.2", {"X-Forwarded-For": "10.9.9.9"}
            )
            is False
        )
        # A chained header: the trusted proxy is the last hop, the client the first.
        assert (
            unauthenticated_terminal_allowed(
                "10.0.0.2", {"X-Forwarded-For": "127.0.0.1, 10.0.0.2"}
            )
            is True
        )
        assert (
            unauthenticated_terminal_allowed(
                "10.0.0.2", {"X-Forwarded-For": "203.0.113.7, 10.0.0.2"}
            )
            is False
        )
        # The proxy itself connecting without a forwarded header is not loopback.
        assert unauthenticated_terminal_allowed("10.0.0.2", {}) is False
    finally:
        settings.TRUSTED_PROXIES = previous
