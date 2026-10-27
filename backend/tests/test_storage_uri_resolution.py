"""STORAGE_URI must resolve to a directory that survives the container.

The image shipped ``STORAGE_URI=file:///data/lakehouse`` and mounted ``./data:/app/data``.
Neither path agreed with the other, so the lakehouse — every table and the DuckLake
catalog describing it — was created at ``/data/lakehouse`` inside the container's writable
layer and was lost the moment the container was recreated. ``docker compose up`` looked
healthy the whole time.

These tests pin the resolution rules without a container: a checkout keeps its lakehouse
beside the code, and the image (whose project root is ``/``) can only use the bind mount.
"""
from __future__ import annotations

from pathlib import Path

from app import config as config_module
from app.config import Settings, resolve_lakehouse_directory


def _settings(**overrides) -> Settings:
    """A Settings instance with env untouched, so the real environment cannot leak in."""
    base = {
        "STORAGE_URI": "file:///data/lakehouse",
        "DATABASE_URL": "sqlite+aiosqlite:///:memory:",
    }
    base.update(overrides)
    return Settings(**base)


def _repo_root() -> Path:
    return Path(config_module.__file__).resolve().parent.parent.parent


def test_a_checkout_resolves_the_container_default_beside_the_code():
    """On a checkout the lakehouse is <repo>/data/lakehouse — never the OS root."""
    resolved = resolve_lakehouse_directory(
        "/data/lakehouse", _repo_root(), app_data_available=False
    )

    assert Path(resolved) == _repo_root() / "data" / "lakehouse"
    assert resolved != config_module.CONTAINER_LAKEHOUSE_PATH


def test_the_image_resolves_the_container_default_onto_the_bind_mount():
    """Inside the image the project root is "/", so the bind mount is the only option.

    Passing ``project_root=Path("/")`` reproduces the image exactly: the checkout
    candidate collapses onto the container path being escaped. With ``/app/data``
    mounted, that is where the lakehouse has to go.
    """
    resolved = resolve_lakehouse_directory("/data/lakehouse", Path("/"), app_data_available=True)

    assert resolved == config_module.APP_DATA_LAKEHOUSE_PATH
    assert resolved != config_module.CONTAINER_LAKEHOUSE_PATH


def test_the_image_without_the_bind_mount_does_not_pretend_to_be_persistent():
    """No mount means nothing survives the container; the answer is still not /data.

    There is no persistent directory to choose, so the code keeps its historical
    fallback. The point of the assertion is that the *default* answer inside the image
    is not the writable layer when a mount exists — see the test above.
    """
    resolved = resolve_lakehouse_directory("/data/lakehouse", Path("/"), app_data_available=False)

    assert resolved == config_module.CONTAINER_LAKEHOUSE_PATH


def test_a_subpath_of_the_container_default_keeps_its_suffix():
    """``/data/lakehouse/warehouse`` must not lose ``warehouse`` when it is remapped."""
    resolved = resolve_lakehouse_directory(
        "/data/lakehouse/warehouse", Path("/"), app_data_available=True
    )

    assert resolved == f"{config_module.APP_DATA_LAKEHOUSE_PATH}/warehouse"


def test_an_explicit_storage_uri_is_left_alone():
    """A real URI is a decision, not a default: it must survive resolution untouched."""
    explicit = f"file://{(_repo_root() / 'data' / 'lakehouse').as_posix()}"

    assert resolve_lakehouse_directory(explicit, _repo_root(), app_data_available=True) == explicit


def test_the_catalog_directory_is_created_on_first_use(tmp_path):
    """A dbt run attaches the catalog from its profile, so the folder must already exist.

    Before this, a warehouse with no ``.obelisk/`` failed with
    ``unable to open database file`` and no model could compile.
    """
    settings = _settings(STORAGE_URI=f"file:///{(tmp_path / 'lake').as_posix()}")

    catalog = settings.lakehouse_catalog_path

    assert catalog == tmp_path / "lake" / ".obelisk" / "catalog.sqlite"
    assert catalog.parent.is_dir()

