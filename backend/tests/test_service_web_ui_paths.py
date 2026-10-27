"""Where each service's web UI lives, and who is responsible for routing it there.

Obelisk used to advertise Superset at ``/superset``: the blueprint said so, Caddy proxied
that prefix, the Vite dev server forwarded it verbatim, and the Dashboards page framed it.
Superset was never told to serve a subpath — that takes *both*
``SUPERSET_WEBSERVER_BASEURL`` and ``APPLICATION_ROOT``, and its asset paths assume the
root anyway — so ``/superset`` answered a Flask 404 in front of a perfectly healthy
service, which is exactly what the operator saw.

The rule these tests defend: a blueprint either serves at the root of its own port
(``web_ui_path is None``, and the UI links straight to ``host:port``) or it declares a
subpath **and** the gateway rewrites that subpath onto a root-mounted app. There is no
third option, because the third option is the bug.

Comments are stripped before the configs are searched, so the prose explaining a route's
absence cannot itself look like the route.
"""
import re
from pathlib import Path

import pytest

from app.core.service_blueprints import list_blueprints

REPO_ROOT = Path(__file__).resolve().parents[2]
CADDYFILE = REPO_ROOT / "Caddyfile"
VITE_CONFIG = REPO_ROOT / "frontend" / "vite.config.ts"
DASHBOARDS_VIEW = (
    REPO_ROOT / "frontend" / "src" / "components" / "dashboards" / "DashboardsView.tsx"
)


def _strip_hash_comments(text: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def _strip_slash_comments(text: str) -> str:
    return "\n".join(re.sub(r"(?<!:)//.*$", "", line) for line in text.splitlines())


def _caddy() -> str:
    """The Caddyfile's directives, without its comments."""
    return _strip_hash_comments(CADDYFILE.read_text(encoding="utf-8"))


def _vite() -> str:
    """The Vite dev-proxy map, without its comments."""
    return _strip_slash_comments(VITE_CONFIG.read_text(encoding="utf-8"))


def _superset():
    return next(bp for bp in list_blueprints() if bp.name == "superset")


def test_the_files_these_guards_read_are_where_they_are_expected() -> None:
    """A guard that silently reads nothing passes for the wrong reason."""
    for path in (CADDYFILE, VITE_CONFIG, DASHBOARDS_VIEW):
        assert path.is_file(), f"{path} moved; the routing guard would pass vacuously"


def test_superset_serves_at_the_root_of_its_own_port() -> None:
    """No subpath: the browser is sent to ``host:8088`` and Superset's own routes resolve."""
    superset = _superset()

    assert superset.web_ui is True, "Superset has a UI; the catalog must offer a link"
    assert superset.web_ui_path is None, (
        "/superset is not a path Superset serves: it would 404 in front of a healthy container"
    )
    assert superset.primary_host_port == 8088
    assert superset.readiness.path == "/health"


def test_superset_is_not_told_to_serve_a_subpath() -> None:
    """Half the setting is worse than neither: ``APPLICATION_ROOT`` without matching asset
    paths breaks the UI even when the mount exists."""
    env = _superset().env

    suspicious = [
        key for key in env if key.upper().endswith("BASEURL") or key.upper() == "APPLICATION_ROOT"
    ]
    assert not suspicious, (
        f"{suspicious} describe a subpath the gateway no longer proxies; drop them or restore "
        f"the route, but never ship one without the other"
    )


def test_no_superset_route_survives_in_the_gateway() -> None:
    """Caddy's ``handle /superset/*`` did not even match the bare ``/superset`` the UI
    linked to: the SPA catch-all answered, which is why the page vanished instead of 404ing."""
    caddy = _caddy()

    assert "handle /superset" not in caddy
    assert "handle_path /superset" not in caddy
    assert "obelisk_superset" not in caddy, "the gateway has no business proxying Superset"


def test_no_superset_route_survives_in_the_dev_proxy() -> None:
    """Vite forwarded ``/superset`` unchanged to a root-mounted app: the same 404 in dev."""
    assert "'/superset'" not in _vite()


def test_the_dashboards_page_links_to_superset_instead_of_framing_it() -> None:
    """The iframe is what put the 404 *inside* Obelisk. Nothing about this page may claim
    Superset is served from Obelisk's own origin."""
    source = DASHBOARDS_VIEW.read_text(encoding="utf-8")

    assert "<iframe" not in source
    assert 'src="/superset"' not in source
    assert "serviceUrl" in source, "the link must keep the host the browser already reached"


@pytest.mark.parametrize("blueprint", list_blueprints(), ids=lambda bp: bp.name)
def test_a_declared_subpath_is_one_the_gateway_actually_serves(blueprint) -> None:
    """Gitea declares ``/gitea``; Caddy strips the prefix before forwarding. Anything else
    must not declare a subpath it cannot back up."""
    if not blueprint.web_ui_path:
        return

    path = blueprint.web_ui_path
    assert path.startswith("/") and not path.endswith("/"), (
        f"{blueprint.name}: web_ui_path {path!r} must be a bare prefix like '/gitea'"
    )
    caddy = _caddy()
    served = f"handle_path {path}/" in caddy or f"handle {path}/*" in caddy
    assert served, f"{blueprint.name} links to {path}, but Caddy serves nothing there"
    assert f"'{path}'" in _vite(), (
        f"{blueprint.name} links to {path}, but the dev server proxies nothing there"
    )
