"""The shipped REST connector presets are data, and must stay true.

A preset that no longer parses is worse than no preset: the form would fill in options
the engine rejects, and the operator would read that as the platform being broken.
Every preset is therefore validated against the real option schema here, and one of
them is exercised end to end against a local server that speaks its pagination style.
"""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from app.engines.api_source import build_plan, load_presets


def test_every_preset_parses_into_a_plan() -> None:
    presets = load_presets()
    assert presets, "the presets file must ship at least one recipe"
    for preset in presets:
        plan = build_plan(preset.get("options") or {}, watermark_column="updated_at")
        assert plan.kind in ("none", "offset", "page", "cursor", "link")


def test_every_preset_is_identified_and_described() -> None:
    ids = set()
    for preset in load_presets():
        assert preset.get("id"), f"a preset without an id: {preset!r}"
        assert preset["id"] not in ids, f"duplicate preset id {preset['id']!r}"
        ids.add(preset["id"])
        assert preset.get("name"), f"{preset['id']} has no display name"
        assert preset.get("description"), f"{preset['id']} has no description"


def test_the_github_preset_pages_a_link_header_source() -> None:
    """The layout the recipe promises, against a server that answers it."""
    preset = next(item for item in load_presets() if item["id"] == "github-rest")
    options = dict(preset["options"])
    options["pagination"] = {**options["pagination"], "page_size": 2}

    rows = [{"id": number} for number in range(1, 6)]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - the name is the HTTP method
            assert self.headers.get("Authorization") == "Bearer tok"
            assert self.headers.get("Accept") == "application/vnd.github+json"
            page = 1
            if "page=" in self.path:
                page = int(self.path.split("page=")[1].split("&")[0])
            start = (page - 1) * 2
            body = str(rows[start : start + 2]).replace("'", '"').encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if start + 2 < len(rows):
                self.send_header(
                    "Link",
                    f'<http://127.0.0.1:{self.server.server_address[1]}/repos?page={page + 1}>;'
                    ' rel="next"',
                )
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        from app.engines.api_source import ApiSource

        source = ApiSource(
            f"http://127.0.0.1:{server.server_address[1]}", "repos", token="tok", options=options
        )
        pages = [page.records(None) for page in source.pages()]
    finally:
        server.shutdown()
        server.server_close()

    assert [len(page) for page in pages] == [2, 2, 1], "the recipe walked every page"
