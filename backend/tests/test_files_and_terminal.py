import pytest
import os
import time
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.api.routes_terminal import create_terminal_session


@pytest.mark.asyncio
async def test_file_manager_crud_and_security():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Get file tree
        resp = await client.get("/api/files/tree")
        assert resp.status_code == 200
        data = resp.json()
        assert data["root"] == "git_repo"
        assert isinstance(data["children"], list)

        # 2. Path traversal security check (should return 403)
        resp_sec = await client.get("/api/files/content", params={"path": "../../etc/passwd"})
        assert resp_sec.status_code == 403
        assert "Access denied" in resp_sec.json()["detail"]

        resp_sec2 = await client.get("/api/files/content", params={"path": "../backend/app/main.py"})
        assert resp_sec2.status_code == 403

        # 3. Create a new test file
        test_file_path = "scripts/_test_crud_unit.py"
        resp_create = await client.post("/api/files/create", json={"path": test_file_path, "type": "file"})
        assert resp_create.status_code == 200
        assert resp_create.json()["status"] == "created"

        # 4. Save content into the file
        sample_code = 'print("Hello from Obelisk Unit Test")\n'
        resp_save = await client.post("/api/files/save", json={"path": test_file_path, "content": sample_code})
        assert resp_save.status_code == 200
        assert resp_save.json()["status"] == "saved"

        # 5. Read back content
        resp_read = await client.get("/api/files/content", params={"path": test_file_path})
        assert resp_read.status_code == 200
        read_data = resp_read.json()
        assert read_data["content"] == sample_code
        assert read_data["extension"] == ".py"

        # 6. Rename the file
        renamed_path = "scripts/_test_crud_unit_renamed.py"
        resp_rename = await client.post("/api/files/rename", json={"old_path": test_file_path, "new_path": renamed_path})
        assert resp_rename.status_code == 200
        assert resp_rename.json()["status"] == "renamed"

        # Verify old path is gone, new path exists
        resp_old = await client.get("/api/files/content", params={"path": test_file_path})
        assert resp_old.status_code == 404

        resp_new = await client.get("/api/files/content", params={"path": renamed_path})
        assert resp_new.status_code == 200

        # 7. Delete the renamed file
        resp_del = await client.post("/api/files/delete", json={"path": renamed_path})
        assert resp_del.status_code == 200
        assert resp_del.json()["status"] == "deleted"

        # Verify deleted
        resp_after_del = await client.get("/api/files/content", params={"path": renamed_path})
        assert resp_after_del.status_code == 404


@pytest.mark.asyncio
async def test_a_save_onto_a_file_that_moved_on_disk_is_refused():
    """Two writers must not silently resolve to "whoever saved last".

    The editor sends back the ``modified_at`` it loaded. If the file changed since — another
    browser tab, a dbt run, an operator on the box — the save is refused with 409 and the
    current timestamp, so the UI can offer a reload instead of discarding the other version.
    """
    path = "scripts/_test_conflict_unit.py"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post("/api/files/create", json={"path": path, "type": "file"})
        assert created.status_code == 200
        try:
            first = await client.post(
                "/api/files/save", json={"path": path, "content": "first\n"}
            )
            assert first.status_code == 200
            loaded_stamp = first.json()["modified_at"]

            # A second writer touches the file after this editor read it.
            on_disk = settings.git_repo_path / "scripts" / "_test_conflict_unit.py"
            on_disk.write_text("someone else\n", encoding="utf-8")
            later = time.time() + 5
            os.utime(on_disk, (later, later))

            stale = await client.post(
                "/api/files/save",
                json={"path": path, "content": "mine\n", "modified_at": loaded_stamp},
            )
            assert stale.status_code == 409, stale.text
            conflict = stale.json()["detail"]
            assert conflict["error"] == "conflict"
            assert "changed on disk" in conflict["message"]
            assert conflict["modified_at"], "the client needs the current stamp to retry"
            assert on_disk.read_text(encoding="utf-8") == "someone else\n", (
                "a refused save must not have written anything"
            )

            # Reloading and saving with the current stamp succeeds.
            current = await client.get("/api/files/content", params={"path": path})
            retried = await client.post(
                "/api/files/save",
                json={
                    "path": path,
                    "content": "mine\n",
                    "modified_at": current.json()["modified_at"],
                },
            )
            assert retried.status_code == 200, retried.text

            # No stamp at all stays unconditional: that is how a new file is written.
            overwrite = await client.post(
                "/api/files/save", json={"path": path, "content": "over\n"}
            )
            assert overwrite.status_code == 200
        finally:
            await client.post("/api/files/delete", json={"path": path})


@pytest.mark.asyncio
async def test_terminal_session_lifecycle():
    cwd = str(settings.git_repo_path.resolve())
    session = create_terminal_session(cwd)
    await session.start(cols=80, rows=24)
    assert session._alive is True

    # Send a newline or echo
    await session.write("\r\n")
    # Read output
    output = await session.read()
    assert output is not None

    await session.close()
    assert session._alive is False
