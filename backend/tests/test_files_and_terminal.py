import pytest
import os
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
