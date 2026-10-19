import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.sdk.obelisk import var as obelisk_var


@pytest.mark.asyncio
async def test_variables_lifecycle_and_vault_encryption():
    """
    Real integration test for Obelisk Variables & Secrets Engine:
    - Verifies default seeded variables in SQLite
    - Creates plaintext and AES-256 Fernet encrypted variables
    - Verifies masking on list endpoint
    - Verifies reveal endpoint decrypts accurately
    - Verifies Python SDK obelisk.var() reads and decrypts directly
    - Verifies deletion cleanup
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Fetch variables list (auto-seeds defaults if not present)
        res = await client.get("/api/variables")
        assert res.status_code == 200
        vars_list = res.json()
        assert len(vars_list) >= 3
        keys = [v["key"] for v in vars_list]
        assert "ENV" in keys
        assert "DEFAULT_CURRENCY" in keys

        # 2. Create a plaintext variable
        plain_payload = {
            "key": "INTEGRATION_TEST_PARAM",
            "value": "batch_mode_v2",
            "is_secret": False,
            "description": "Integration test batch parameter",
        }
        create_res = await client.post("/api/variables", json=plain_payload)
        assert create_res.status_code == 200
        plain_data = create_res.json()
        assert plain_data["key"] == "INTEGRATION_TEST_PARAM"
        assert plain_data["value"] == "batch_mode_v2"
        assert plain_data["is_secret"] is False

        # 3. Create an AES-256 encrypted secret
        secret_payload = {
            "key": "INTEGRATION_TEST_SECRET_KEY",
            "value": "super-confidential-token-9988",
            "is_secret": True,
            "description": "Integration test encrypted secret",
        }
        secret_res = await client.post("/api/variables", json=secret_payload)
        assert secret_res.status_code == 200
        secret_data = secret_res.json()
        var_id = secret_data["id"]
        assert secret_data["key"] == "INTEGRATION_TEST_SECRET_KEY"
        # Secret value MUST be masked in the returned payload
        assert secret_data["value"] == "••••••••••••"
        assert secret_data["is_secret"] is True

        # 4. Verify list endpoint masks secrets
        list_res = await client.get("/api/variables")
        assert list_res.status_code == 200
        found_secret = next(v for v in list_res.json() if v["key"] == "INTEGRATION_TEST_SECRET_KEY")
        assert found_secret["value"] == "••••••••••••"

        # 5. Reveal secret via authorized UI endpoint
        reveal_res = await client.get("/api/variables/INTEGRATION_TEST_SECRET_KEY/reveal")
        assert reveal_res.status_code == 200
        assert reveal_res.json()["value"] == "super-confidential-token-9988"

        # 6. Verify Python SDK obelisk.var() retrieves decrypted value
        sdk_val = obelisk_var("INTEGRATION_TEST_SECRET_KEY")
        assert sdk_val == "super-confidential-token-9988"

        plain_sdk_val = obelisk_var("INTEGRATION_TEST_PARAM")
        assert plain_sdk_val == "batch_mode_v2"

        fallback_val = obelisk_var("NON_EXISTING_KEY_XYZ", default="fallback_val")
        assert fallback_val == "fallback_val"

        # 7. Delete the created variables
        del_res1 = await client.delete(f"/api/variables/{var_id}")
        assert del_res1.status_code == 200
        assert del_res1.json()["status"] == "deleted"

        del_res2 = await client.delete(f"/api/variables/{plain_data['id']}")
        assert del_res2.status_code == 200
