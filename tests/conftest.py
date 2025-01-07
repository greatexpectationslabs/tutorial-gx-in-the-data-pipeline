import pytest


@pytest.fixture(autouse=True)
def remove_gx_cloud_envvars(monkeypatch):
    """Set testing environment to not contain GX Cloud credentials by default."""
    monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID", raising=False)
    monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN", raising=False)
