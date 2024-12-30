"""Tests for GX Cloud helper functions."""

import pytest
import tutorial_code as tutorial


def test_check_for_gx_cloud_credentials_exist(monkeypatch):
    """Test that function raises an error when credentials are not found."""

    # monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID", raising=False)
    # monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN", raising=False)

    with pytest.raises(
        ValueError, match=r"GX_CLOUD_ORGANIZATION_ID environment variable is undefined"
    ):
        tutorial.cloud.check_for_gx_cloud_credentials_exist()

    # monkeypatch.setenv("API_AUDIENCE", "https://mock.com")
