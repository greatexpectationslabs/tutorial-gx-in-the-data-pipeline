"""Tests for GX Cloud helper functions."""

import pytest
import tutorial_code as tutorial


def test_check_for_gx_cloud_credentials_exist(monkeypatch):
    """Test that function raises an error when credentials are not found."""

    with pytest.raises(
        ValueError, match=r"GX_CLOUD_ORGANIZATION_ID environment variable is undefined"
    ):
        tutorial.cloud.check_for_gx_cloud_credentials_exist()

    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", "<test-org-id>")

    with pytest.raises(
        ValueError, match=r"GX_CLOUD_ACCESS_TOKEN environment variable is undefined"
    ):
        tutorial.cloud.check_for_gx_cloud_credentials_exist()

    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "<test-access-token>")

    # GX Cloud credentials are found.
    tutorial.cloud.check_for_gx_cloud_credentials_exist()
