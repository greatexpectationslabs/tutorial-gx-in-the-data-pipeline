"""Helper functions for tutorial notebooks to interact with GX Cloud."""

import os


def gx_cloud_credentials_exist() -> bool:
    """Checks for the presence of the GX Cloud organization id and access token.

    * The organization id should be provided in the GX_CLOUD_ORGANIZATION_ID environment variable.
    * The access token should be provided in the GX_CLOUD_ACCESS_TOKEN environment variable.

    Raises:
        ValueError if either the GX_CLOUD_ORGANIZATION_ID or GX_CLOUD_ACCESS_TOKEN environment variable is undefined or contains a null/empty string value.

    Returns:
        True if credentials are found
    """

    # Check for organization id.
    organization_id = os.getenv("GX_CLOUD_ORGANIZATION_ID", None)
    if (organization_id is None) or (organization_id == ""):
        raise ValueError(
            "GX_CLOUD_ORGANIZATION_ID environment variable is undefined. Use this environment variable to provide your GX Cloud organization id."
        )

    # Check for access token.
    access_token = os.getenv("GX_CLOUD_ACCESS_TOKEN", None)
    if (access_token is None) or (access_token == ""):
        raise ValueError(
            "GX_CLOUD_ACCESS_TOKEN environment variable is undefined. Use this environment variable to provide your GX Cloud access token."
        )

    return True
