import os

import great_expectations as gx
import pytest
import requests

TEST_PREFIX = "ci-test-"


@pytest.fixture
def tutorial_db_connection_string() -> str:
    """Return connection string for the tutorial database."""
    return "postgresql+psycopg2://gx_user:gx_user_password@postgres:5432/gx"


@pytest.fixture
def gx_cloud_context() -> (
    gx.data_context.data_context.cloud_data_context.CloudDataContext
):
    """Return CloudDataContext connecting to the GX Cloud org defined by
    GX_CLOUD_ORGANIZATION_ID and GX_CLOUD_ACCESS_TOKEN envs.
    """
    org_id = os.environ.get("GX_CLOUD_ORGANIZATION_ID")
    if org_id is None:
        raise Exception("GX_CLOUD_ORGANIZATION_ID environment variable is not defined")

    access_token = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
    if access_token is None:
        raise Exception("GX_CLOUD_ACCESS_TOKEN environment variable is not defined")

    return gx.get_context(cloud_organization_id=org_id, cloud_access_token=access_token)


@pytest.fixture
def gx_ephemeral_context() -> (
    gx.data_context.data_context.ephemeral_data_context.EphemeralDataContext
):
    """Return Ephemeral Data Context."""
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def tutorial_db_data_source(
    gx_ephemeral_context, tutorial_db_connection_string
) -> gx.datasource.fluent.sql_datasource.SQLDatasource:
    """Return Data Source that connects to the tutorial database."""
    context = gx_ephemeral_context

    DATA_SOURCE_NAME = f"{TEST_PREFIX}tutorial-db"

    try:
        data_source = context.data_sources.add_postgres(
            name=DATA_SOURCE_NAME,
            connection_string=tutorial_db_connection_string,
        )

    except gx.exceptions.DataContextError:
        data_source = context.get_datasource(DATA_SOURCE_NAME)

    return data_source, context


@pytest.fixture
def airflow_api_healthcheck():
    """Check that the local airflow api is available."""

    def run_healthcheck():
        retry = requests.adapters.Retry(
            total=10,
            connect=5,
            read=5,
            allowed_methods=["GET"],
            backoff_factor=10,
            status_forcelist=[500, 502, 503, 504],
        )
        session = requests.Session()
        session.auth = ("admin", "gx")
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))

        response = session.get("http://airflow:8080/api/v1/dags")

        return True

    return run_healthcheck()
