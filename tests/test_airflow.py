"""Tests for Airflow-related helper functions."""

import time

import pytest
import requests
import tutorial_code as tutorial


@pytest.fixture
def wait_on_airflow_api_healthcheck():
    """Check that the local Airflow API is available using retry."""

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

        return response.json()

    response = run_healthcheck()

    if response.get("dags", None) is None:
        raise Exception("Unable to reach local Airflow API.")


def test_airflow_dag_trigger(wait_on_airflow_api_healthcheck):
    """Test that triggering an Airflow DAG runs without error."""

    wait_on_airflow_api_healthcheck

    # Use Cookbook 1 DAG as test case.
    # Remove existing db rows to verify that new rows were inserted after DAG run.
    tutorial.db.drop_all_table_rows("customers")
    assert tutorial.db.get_table_row_count("customers") == 0

    dag_id = "cookbook1_validate_and_ingest_to_postgres"
    dag_run_id, _ = tutorial.airflow.trigger_airflow_dag(dag_id)

    dag_run_completed = tutorial.airflow.dag_run_completed(dag_id, dag_run_id)
    dag_run_completion_checks = 1

    # Wait for the DAG to finish running before test continues.
    while not dag_run_completed:
        time.sleep(dag_run_completion_checks * 10)
        dag_run_completed = tutorial.airflow.dag_run_completed(dag_id, dag_run_id)
        dag_run_completion_checks += 1
        if dag_run_completion_checks == 4:
            raise Exception(f"Test DAG is still running: {dag_id}")

    assert tutorial.db.get_table_row_count("customers") == 15266
