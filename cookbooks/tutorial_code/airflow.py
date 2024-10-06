"""Helper functions for tutorial notebooks to interact with Airflow."""

import uuid
import warnings
from typing import Tuple

import airflow_client.client
from airflow_client.client.api import dag_api, dag_run_api
from airflow_client.client.model.dag_run import DAGRun

AIRFLOW_HOST = "http://airflow:8080/api/v1"


def trigger_airflow_dag(dag_id: str) -> Tuple[str, str]:
    """Trigger a tutorial Airflow DAG.

    Args:
        dag_id: string identifier of the Airflow dag

    Returns:
        Tuple containing the (dag run id, dag run state)
    """
    config = airflow_client.client.Configuration(
        host=AIRFLOW_HOST, username="admin", password="gx"
    )

    with warnings.catch_warnings():
        # Suppress DeprecationWarnings caused by airflow library code.
        warnings.simplefilter("ignore", category=DeprecationWarning)

        with airflow_client.client.ApiClient(config) as api_client:
            dag_api_instance = dag_api.DAGApi(api_client)

            try:
                api_response = dag_api_instance.get_dags()
            except airflow_client.client.OpenApiException as e:
                raise Exception(f"Exception when calling DagAPI->get_dags: {e}")

            # Check that requested DAG exists.
            dags_by_id = [x["dag_id"] for x in api_response["dags"]]
            if dag_id not in dags_by_id:
                raise Exception(f"DAG {dag_id} not found.")

            # Post the DAG run.
            dag_run_api_instance = dag_run_api.DAGRunApi(api_client)

            dag_run_id = DAGRun(dag_run_id=f"{dag_id}_{uuid.uuid4().hex}")
            api_response = dag_run_api_instance.post_dag_run(dag_id, dag_run_id)
            dag_run_state = api_response["state"]

        return dag_run_id["dag_run_id"], dag_run_state
