"""Helper functions for tutorial notebooks to interact with Airflow."""

import datetime
import time
import uuid
import warnings
from typing import Tuple, Union

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


def get_airflow_dag_run_status(
    dag_id: str, dag_run_id: str
) -> Tuple[str, Union[datetime.datetime, None]]:
    """Get the run status of a tutorial Airflow DAG.

    Args:
        dag_id: string identifier of the Airflow dag
        dag_run_id: string identifier of the Airflow dag run

    Returns:
        Tuple containing (dag run status, dag run end datetime)
    """
    config = airflow_client.client.Configuration(
        host=AIRFLOW_HOST, username="admin", password="gx"
    )

    with warnings.catch_warnings():
        # Suppress DeprecationWarnings caused by airflow library code.
        warnings.simplefilter("ignore", category=DeprecationWarning)

        with airflow_client.client.ApiClient(config) as api_client:
            dag_run_api_instance = dag_run_api.DAGRunApi(api_client)

            try:
                api_response = dag_run_api_instance.get_dag_run(
                    dag_id=dag_id, dag_run_id=dag_run_id
                )
            except airflow_client.client.OpenApiException as e:
                raise Exception(f"Error calling DAGRunApi->get_dag_run: {e}")

            dag_run_status = api_response["state"]
            dag_run_end_date = api_response["end_date"]

            return dag_run_status, dag_run_end_date


def dag_run_completed(dag_id: str, dag_run_id: str) -> bool:
    """Returns whether DAG run has completed.

    Args:
        dag_id: string identifier of the Airflow dag
        dag_run_id: string identifier of the Airflow dag run

    Returns:
        True if dag has completed running, False otherwise
    """
    dag_run_status, dag_run_end_date = get_airflow_dag_run_status(dag_id, dag_run_id)

    if (dag_run_end_date is not None) and dag_run_status not in ["queued", "running"]:
        return True
    else:
        return False


def trigger_airflow_dag_and_wait_for_run(dag_id: str) -> None:
    """Trigger a tutorial Airflow DAG and wait for it to run.

    Args:
        dag_id: string identifier of the Airflow dag
    """
    dag_run_id, _ = trigger_airflow_dag(dag_id)

    dag_run_finished = dag_run_completed(dag_id, dag_run_id)
    dag_run_completion_checks = 1

    while not dag_run_finished:
        time.sleep(dag_run_completion_checks * 10)
        dag_run_finished = dag_run_completed(dag_id, dag_run_id)
        dag_run_completion_checks += 1
        if dag_run_completion_checks == 4:
            raise Exception(f"Test DAG is still running: {dag_id}")
