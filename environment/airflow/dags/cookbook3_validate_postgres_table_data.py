import datetime
import logging
import great_expectations as gx

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("GX validation")


def cookbook3_validate_postgres_table_data():

    # Fetch and run the GX Cloud Checkpoint.
    context = gx.get_context()

    checkpoint = context.checkpoints.get("Customer profile checkpoint")

    checkpoint_result = checkpoint.run()

    # Extract and log the validation result and results url.
    validation_result = checkpoint_result.run_results[
        list(checkpoint_result.run_results.keys())[0]
    ]

    if validation_result["success"]:
        log.info(f"Validation succeeded: {validation_result.result_url}")
    else:
        log.warning(f"Validation failed: {validation_result.result_url}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.today(),
}

gx_dag = DAG(
    "cookbook3_validate_postgres_table_data",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
)

run_gx_task = PythonOperator(
    task_id="cookbook3_validate_postgres_table_data",
    python_callable=cookbook3_validate_postgres_table_data,
    dag=gx_dag,
)

run_gx_task
