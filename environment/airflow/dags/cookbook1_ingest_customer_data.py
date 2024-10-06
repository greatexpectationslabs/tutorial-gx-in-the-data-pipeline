import datetime
import os
import pathlib

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

import tutorial_code as tutorial


def cookbook1_validate_and_ingest_to_postgres():

    DATA_DIR = pathlib.Path(os.getenv("AIRFLOW_HOME")) / "data/raw"

    # Load and clean raw customer data.
    df_customers_raw = pd.read_csv(
        DATA_DIR / "customers.csv", encoding="unicode_escape"
    )
    df_customers = tutorial.cookbook1.clean_customer_data(df_customers_raw)

    # Validate customer data using GX.
    validation_result = tutorial.cookbook1.validate_customer_data(df_customers)

    # Halt pipeline with error if validation fails.
    if not validation_result["success"]:
        raise Exception("GX data validation failed.")

    # Write data to Postgres table.
    rows_inserted = tutorial.db.insert_ignore_dataframe_to_postgres(
        table_name="customers", dataframe=df_customers
    )

    print(f"{rows_inserted} new rows inserted.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.today(),
}

gx_dag = DAG(
    "cookbook1_validate_and_ingest_to_postgres",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
)

run_gx_task = PythonOperator(
    task_id="cookbook1_validate_and_ingest_to_postgres",
    python_callable=cookbook1_validate_and_ingest_to_postgres,
    dag=gx_dag,
)

run_gx_task
