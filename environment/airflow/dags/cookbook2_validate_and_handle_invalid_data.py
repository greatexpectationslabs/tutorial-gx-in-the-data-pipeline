import datetime
import logging
import os
import pathlib

import pandas as pd
import tutorial_code as tutorial
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("GX validation")


def get_airflow_home_dir() -> pathlib.Path:
    return pathlib.Path(os.getenv("AIRFLOW_HOME"))


def cookbook2_validate_and_handle_invalid_data():

    RAW_DATA_DIR = get_airflow_home_dir() / "data/raw"
    OUTPUT_DATA_DIR = get_airflow_home_dir() / "airflow_pipeline_output"

    # Load and clean raw product data.
    df_products_raw = pd.read_csv(
        RAW_DATA_DIR / "products.csv", encoding="unicode_escape"
    )

    df_products, df_product_categories, df_product_subcategories = (
        tutorial.cookbook2.clean_product_data(df_products_raw)
    )

    # Validate product data using GX.
    (
        products_validation_result,
        product_category_validation_result,
        product_subcategory_validation_result,
    ) = tutorial.cookbook2.validate_product_data(
        df_products, df_product_categories, df_product_subcategories
    )

    # Halt pipeline with error if validation fails for product category or subcategory results.
    if not product_category_validation_result["success"]:
        raise Exception("GX data validation for product categories failed.")

    if not product_subcategory_validation_result["success"]:
        raise Exception("GX data validation for product subcategories failed.")

    # Write product category and subcategory data to Postgres tables.
    product_category_rows_inserted = tutorial.db.insert_ignore_dataframe_to_postgres(
        table_name="product_category", dataframe=df_product_categories
    )

    log.info(f"{product_category_rows_inserted} new product category rows inserted.")

    product_subcategory_rows_inserted = tutorial.db.insert_ignore_dataframe_to_postgres(
        table_name="product_subcategory", dataframe=df_product_subcategories
    )

    log.info(
        f"{product_subcategory_rows_inserted} new product subcategory rows inserted."
    )

    # If validation fails for product rows, automatically remove failing rows and write
    # to error file. Write all remaining valid rows to Postgres.
    if not products_validation_result["success"]:
        df_products_valid, df_products_invalid = (
            tutorial.cookbook2.separate_valid_and_invalid_product_rows(
                df_products, products_validation_result
            )
        )
        tutorial.cookbook2.write_invalid_rows_to_file(
            OUTPUT_DATA_DIR / "cookbook2_invalid_product_rows.csv", df_products_invalid
        )

    else:
        df_products_valid = df_products

    product_rows_inserted = tutorial.db.insert_ignore_dataframe_to_postgres(
        table_name="products", dataframe=df_products_valid
    )

    log.info(f"{product_rows_inserted} new product rows inserted.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime.today(),
}

gx_dag = DAG(
    "cookbook2_validate_and_handle_invalid_data",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
)

run_gx_task = PythonOperator(
    task_id="cookbook2_validate_and_handle_invalid_data",
    python_callable=cookbook2_validate_and_handle_invalid_data,
    dag=gx_dag,
)

run_gx_task
