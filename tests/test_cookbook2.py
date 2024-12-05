"""Tests for Cookbook2 functions."""

import time
from typing import Tuple

import great_expectations as gx
import pandas as pd
import pytest

import tutorial_code as tutorial


@pytest.fixture
def raw_product_data(tmp_path) -> pd.DataFrame:
    """Return sample raw product data for tests."""

    TEST_CSV_FILENAME = "test_product_data.csv"

    test_data = [
        "ProductKey,Product Name,Brand,Color,Unit Cost USD,Unit Price USD,SubcategoryKey,Subcategory,CategoryKey,Category",
        "1,Contoso 512MB MP3 Player E51 Silver,Contoso,Silver,$6.62 ,$12.99 ,0101,MP4&MP3,01,Audio",
        '374,Adventure Works Laptop19W X1980 Silver,Adventure Works,Silver,$430.38 ,"$1,299.00 ",0301,Laptops,03,Computers',
        '657,Proseware Duplex Scanner M200 Black,Proseware,Black,$68.52 ,$149.00 ,0306,"Printers, Scanners & Fax",03,Computers',
    ]

    with open(tmp_path / TEST_CSV_FILENAME, "w") as fh:
        for line in test_data:
            fh.write(f"{line}\n")

    return pd.read_csv(tmp_path / TEST_CSV_FILENAME)


@pytest.fixture
def valid_product_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return cleaned, valid product and (sub)category data."""

    valid_products = [
        {
            "product_id": 2486,
            "name": "Litware 18'' Oscillating Pedestal Fan M145 Blue",
            "brand": "Litware",
            "color": "Blue",
            "unit_cost_usd": 183.95,
            "unit_price_usd": 400.00,
            "product_category_id": 8,
            "product_subcategory_id": 808,
        },
        {
            "product_id": 2467,
            "name": "Litware 80mm LED Dual PCI Slot Fan E1501 Yellow",
            "brand": "Litware",
            "color": "Yellow",
            "unit_cost_usd": 15.80,
            "unit_price_usd": 30.99,
            "product_category_id": 8,
            "product_subcategory_id": 808,
        },
    ]

    valid_product_categories = [
        {"product_category_id": 1, "name": "category 1"},
        {"product_category_id": 2, "name": "category 2"},
    ]

    valid_product_subcategories = [
        {"product_subcategory_id": 101, "name": "subcategory 101"},
        {"product_subcategory_id": 201, "name": "subcategory 201"},
    ]

    return (
        pd.DataFrame(valid_products),
        pd.DataFrame(valid_product_categories),
        pd.DataFrame(valid_product_subcategories),
    )


@pytest.fixture
def invalid_product_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return cleaned, invalid product and (sub)category data."""

    invalid_products = [
        {
            "product_id": 1934,
            "name": "Fabrikam Refrigerator 19CuFt M7600 Orange",
            "brand": "Fabrikam",
            "color": "Orange",
            "unit_cost_usd": 913.42,
            "unit_price_usd": 499.09,
            "product_category_id": 8,
            "product_subcategory_id": 802,
        },
        {
            "product_id": 2133,
            "name": "Contoso Coffee Maker 4C E0800 White",
            "brand": "Contoso",
            "color": "White",
            "unit_cost_usd": 0.62,
            "unit_price_usd": 0.26,
            "product_category_id": 8,
            "product_subcategory_id": 805,
        },
        {
            "product_id": 1234,
            "name": "Adventure Works Coffee Maker 12C M100 Silver",
            "brand": "Adventure Works",
            "color": "Silver",
            "unit_cost_usd": 0.56,
            "unit_price_usd": 0.99,
            "product_category_id": 8,
            "product_subcategory_id": 805,
        },
    ]

    invalid_product_categories = [
        {"product_category_id": 3, "name": "category 3", "extra_col": "invalid schema"},
        {"product_category_id": 4, "name": "category 4", "extra_col": "invalid schema"},
    ]

    invalid_product_subcategories = [
        {"subcategory_id": "378", "name": "subcategory 378"},
        {"subcategory_id": "379", "name": "subcategory 379"},
    ]

    return (
        pd.DataFrame(invalid_products),
        pd.DataFrame(invalid_product_categories),
        pd.DataFrame(invalid_product_subcategories),
    )


def test_clean_product_data(raw_product_data):
    """Test that raw product data is cleaned and normalized as expected."""

    df_products, df_product_categories, df_product_subcategories = (
        tutorial.cookbook2.clean_product_data(raw_product_data)
    )

    assert df_products.shape == (3, 8)
    assert df_product_categories.shape == (2, 2)
    assert df_product_subcategories.shape == (3, 2)

    # Products.
    expected_products = [
        {
            "product_id": 1,
            "name": "Contoso 512MB MP3 Player E51 Silver",
            "brand": "Contoso",
            "color": "Silver",
            "unit_cost_usd": 6.62,
            "unit_price_usd": 12.99,
            "product_category_id": 1,
            "product_subcategory_id": 101,
        },
        {
            "product_id": 374,
            "name": "Adventure Works Laptop19W X1980 Silver",
            "brand": "Adventure Works",
            "color": "Silver",
            "unit_cost_usd": 430.38,
            "unit_price_usd": 1299.00,
            "product_category_id": 3,
            "product_subcategory_id": 301,
        },
        {
            "product_id": 657,
            "name": "Proseware Duplex Scanner M200 Black",
            "brand": "Proseware",
            "color": "Black",
            "unit_cost_usd": 68.52,
            "unit_price_usd": 149.00,
            "product_category_id": 3,
            "product_subcategory_id": 306,
        },
    ]

    assert df_products.to_dict(orient="records") == expected_products

    # Product categories.
    expected_product_categories = [
        {"product_category_id": 1, "name": "Audio"},
        {"product_category_id": 3, "name": "Computers"},
    ]

    assert (
        df_product_categories.to_dict(orient="records") == expected_product_categories
    )

    # Product subcategories.
    expected_product_subcategories = [
        {"product_subcategory_id": 101, "name": "MP4&MP3"},
        {"product_subcategory_id": 301, "name": "Laptops"},
        {"product_subcategory_id": 306, "name": "Printers, Scanners & Fax"},
    ]

    assert (
        df_product_subcategories.to_dict(orient="records")
        == expected_product_subcategories
    )


def test_validate_valid_data(valid_product_data):
    """Test that validation of valid data succeeds as expected."""

    df_products, df_product_categories, df_product_subcategories = valid_product_data

    (
        products_validation_result,
        product_category_validation_result,
        product_subcategory_validation_result,
    ) = tutorial.cookbook2.validate_product_data(
        df_products, df_product_categories, df_product_subcategories
    )

    for result in [
        products_validation_result,
        product_category_validation_result,
        product_subcategory_validation_result,
    ]:
        assert isinstance(
            result,
            gx.core.expectation_validation_result.ExpectationSuiteValidationResult,
        )
        assert result["success"] is True


def test_validate_invalid_data(invalid_product_data):
    """Test that validation of invalid data fails as expected."""

    df_products, df_product_categories, df_product_subcategories = invalid_product_data

    (
        products_validation_result,
        product_category_validation_result,
        product_subcategory_validation_result,
    ) = tutorial.cookbook2.validate_product_data(
        df_products, df_product_categories, df_product_subcategories
    )

    for result in [
        products_validation_result,
        product_category_validation_result,
        product_subcategory_validation_result,
    ]:
        assert isinstance(
            result,
            gx.core.expectation_validation_result.ExpectationSuiteValidationResult,
        )
        assert result["success"] is False


def test_separate_valid_and_invalid_product_rows(
    valid_product_data, invalid_product_data
):
    """Test that valid and invalid rows are separated into two dataframes as expected."""

    df_products_valid, _, _ = valid_product_data
    df_products_invalid, _, _ = invalid_product_data

    df_products = pd.concat(
        [df_products_valid, df_products_invalid], axis=0
    ).reset_index(drop=True)

    context = gx.get_context()
    context.data_sources.add_pandas("pandas")

    validation_result = tutorial.cookbook2._validate_products(context, df_products)

    df_valid, df_invalid = tutorial.cookbook2.separate_valid_and_invalid_product_rows(
        df_products, validation_result
    )

    assert sorted(list(df_valid["product_id"])) == [2467, 2486]
    assert sorted(list(df_invalid["product_id"])) == [1234, 1934, 2133]


def test_airflow_dag_trigger(wait_on_airflow_api_healthcheck):
    """Test Airflow DAG trigger runs without error."""

    wait_on_airflow_api_healthcheck

    expected_table_row_count = {
        "products": 2510,
        "product_category": 8,
        "product_subcategory": 32,
    }

    for table_name in expected_table_row_count.keys():
        tutorial.db.drop_all_table_rows(table_name)
        assert tutorial.db.get_table_row_count(table_name) == 0

    dag_id = "cookbook2_validate_and_ingest_to_postgres_handle_invalid_data"
    dag_run_id, _ = tutorial.airflow.trigger_airflow_dag(dag_id)

    dag_run_completed = tutorial.airflow.dag_run_completed(dag_id, dag_run_id)

    # Wait for the DAG to finish running before test continues.
    while not dag_run_completed:
        time.sleep(10)
        dag_run_completed = tutorial.airflow.dag_run_completed(dag_id, dag_run_id)

    for table_name in expected_table_row_count.keys():
        assert (
            tutorial.db.get_table_row_count(table_name)
            == expected_table_row_count[table_name]
        )
