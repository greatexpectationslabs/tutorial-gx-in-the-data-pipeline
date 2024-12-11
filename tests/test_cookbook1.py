"""Tests for Cookbook1 functions."""

import datetime
import shutil
import time

import cookbooks.airflow_dags.cookbook1_ingest_customer_data as airflow_dag
import great_expectations as gx
import pandas as pd
import pytest
import tutorial_code as tutorial


@pytest.fixture
def raw_customer_data() -> pd.DataFrame:
    customer_data = [
        [
            1693133,
            "Male",
            "Samuel Hall",
            "Norcross",
            "GA",
            "Georgia",
            "30091",
            "United States",
            "North America",
            "11/19/1976",
        ],
        [
            887837,
            "Female",
            "Ileen van Dael",
            "Utrecht",
            "UT",
            "Utrecht",
            "3532 XR",
            "Netherlands",
            "Europe",
            "9/15/1983",
        ],
    ]

    customer_columns = [
        "CustomerKey",
        "Gender",
        "Name",
        "City",
        "State Code",
        "State",
        "Zip Code",
        "Country",
        "Continent",
        "Birthday",
    ]

    return pd.DataFrame(customer_data, columns=customer_columns)


@pytest.fixture
def valid_cleaned_customer_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "customer_id": 123,
                "name": "Cookie Monster",
                "dob": datetime.datetime(1966, 11, 2),
                "city": "New York",
                "state": "NY",
                "zip": "10123",
                "country": "US",
            }
        ]
    )


@pytest.fixture
def invalid_cleaned_customer_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "customer_id": 987,
                "name": "Oscar the Grouch",
                "dob": "June 1, 1969",
                "city": "New York",
                "state": "NY",
                "zip": None,
                "country": "US",
            }
        ]
    )


def test_clean_customer_data(raw_customer_data):
    df_cleaned = tutorial.cookbook1.clean_customer_data(raw_customer_data)

    assert df_cleaned.shape == (2, 7)

    customer0 = df_cleaned.to_dict(orient="records")[0]
    assert customer0 == {
        "customer_id": 1693133,
        "name": "Samuel Hall",
        "dob": pd.Timestamp("1976-11-19"),
        "city": "Norcross",
        "state": "GA",
        "zip": "30091",
        "country": "US",
    }

    customer1 = df_cleaned.to_dict(orient="records")[1]
    assert customer1 == {
        "customer_id": 887837,
        "name": "Ileen van Dael",
        "dob": pd.Timestamp("1983-09-15"),
        "city": "Utrecht",
        "state": "UT",
        "zip": "3532 XR",
        "country": "NL",
    }


def test_validate_customer_data_with_valid_data(valid_cleaned_customer_data):
    """Test validate_customer_data succeeds on valid data."""
    validation_result = tutorial.cookbook1.validate_customer_data(
        valid_cleaned_customer_data
    )
    assert validation_result["success"] is True
    assert isinstance(
        validation_result,
        gx.core.expectation_validation_result.ExpectationSuiteValidationResult,
    )


def test_validate_customer_data_with_invalid_data(invalid_cleaned_customer_data):
    """Test validate_customer_data fails on invalid data and correctly flags failing Expectation."""
    validation_result = tutorial.cookbook1.validate_customer_data(
        invalid_cleaned_customer_data
    )

    failed_expectations = sorted(
        [
            x["expectation_config"]["type"]
            for x in validation_result["results"]
            if x["success"] is not True
        ]
    )

    assert isinstance(
        validation_result,
        gx.core.expectation_validation_result.ExpectationSuiteValidationResult,
    )
    assert validation_result["success"] is False
    assert failed_expectations == ["expect_column_values_to_match_regex"]


def test_airflow_dag_trigger(tmp_path, monkeypatch):
    """Test Airflow DAG code runs without error."""

    # Create tmp directories for test data.
    (tmp_path / "data" / "raw").mkdir(parents=True)

    # Write pipeline invalid row output to tmp directory.
    def mock_get_airflow_home_dir():
        return tmp_path

    monkeypatch.setattr(airflow_dag, "get_airflow_home_dir", mock_get_airflow_home_dir)

    # Add product data to tmp directory.
    source_file = "/cookbooks/data/raw/customers.csv"
    destination_directory = tmp_path / "data/raw"

    shutil.copy(source_file, destination_directory)

    tutorial.db.drop_all_table_rows("customers")
    assert tutorial.db.get_table_row_count("customers") == 0

    airflow_dag.cookbook1_validate_and_ingest_to_postgres()

    assert tutorial.db.get_table_row_count("customers") == 15266
