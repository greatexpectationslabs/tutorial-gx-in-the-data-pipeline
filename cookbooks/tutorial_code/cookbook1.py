"""Helper functions for Cookbook 1 notebook and DAG."""

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd


def clean_customer_data(df_original: pd.DataFrame) -> pd.DataFrame:
    """Clean sample customer data for Cookbook 1."""

    # Generate a separate copy of original data to transform.
    df = df_original.copy()

    # Rename original columns.
    RENAME_COLUMNS = {
        "CustomerKey": "customer_id",
        "Gender": "gender",
        "Name": "name",
        "City": "city",
        "State Code": "state",
        "Zip Code": "zip",
        "Country": "country",
        "Continent": "continent",
        "Birthday": "dob",
    }

    df = df.rename(columns=RENAME_COLUMNS)

    # Clean and standardize customer data.
    COUNTRY_NAME_TO_CODE = {
        "Australia": "AU",
        "Canada": "CA",
        "Germany": "DE",
        "France": "FR",
        "Italy": "IT",
        "Netherlands": "NL",
        "United Kingdom": "GB",
        "United States": "US",
    }

    df["country"] = df["country"].apply(lambda x: COUNTRY_NAME_TO_CODE[x])
    df["city"] = df["city"].apply(lambda x: x.title())

    # Format final dataframe.
    RETAIN_COLUMNS = ["customer_id", "name", "city", "state", "zip", "country"]
    df = df[RETAIN_COLUMNS]

    return df


def validate_customer_data(
    df_customers: pd.DataFrame,
) -> gx.core.expectation_validation_result.ExpectationSuiteValidationResult:
    """Run GX data validation on sample customer data for Cookbook 1 and DAG, and return Validation Result."""

    # Get GX context.
    context = gx.get_context(mode="ephemeral")

    # Create Data Source, Data Asset, Batch Definition, and get Batch.
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="customer data")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "batch definition"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df_customers})

    # Create Expectation Suite and add Expectations.
    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="customer expectations")
    )

    expectations = [
        gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=[
                "customer_id",
                "name",
                "city",
                "state",
                "zip",
                "country",
            ]
        ),
        gxe.ExpectColumnValuesToBeOfType(column="customer_id", type_="int"),
        *[
            gxe.ExpectColumnValuesToBeOfType(column=x, type_="str")
            for x in ["name", "city", "state", "zip"]
        ],
        gxe.ExpectColumnValuesToBeInSet(
            column="country", value_set=["AU", "CA", "DE", "FR", "GB", "IT", "NL", "US"]
        ),
    ]

    for expectation in expectations:
        expectation_suite.add_expectation(expectation)

    # Validate Batch using Expectation Suite and return result.
    return batch.validate(expectation_suite)
