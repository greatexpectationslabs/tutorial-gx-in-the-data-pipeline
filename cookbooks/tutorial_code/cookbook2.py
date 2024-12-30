"""Helper functions for Cookbook 2 notebook and DAG."""

import logging
import pathlib
from typing import Tuple

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

log = logging.getLogger("GX validation")

DATA_SOURCE_NAME = "pandas"

# Define short name types to keep function type hints cleaner.
GxDataContext = gx.data_context.data_context.ephemeral_data_context.EphemeralDataContext
GxValidationResult = (
    gx.core.expectation_validation_result.ExpectationSuiteValidationResult
)
GxCheckpointResult = gx.checkpoint.checkpoint.CheckpointResult


def _extract_validation_result_from_checkpoint_result(
    checkpoint_result: GxCheckpointResult,
) -> GxValidationResult:
    """Helper function that extracts the first Validation Result from a Checkpoint run result."""
    validation_result = checkpoint_result.run_results[
        list(checkpoint_result.run_results.keys())[0]
    ]
    return validation_result


def clean_product_data(
    df_original: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Clean sample product data for Cookbook 2.

    Returns:
        Tuple of pandas dataframes: product data, product categories, product subcategories
    """

    # Generate a separate copy of original data to transform.
    df_products = df_original.copy()

    # Rename original columns.
    RENAME_COLUMNS = {
        "ProductKey": "product_id",
        "Product Name": "name",
        "Brand": "brand",
        "Color": "color",
        "Unit Cost USD": "unit_cost_usd",
        "Unit Price USD": "unit_price_usd",
        "SubcategoryKey": "product_subcategory_id",
        "Subcategory": "product_subcategory_name",
        "CategoryKey": "product_category_id",
        "Category": "product_category_name",
    }

    df_products = df_products.rename(columns=RENAME_COLUMNS)

    # Clean cost and price figures.
    for currency_col in ["unit_cost_usd", "unit_price_usd"]:
        df_products[currency_col] = df_products[currency_col].apply(
            lambda x: float(x.replace("$", "").replace(",", "").strip())
        )

    # Generate product category and subcategory dataframes.
    df_product_categories = (
        df_products[["product_category_id", "product_category_name"]]
        .copy()
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"product_category_name": "name"})
    )

    df_product_subcategories = (
        df_products[["product_subcategory_id", "product_subcategory_name"]]
        .copy()
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"product_subcategory_name": "name"})
    )

    # Format final product dataframe.
    PRODUCT_RETAIN_COLUMNS = [
        "product_id",
        "name",
        "brand",
        "color",
        "unit_cost_usd",
        "unit_price_usd",
        "product_category_id",
        "product_subcategory_id",
    ]

    df_products = df_products[PRODUCT_RETAIN_COLUMNS]

    return df_products, df_product_categories, df_product_subcategories


def _validate_products(context: GxDataContext, df: pd.DataFrame) -> GxValidationResult:
    """Validate sample product data.

    Args:
        context: GX Data Context
        df: pandas dataframe containing product data

    Returns:
        GX Validation Result object containing result metadata
    """

    data_source = context.data_sources.get(DATA_SOURCE_NAME)

    data_asset = data_source.add_dataframe_asset(name="products")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "products batch definition"
    )

    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="product expectations")
    )

    expectations = [
        gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=[
                "product_id",
                "name",
                "brand",
                "color",
                "unit_cost_usd",
                "unit_price_usd",
                "product_category_id",
                "product_subcategory_id",
            ]
        ),
        gxe.ExpectColumnValuesToBeBetween(column="unit_price_usd", min_value=1.0),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="unit_price_usd", column_B="unit_cost_usd"
        ),
    ]

    for expectation in expectations:
        expectation_suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="products validation definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="products checkpoint",
            validation_definitions=[validation_definition],
            result_format={
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["product_id"],
            },
        )
    )

    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

    return _extract_validation_result_from_checkpoint_result(checkpoint_result)


def _validate_product_categories(
    context: GxDataContext, df: pd.DataFrame
) -> GxValidationResult:
    """Validate sample product category data.

    Args:
        context: GX Data Context
        df: pandas dataframe containing product category data

    Returns:
        GX Validation Result object containing result metadata
    """

    data_source = context.data_sources.get(DATA_SOURCE_NAME)

    data_asset = data_source.add_dataframe_asset(name="product categories")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "product category batch definition"
    )

    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="product category expectations")
    )

    expectations = [
        gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=["product_category_id", "name"]
        )
    ]

    for expectation in expectations:
        expectation_suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="product category validation definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="product category checkpoint",
            validation_definitions=[validation_definition],
            result_format={
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["product_category_id"],
            },
        )
    )

    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

    return _extract_validation_result_from_checkpoint_result(checkpoint_result)


def _validate_product_subcategories(
    context: GxDataContext, df: pd.DataFrame
) -> GxValidationResult:
    """Validate sample product subcategory data.

    Args:
        context: GX Data Context
        df: pandas dataframe containing product subcategory data

    Returns:
        GX Validation Result object containing result metadata
    """

    data_source = context.data_sources.get(DATA_SOURCE_NAME)

    data_asset = data_source.add_dataframe_asset(name="product subcategories")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "product subcategory batch definition"
    )

    expectation_suite = context.suites.add(
        gx.ExpectationSuite(name="product subcategory expectations")
    )

    expectations = [
        gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=["product_subcategory_id", "name"]
        )
    ]

    for expectation in expectations:
        expectation_suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="product subcategory validation definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="product subcategory checkpoint",
            validation_definitions=[validation_definition],
            result_format={
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["product_subcategory_id"],
            },
        )
    )

    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

    return _extract_validation_result_from_checkpoint_result(checkpoint_result)


def validate_product_data(
    df_products: pd.DataFrame,
    df_product_categories: pd.DataFrame,
    df_product_subcategories: pd.DataFrame,
) -> Tuple[GxValidationResult, GxValidationResult, GxValidationResult]:
    """Run GX data validation on sample product data for Cookbook 2 and DAG, and return Validation Results.

    Args:
        df_products: pandas dataframe containing product data
        df_product_categories: pandas dataframe containing product category data
        df_product_subcategories: pandas dataframe containing product subcategory data

    Returns:
        Tuple of Validation Results for:
            * product validation
            * product category validation
            * product subcategory
    """

    # Get GX context.
    context = gx.get_context(mode="ephemeral")

    # Create the Data Source.
    data_source = context.data_sources.add_pandas("pandas")

    # Validate product, product category, and product subcategory data, return results.
    return (
        _validate_products(context, df_products),
        _validate_product_categories(context, df_product_categories),
        _validate_product_subcategories(context, df_product_subcategories),
    )


def separate_valid_and_invalid_product_rows(
    df_products: pd.DataFrame, validation_result: GxValidationResult
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separate valid and invalid product rows based on validation results.

    Args:
        df_products: pandas dataframe containing product data
        validation_result: GX Validation Result object (requires COMPLETE results format)

    Returns:
        Tuple of pandas dataframes:
            Valid product data rows
            Invalid (failed validation) product data rows
    """

    # Identify failing Expectations.
    failing_expectations = []

    for result in validation_result["results"]:
        if result["success"] is False:
            failing_expectations.append(result)

    # Identify invalid rows based on Expectation unexpected_index_list.
    invalid_row_product_ids = []

    for expectation in failing_expectations:
        invalid_row_product_ids.extend(
            [x["product_id"] for x in expectation["result"]["unexpected_index_list"]]
        )

    # Deduplicate invalid row product ids.
    invalid_row_product_ids = list(set(invalid_row_product_ids))

    # Separate invalid rows.
    df_products_invalid = df_products[
        df_products["product_id"].isin(invalid_row_product_ids)
    ].reset_index(drop=True)

    # Separate valid rows.
    df_products_valid = df_products.drop(
        df_products[df_products["product_id"].isin(invalid_row_product_ids)].index
    ).reset_index(drop=True)

    return df_products_valid, df_products_invalid


def write_invalid_rows_to_file(filepath: pathlib.Path, df: pd.DataFrame):
    """Write invalid rows to an error file.

    Args:
        filepath: full filepath to write file to
        df: pandas dataframe containing invalid rows
    """
    df.to_csv(filepath, index=False)
    log.warning(f"{df.shape[0]} invalid rows written to error file.")
