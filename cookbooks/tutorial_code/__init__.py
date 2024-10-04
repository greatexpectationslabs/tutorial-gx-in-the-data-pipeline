"""Code to support GX in the data pipeline tutorials."""

import warnings

import tutorial_code.airflow as airflow
import tutorial_code.cookbook1 as cookbook1
import tutorial_code.db as db

# Filter DeprecationWarnings, some older libraries are intentionally pinned for Airflow compatibility.
warnings.filterwarnings("ignore", category=DeprecationWarning)
