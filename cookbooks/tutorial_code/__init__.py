"""Code to support GX in the data pipeline tutorials."""

import logging
import warnings

import tutorial_code.airflow as airflow
import tutorial_code.cloud as cloud
import tutorial_code.cookbook1 as cookbook1
import tutorial_code.cookbook2 as cookbook2
import tutorial_code.cookbook3 as cookbook3
import tutorial_code.db as db

# Filter Deprecation/FutureWarnings, some older libraries are intentionally pinned for Airflow and Altair compatibility.
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Set explicit logging levels, importing the airflow module code imported by in the
# notebooks causes a change in logging levels for GX.
logging.basicConfig(level=logging.WARNING)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
