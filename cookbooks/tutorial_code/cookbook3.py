"""Helper functions for Cookbook 3."""

import altair as alt
import pandas as pd
import tutorial_code as tutorial

CUSTOMER_PROFILE_TABLE_NAME = "customer_profile"
BINS = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
CHART_WIDTH = 600
CHART_HEIGHT = 300


def _load_customer_profile_data() -> pd.DataFrame:
    """Return sample customer profile data as pandas dataframe."""

    return pd.read_sql_query(
        "select * from customer_profile",
        con=tutorial.db.get_cloud_postgres_engine(),
    )


def _format_chart(chart: alt.Chart, chart_title: str) -> alt.Chart:
    """Standardize chart formatting."""

    return (
        chart.properties(title=chart_title, height=300, width=600)
        .configure_title(fontSize=18, anchor="start", color="gray")
        .configure_axis(grid=False)
        .configure_view(strokeWidth=0)
    )


def visualize_customer_age_distribution() -> alt.Chart:
    """Return histogram visualization of customer age data."""

    # Fetch data.
    df = tutorial.cookbook3._load_customer_profile_data()

    # Prep data for display.
    df_hist = pd.cut(df["age"], bins=BINS).value_counts().reset_index()
    df_hist = df_hist.rename(columns={"age": "binterval"})
    df_hist["bin_lower"] = df_hist["binterval"].apply(lambda x: x.left)
    df_hist["bin_upper"] = df_hist["binterval"].apply(lambda x: x.right)
    df_hist["bin_mid"] = df_hist.apply(
        lambda row: (row["bin_lower"] + row["bin_upper"]) / 2, axis=1
    )
    df_hist["binterval_str"] = df_hist.apply(
        lambda row: f"{row['bin_lower']}-{row['bin_upper']} years", axis=1
    )
    df_hist = (
        df_hist[["binterval_str", "bin_lower", "bin_mid", "bin_upper", "count"]]
        .sort_values("bin_lower")
        .reset_index(drop=True)
    )

    # Assemble chart.
    chart = (
        alt.Chart(df_hist)
        .mark_bar()
        .encode(
            alt.X("bin_mid", bin=True, axis=alt.Axis(title="Customer age")),
            alt.Y("count", axis=alt.Axis(title="Number of customers")),
            tooltip=[
                alt.Tooltip("binterval_str", title="Age"),
                alt.Tooltip("count", title="Customer count", format=","),
            ],
        )
    )

    return _format_chart(chart, chart_title="Customer age distribution")


def visualize_customer_income_distribution() -> alt.Chart:
    """Return histogram visualization of customer age data."""

    # Fetch data.
    df = tutorial.cookbook3._load_customer_profile_data()

    # Prep data for display.
    def convert_bin_to_tooltip(bin_lower: int, bin_upper: int) -> str:
        if bin_lower == 0:
            return "Less than $10k"
        elif bin_upper == 100_000:
            return f"$90k+"
        else:
            return f"${int(bin_lower/1_000)}k-{int(bin_upper/1_000)}k"

    df_hist = (
        pd.cut(df["annual_income_usd"], bins=[x * 1_000 for x in BINS])
        .value_counts()
        .reset_index()
    )
    df_hist = df_hist.rename(columns={"annual_income_usd": "binterval"})
    df_hist["bin_lower"] = df_hist["binterval"].apply(lambda x: x.left)
    df_hist["bin_upper"] = df_hist["binterval"].apply(lambda x: x.right)
    df_hist["bin_mid"] = df_hist.apply(
        lambda row: (row["bin_lower"] + row["bin_upper"]) / 2, axis=1
    )
    df_hist["binterval_str"] = df_hist.apply(
        lambda row: convert_bin_to_tooltip(row["bin_lower"], row["bin_upper"]), axis=1
    )
    df_hist = (
        df_hist[["binterval_str", "bin_lower", "bin_mid", "bin_upper", "count"]]
        .sort_values("bin_lower")
        .reset_index(drop=True)
    )

    # Assemble chart.
    chart = (
        alt.Chart(df_hist)
        .mark_bar()
        .encode(
            alt.X(
                "bin_mid",
                bin=True,
                axis=alt.Axis(
                    title="Customer annual income (USD)",
                    labelExpr="'$' + format(datum.value, ',')",
                    labelAngle=-25,
                ),
            ),
            alt.Y("count", axis=alt.Axis(title="Number of customers")),
            tooltip=[
                alt.Tooltip("binterval_str", title="Annual income"),
                alt.Tooltip("count", title="Customer count", format=","),
            ],
        )
    )

    return _format_chart(chart, chart_title="Customer annual income distribution (USD)")
