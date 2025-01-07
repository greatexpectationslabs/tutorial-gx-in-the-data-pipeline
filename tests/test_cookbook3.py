import altair as alt
import tutorial_code as tutorial


def test_visualize_customer_age_distribution():
    """Test that an altair chart is returned for distribution visualizations."""
    chart = tutorial.cookbook3.visualize_customer_age_distribution()
    assert isinstance(chart, alt.Chart)


def test_visualize_customer_income_distribution():
    """Test that an altair chart is returned for distribution visualizations."""
    chart = tutorial.cookbook3.visualize_customer_income_distribution()
    assert isinstance(chart, alt.Chart)
