"""A script to analyse book data."""
import pandas as pd
import altair as alt


def extract_data() -> pd.DataFrame:
    """Extract processed data and load to dataframe"""

    return pd.read_csv("data/PROCESSED_DATA.csv")


def add_decade_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a decade column to the dataframe"""
    df["decade"] = (df["year"]//10) * 10
    return df


def create_pie_chart(df: pd.DataFrame):
    """Create and save pie chart"""
    chart = alt.Chart(df).mark_arc().encode(
        theta=alt.Theta("count()", title="Total Books"),
        color=alt.Color("decade:N"),
        tooltip=["decade", "count()"]
    )

    chart.save("decade_releases.png")


def create_bar_chart(df: pd.DataFrame):
    """Create Bar Chart"""
    group_ratings = (
        df.groupby("author_name", as_index=False)["ratings"].sum()
        .sort_values("ratings", ascending=False)
        .head(10)
    )

    chart = alt.Chart(group_ratings).mark_bar().encode(
        x=alt.X("ratings:Q"),
        y=alt.Y("author_name:N").sort('-x'),
        tooltip=["author_name", "ratings"]
    )

    chart.save("top_authors.png")


if __name__ == "__main__":
    dataframe = extract_data()
    dataframe = add_decade_column(dataframe)

    # Create and save pie charts
    create_pie_chart(dataframe)
    create_bar_chart(dataframe)
