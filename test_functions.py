import pandas as pd
from process_raw_data import clean_columns_names, clean_rows, clean_title, get_db_connection


unclean_df = pd.read_csv("data/RAW_DATA_1.csv").head(1)
author_data = get_db_connection()
author_data = author_data.rename(columns={"id": "author_id"})
unclean_df = unclean_df.merge(author_data, on="author_id", how="left")
# Get uncleaned version

clean_df = pd.read_csv("data/PROCESSED_DATA.csv").head(1)
# Cleaned version


def test_clean_title():
    """Test cleaning of title"""
    title = "The Fault in Our Stars (Hardcover)"
    title = clean_title(title)
    assert title == "The Fault in Our Stars "


def test_clean_title_multiple_brackets():
    """Test multiple brackets"""
    title = "The Fault in Our Stars (Hardcover) (Hardcover) (Hardcover)"
    title = clean_title(title)
    assert title == "The Fault in Our Stars "


def test_clean_title_empty_brackets():
    """Test empty brackets"""
    title = "The Fault in Our Stars ()"
    title = clean_title(title)
    assert title == "The Fault in Our Stars "


def test_clean_title_square_brackets():
    """Test square brackets inside brackets"""
    title = "The Fault in Our Stars [(Hardcover)]"
    title = clean_title(title)
    assert title == "The Fault in Our Stars "


def test_clean_column_names():
    """Test if unnecesary columns have been reduced"""
    cleaned = clean_columns_names(unclean_df.copy())
    assert clean_df.shape == cleaned.shape


def test_clean_rows():
    """Test cleaning of rows, compare to cleaned version after cleaning"""
    default_cleaned = clean_df.copy()  # Already cleaned version

    to_clean = clean_columns_names(unclean_df.copy())
    cleaned_rows = clean_rows(to_clean)

    assert default_cleaned["title"].equals(cleaned_rows["title"])
    assert default_cleaned["author_name"].equals(cleaned_rows["author_name"])
    assert default_cleaned["ratings"].equals(cleaned_rows["ratings"])
    assert default_cleaned["rating"].equals(cleaned_rows["rating"])
    assert default_cleaned["year"].equals(cleaned_rows["year"])
