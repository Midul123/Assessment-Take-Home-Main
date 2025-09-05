"""A script to process book data."""
import argparse
import sqlite3
import pandas as pd


def get_db_connection():
    """Connect to author table and retrieve data"""
    con = sqlite3.connect("data/authors.db")

    result = pd.read_sql("SELECT * FROM author", con)
    con.close()
    return result


def parse_args():
    """Take cli arguments"""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--file',
        type=str,
        default="RAW_DATA_1.csv",
        help="specify csv file"
    )

    return parser.parse_args()


def load_csv_to_df(file_path: str):
    """Load file content into a dataframe"""

    return pd.read_csv(f"data/{file_path}")


def clean_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    """Take unclean dataframe, and return a clean version"""

    df = df[['book_title',
            'name', 'Year released', 'Rating', 'ratings']]
    df = df.rename(columns={
        "book_title": "title",
        "name": "author_name",
        "Year released": "year",
        "Rating": "rating",

    })
    return df


def clean_title(title: str) -> str:
    """Clean title of any info in parethesis"""
    title = title.split("(")[0]
    title = title.split("[")[0]
    return title


def clean_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Take unclean dataframe, and return a clean version"""

    # Clean titles
    df["title"] = df["title"].apply(clean_title)

    # replace ratings (, with .)
    df["rating"] = df["rating"].str.replace(",", ".")

    # convert rating and ratings table to integer
    df["rating"] = df["rating"] = df["rating"].astype(float)

    # Remove backticks from ratings
    df["ratings"] = df["ratings"].str.replace("`", "")
    # Convert to int
    df["ratings"] = df["ratings"] = df["ratings"].astype(int)

    return df


if __name__ == "__main__":
    args = parse_args()
    file_name = args.file
    if args:
        df = (load_csv_to_df(file_name))

    author_data = get_db_connection()
    author_data = author_data.rename(columns={"id": "author_id"})
    # Combine dataframes on matching id
    df = df.merge(author_data, on="author_id", how="left")

    # Clean column names
    df = clean_columns_names(df)

    # Drop missing rows
    df = df.dropna()

    # Clean rest of rows
    df = clean_rows(df)

    # Output as csv file
    df.to_csv("data/PROCESSED_DATA.csv", index=False)
