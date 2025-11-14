import os
import re
import sqlite3
import pandas as pd
from pathlib import Path


# ------------------------------------------------------------------------------
#  Helper: Find latest CSV file automatically
# ------------------------------------------------------------------------------

def get_latest_csv(folder="data"):
    """
    Automatically find the newest CSV file in the specified folder.
    Returns:
        str: Path to the latest CSV file.
    """
    folder_path = Path(folder)
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in folder: {folder}")

    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    return str(latest_file)


# ------------------------------------------------------------------------------
#  Load Articles Table
# ------------------------------------------------------------------------------

def load_main_table(cursor, df):
    """
    Create and populate the NewsArticles table.
    """

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticles (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            author TEXT,
            image TEXT,
            language TEXT,
            category TEXT,
            published DATE
        )
    """)

    # Insert data
    df.to_sql("NewsArticles", cursor.connection, if_exists="replace", index=False)


# ------------------------------------------------------------------------------
#  Load Category Table (One-to-Many)
# ------------------------------------------------------------------------------

def load_category_table(cursor, df_full):
    """
    Create and populate the NewsCategory table.
    Each news can have multiple categories.
    """

    df = df_full.copy()

    # Clean categories
    df["category"] = (
        df["category"]
        .fillna("")
        .replace(r"[\[\]']", "", regex=True)
        .str.split(r"\s*,\s*")
    )

    df = df.explode("category", ignore_index=True)

    df = df.rename(columns={"id": "news_id"})
    df = df[["news_id", "category"]]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsCategory (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id TEXT NOT NULL,
            category TEXT,
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id)
        )
    """)

    # Temp table
    df.to_sql("TempCategory", cursor.connection, if_exists="replace", index=False)

    cursor.execute("""
        INSERT INTO NewsCategory (news_id, category)
        SELECT news_id, category
        FROM TempCategory
        WHERE category IS NOT NULL AND TRIM(category) != '';
    """)

    cursor.execute("DROP TABLE IF EXISTS TempCategory;")


# ------------------------------------------------------------------------------
#  Load Source Table (Extract Domain)
# ------------------------------------------------------------------------------

def load_source_table(cursor, df_full):
    """
    Create and populate a table mapping news_id to its source domain.
    """

    df = df_full.copy()
    df_source = df[["id", "url"]].rename(columns={"id": "news_id"})

    # Extract domain
    df_source["source"] = (
        df_source["url"]
        .fillna("")
        .apply(lambda x: re.search(r"https?://([^/]+)/", x).group(1)
               if re.search(r"https?://([^/]+)/", x) else "")
    )

    # Clean domain
    df_source["source"] = (
        df_source["source"]
        .str.replace(r"^www\.", "", regex=True)
        .str.replace(r"\.(com|co|org).*", "", regex=True)
    )

    df_source = df_source[["news_id", "source"]]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsSource (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id TEXT NOT NULL,
            source TEXT NOT NULL,
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id)
        )
    """)

    df_source.to_sql("TempSource", cursor.connection, if_exists="replace", index=False)

    cursor.execute("""
        INSERT INTO NewsSource (news_id, source)
        SELECT news_id, source
        FROM TempSource
        WHERE source IS NOT NULL AND TRIM(source) != '';
    """)

    cursor.execute("DROP TABLE IF EXISTS TempSource;")


# ------------------------------------------------------------------------------
#  Main Process
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    print("🔍 Looking for latest CSV in /data/...")

    latest_csv = get_latest_csv("data")
    print(f"📄 Latest CSV detected: {latest_csv}")

    # Basic cleaned dataframe
    df = pd.read_csv(latest_csv)
    df = df.drop(columns=["category", "url"], errors="ignore")
    df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date

    # Full version for category & url processing
    df_full = pd.read_csv(latest_csv)

    # Connect to SQLite database
    conn = sqlite3.connect("data/news.db")
    cursor = conn.cursor()

    print("🛠️ Creating database tables...")

    load_main_table(cursor, df)
    load_category_table(cursor, df_full)
    load_source_table(cursor, df_full)

    conn.commit()
    conn.close()

    print("✅ Database successfully built from latest CSV!")
