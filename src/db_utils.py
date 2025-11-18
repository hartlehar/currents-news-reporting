import re
import sqlite3
import pandas as pd
from pathlib import Path
import ast


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
    Create NewsArticleCategory mapping table for many-to-many relationship.
    """

    df = df_full.copy()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsCategory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT
        )
    """)

    df["category"] = df["category"].apply(ast.literal_eval)
    unique_cats = set().union(*df["category"])

    for cat in unique_cats:
        cursor.execute("INSERT INTO NewsCategory (category) VALUES (?);", (cat.strip(),))


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticleCategory (
            news_id TEXT,
            category_id INTEGER,
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id),
            FOREIGN KEY (category_id) REFERENCES NewsCategory(id)
        )
    """)

    for _, row in df.iterrows():
        news_id = row["id"]
        categories = row["category"]

        for cat in categories:
            cursor.execute("SELECT id FROM NewsCategory WHERE category = ?;", (cat.strip(),))
            cat_id = cursor.fetchone()
            if cat_id:
                cursor.execute("""
                    INSERT INTO NewsArticleCategory (news_id, category_id)
                    VALUES (?, ?);
                """, (news_id, cat_id[0]))
    
    conn.commit()

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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL
        )
    """)

    for source in df_source["source"].unique():
        cursor.execute("INSERT INTO NewsSource (source) VALUES (?);", (source,))

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticleSource (
            news_id TEXT,
            source_id INTEGER,
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id),
            FOREIGN KEY (source_id) REFERENCES NewsSource(source_id)
        )
    """)

    for _, row in df_source.iterrows():
        news_id = row["news_id"]
        source = row["source"]

        cursor.execute("SELECT id FROM NewsSource WHERE source = ?;", (source,))
        source_id = cursor.fetchone()
        if source_id:
            cursor.execute("""
                INSERT INTO NewsArticleSource (news_id, source_id)
                VALUES (?, ?);
            """, (news_id, source_id[0]))


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