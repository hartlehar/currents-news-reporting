import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------
# Helper: find the latest CSV file
# ---------------------------------------------------------
def get_latest_csv(folder="data"):
    folder_path = Path(folder)
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {folder}")

    return str(max(csv_files, key=lambda f: f.stat().st_mtime))


# ---------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------
def get_pg_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )
    return conn


# ---------------------------------------------------------
# Create tables in PostgreSQL
# ---------------------------------------------------------
def create_tables(cursor):

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticles (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            author TEXT,
            image TEXT,
            language TEXT,
            published DATE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsCategory (
            id SERIAL PRIMARY KEY,
            news_id TEXT REFERENCES NewsArticles(id),
            category TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsSource (
            id SERIAL PRIMARY KEY,
            news_id TEXT REFERENCES NewsArticles(id),
            source TEXT
        );
    """)


# ---------------------------------------------------------
# Insert Articles (Upsert)
# ---------------------------------------------------------
def load_main_table(cursor, df):

    rows = df[[
        "id", "title", "description", "author",
        "image", "language", "published"
    ]].values.tolist()

    sql = """
        INSERT INTO NewsArticles
        (id, title, description, author, image, language, published)
        VALUES %s
        ON CONFLICT (id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            author = EXCLUDED.author,
            image = EXCLUDED.image,
            language = EXCLUDED.language,
            published = EXCLUDED.published;
    """

    execute_values(cursor, sql, rows)


# ---------------------------------------------------------
# Load categories (many-to-many)
# ---------------------------------------------------------
def load_category_table(cursor, df_full):

    df = df_full.copy()

    df["category"] = (
        df["category"]
        .fillna("")
        .replace(r"[\[\]']", "", regex=True)
        .str.split(r"\s*,\s*")
    )

    df = df.explode("category")
    df = df[df["category"].notna() & (df["category"].str.strip() != "")]

    rows = df[["id", "category"]].values.tolist()

    sql = """
        INSERT INTO NewsCategory (news_id, category)
        VALUES %s;
    """

    execute_values(cursor, sql, rows)


# ---------------------------------------------------------
# Load source table
# ---------------------------------------------------------
def load_source_table(cursor, df_full):

    df = df_full.copy()
    df["source"] = df["url"].apply(lambda u: extract_domain(u))

    rows = df[["id", "source"]].values.tolist()

    sql = """
        INSERT INTO NewsSource (news_id, source)
        VALUES %s;
    """

    execute_values(cursor, sql, rows)


def extract_domain(url):
    if not isinstance(url, str):
        return ""
    match = re.search(r"https?://([^/]+)/?", url)
    if not match:
        return ""
    domain = match.group(1).lower()

    domain = domain.replace("www.", "")
    domain = re.sub(r"\.(com|co|org|net|info|io).*", "", domain)

    return domain


# ---------------------------------------------------------
# Main ETL Process
# ---------------------------------------------------------
def run_load_to_postgres():

    print("🔍 Looking for latest CSV...")
    latest_csv = get_latest_csv("data")
    print(f"📄 Latest CSV found: {latest_csv}")

    df = pd.read_csv(latest_csv)
    df_full = pd.read_csv(latest_csv)

    df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date

    conn = get_pg_conn()
    cursor = conn.cursor()

    print("🛠️ Creating tables if not exists...")
    create_tables(cursor)

    print("🚀 Loading main article table...")
    load_main_table(cursor, df)

    print("📂 Loading categories...")
    load_category_table(cursor, df_full)

    print("🌐 Loading sources...")
    load_source_table(cursor, df_full)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ PostgreSQL load complete!")


if __name__ == "__main__":
    run_load_to_postgres()