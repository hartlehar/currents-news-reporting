import os
import re
import sqlite3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path


def get_latest_csv(folder="/opt/airflow/data"):
    """Find latest CSV file."""
    csv_files = list(Path(folder).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {folder}")
    return str(max(csv_files, key=lambda f: f.stat().st_mtime))


def get_pg_conn():
    """Create PostgreSQL connection."""
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow")
        )
    except psycopg2.Error as e:
        raise Exception(f"❌ Connection failed: {str(e)}")


def create_tables(cursor):
    """
    Creates database tables if they don't exist.
    
    Tables:
    - NewsArticles: Main article data (id, title, description, etc.)
    - NewsCategory: Category relationships (many-to-many)
    - NewsSource: Source relationships (many-to-many)
    """

    try:
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
        print("✅ NewsArticles table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NewsCategory (
                id SERIAL PRIMARY KEY,
                news_id TEXT REFERENCES NewsArticles(id),
                category TEXT
            );
        """)
        print("✅ NewsCategory table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NewsSource (
                id SERIAL PRIMARY KEY,
                news_id TEXT REFERENCES NewsArticles(id),
                source TEXT
            );
        """)
        print("✅ NewsSource table created/verified")
        
    except psycopg2.Error as e:
        raise Exception(f"❌ Failed to create tables: {str(e)}")


# ---------------------------------------------------------
# Insert Articles (Upsert)
# ---------------------------------------------------------
def load_main_table(cursor, df):
    """
    Loads or updates article data in NewsArticles table.
    Uses UPSERT logic: insert if new, update if exists.
    
    Args:
        cursor: Database cursor
        df: DataFrame with article data
    """
    
    try:
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
        print(f"✅ Loaded {len(rows)} articles into NewsArticles")
        
    except Exception as e:
        raise Exception(f"❌ Failed to load articles: {str(e)}")


# ---------------------------------------------------------
# Load categories (many-to-many)
# ---------------------------------------------------------
def load_category_table(cursor, df_full):
    """
    Loads category data into NewsCategory table.
    Handles parsing and normalizing category strings.
    
    Args:
        cursor: Database cursor
        df_full: Full DataFrame with category data
    """
    
    try:
        df = df_full.copy()

        # Parse category strings and split into individual categories
        df["category"] = (
            df["category"]
            .fillna("")
            .replace(r"[\[\]']", "", regex=True)  # Remove brackets and quotes
            .str.split(r"\s*,\s*")  # Split by comma with optional whitespace
        )

        # Expand each category into separate row
        df = df.explode("category")
        df = df[df["category"].notna() & (df["category"].str.strip() != "")]

        rows = df[["id", "category"]].values.tolist()

        if not rows:
            print("ℹ️  No categories to load")
            return

        sql = """
            INSERT INTO NewsCategory (news_id, category)
            VALUES %s;
        """

        execute_values(cursor, sql, rows)
        print(f"✅ Loaded {len(rows)} category entries into NewsCategory")
        
    except Exception as e:
        raise Exception(f"❌ Failed to load categories: {str(e)}")


# ---------------------------------------------------------
# Extract domain helper
# ---------------------------------------------------------
def extract_domain(url):
    """Extract domain from URL."""
    if not url or pd.isna(url):
        return ""
    try:
        match = re.search(r"https?://([^/]+)", str(url))
        if match:
            domain = match.group(1).lower()
            domain = re.sub(r"^www\.", "", domain)
            return domain
        return ""
    except:
        return ""


# ---------------------------------------------------------
# Load source table
# ---------------------------------------------------------
def load_source_table(cursor, df_full):
    """
    Loads source data into NewsSource table.
    Extracts domain names from URLs.
    
    Args:
        cursor: Database cursor
        df_full: Full DataFrame with URL data
    """
    
    try:
        df = df_full.copy()
        df["source"] = df["url"].apply(lambda u: extract_domain(u))

        # Filter out empty sources
        df = df[df["source"] != ""]

        rows = df[["id", "source"]].values.tolist()

        if not rows:
            print("ℹ️  No sources to load")
            return

        sql = """
            INSERT INTO NewsSource (news_id, source)
            VALUES %s;
        """

        execute_values(cursor, sql, rows)
        print(f"✅ Loaded {len(rows)} source entries into NewsSource")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def run_load_to_postgres():
    """CSV → PostgreSQL."""
    print("\n" + "="*50)
    print("🚀 PostgreSQL ETL Starting")
    print("="*50 + "\n")
    
    conn = None
    cursor = None
    
    try:
        # Load CSV
        latest_csv = get_latest_csv("/opt/airflow/data")
        df = pd.read_csv(latest_csv)
        print(f"📋 CSV loaded: {len(df)} rows")
        
        # Process dates
        df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date
        
        # Connect and load
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        print("\n🛠️  Creating tables...\n")
        create_tables(cursor)
        
        print("\n📥 Loading data...\n")
        load_main_table(cursor, df)
        load_category_table(cursor, df)
        load_source_table(cursor, df)
        
        conn.commit()
        
        print("\n✅ PostgreSQL ETL Complete\n")
        
    except Exception as e:
        print(f"\n❌ ETL Failed: {str(e)}\n")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    run_load_to_postgres()