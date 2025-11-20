import os
import re
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
            CREATE TABLE IF NOT EXISTS NewsSource (
                id SERIAL PRIMARY KEY,
                source TEXT UNIQUE
            );
        """)
        print("✅ NewsSource table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NewsArticles (
                id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                author TEXT,
                url TEXT,
                image TEXT,
                language TEXT,
                published DATE,
                source_id INTEGER REFERENCES NewsSource(id)
            );
        """)
        print("✅ NewsArticles table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NewsCategory (
                id SERIAL PRIMARY KEY,
                category TEXT UNIQUE
            );
        """)
        print("✅ NewsCategory table created/verified")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NewsArticleCategory (
                news_id TEXT REFERENCES NewsArticles(id),
                category_id INTEGER REFERENCES NewsCategory(id),
                UNIQUE(news_id, category_id)
            );
        """)
        print("✅ NewsArticleCategory table created/verified")
        
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
            "id", "title", "description", "url", "author",
            "image", "language", "published"
        ]].values.tolist()

        sql = """
            INSERT INTO NewsArticles
            (id, title, description, url, author, image, language, published)
            VALUES %s
            ON CONFLICT (id) 
            DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                url = EXCLUDED.url,
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
        df["category"] = df["category"].str.strip()

        unique_categories = df["category"].drop_duplicates().tolist()
        if unique_categories:
            category_rows = [(c,) for c in unique_categories]
            sql_insert_categories = """
                INSERT INTO NewsCategory (category)
                VALUES %s
                ON CONFLICT (category) DO NOTHING;
            """
            execute_values(cursor, sql_insert_categories, category_rows)
            print(f"✅ Loaded {len(category_rows)} unique categories into NewsCategory")
        else:
            print("ℹ️  No categories to load")
            return

        # --- Insert into NewsArticleCategory join table ---
        for _, row in df.iterrows():
            article_id = row["id"]
            category_name = row["category"]

            # Get category_id
            cursor.execute(
                "SELECT id FROM NewsCategory WHERE category = %s;",
                (category_name,)
            )
            result = cursor.fetchone()
            if not result:
                continue
            category_id = result[0]

            # Insert article-category mapping
            cursor.execute(
                """
                INSERT INTO NewsArticleCategory (news_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT (news_id, category_id) DO NOTHING;
                """,
                (article_id, category_id)
            )

        print("✅ Updated NewsArticleCategory join table with article-category mappings")
        
    except Exception as e:
        raise Exception(f"❌ Failed to load categories: {str(e)}")


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

        rows = df[["source"]].values.tolist()

        if not rows:
            print("ℹ️  No sources to load")
            return

        sql = """
            INSERT INTO NewsSource (source)
            VALUES %s
            ON CONFLICT (source) DO NOTHING;
        """

        execute_values(cursor, sql, rows)
        print(f"✅ Loaded {len(rows)} source entries into NewsSource")

        for idx, row in df.iterrows():
            article_id = row["id"]
            source = row["source"]

            # Get source_id
            cursor.execute(
                "SELECT id FROM NewsSource WHERE source = %s;",
                (source,)
            )
            result = cursor.fetchone()

            if not result:
                continue  # should not happen, but safe

            source_id = result[0]

            # Update the article
            cursor.execute(
                """
                UPDATE NewsArticles
                SET source_id = %s
                WHERE id = %s;
                """,
                (source_id, article_id)
            )

        print("✅ Updated NewsArticles with source_id values")
        
    except Exception as e:
        raise Exception(f"❌ Failed to load sources: {str(e)}")


def extract_domain(url):
    """
    Extracts the domain name from a URL.
    
    Examples:
        'https://www.bbc.com/news' → 'bbc'
        'https://techcrunch.com/article' → 'techcrunch'
    
    Args:
        url: Full URL string
        
    Returns:
        str: Extracted domain name
    """
    if not isinstance(url, str):
        return ""
    
    # Extract domain from URL
    match = re.search(r"https?://([^/]+)/?", url)
    if not match:
        return ""
    
    domain = match.group(1).lower()

    # Remove www. prefix
    domain = domain.replace("www.", "")
    
    # Remove TLD extensions (.com, .co, .org, etc.)
    domain = re.sub(r"\.(com|co|org|net|info|io|uk|ca|au|de|fr|it|es|nl|be|ch|se|no|dk|fi|pl|ru|br|jp|cn|in|kr).*", "", domain)

    return domain


# ---------------------------------------------------------
# Main ETL Process
# ---------------------------------------------------------
def run_load_to_postgres():
    """CSV → PostgreSQL."""
    print("\n" + "="*50)
    print("🚀 PostgreSQL ETL Starting")
    print("="*50)
    
    try:
        # Load CSV
        latest_csv = get_latest_csv("/opt/airflow/data")
        df = pd.read_csv(latest_csv)
        print(f"📄 CSV loaded: {len(df)} rows")
        
        # Process dates
        df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date
        
        # Connect and load
        conn = get_pg_conn()
        cursor = conn.cursor()
        
        create_tables(cursor)
        load_main_table(cursor, df)
        load_category_table(cursor, df)
        load_source_table(cursor, df)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL ETL Complete\n")
        
    except Exception as e:
        print(f"❌ ETL Failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_load_to_postgres()