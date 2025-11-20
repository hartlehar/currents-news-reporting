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
def get_latest_csv(folder="/opt/airflow/data"):
    """
    Finds the latest CSV file in the specified folder.
    
    Args:
        folder: Path to search for CSV files (default: /data)
        
    Returns:
        str: Path to the latest CSV file
        
    Raises:
        FileNotFoundError: If no CSV files are found
    """
    folder_path = Path(folder)
    
    if not folder_path.exists():
        raise FileNotFoundError(f"Data directory does not exist: {folder}")
    
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {folder}")

    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    print(f"📄 Found {len(csv_files)} CSV file(s)")
    print(f"📍 Using: {latest_file}")
    
    return str(latest_file)


# ---------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------
def get_pg_conn():
    """
    Creates and returns a PostgreSQL connection.
    
    Uses environment variables for connection parameters:
    - POSTGRES_HOST (default: postgres)
    - POSTGRES_PORT (default: 5432)
    - POSTGRES_DB (default: airflow)
    - POSTGRES_USER (default: airflow)
    - POSTGRES_PASSWORD (default: airflow)
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow")
        )
        print("✅ Connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        raise Exception(f"❌ Failed to connect to PostgreSQL: {str(e)}")


# ---------------------------------------------------------
# Create tables in PostgreSQL
# ---------------------------------------------------------
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
def run_load_to_postgres(csv_path = None):
    """
    Main ETL process:
    1. Find latest CSV file
    2. Read CSV data
    3. Create tables if needed
    4. Load data into PostgreSQL
    5. Commit changes
    
    Raises:
        Exception: If any step fails
    """

    print("\n" + "="*60)
    print("🚀 Starting PostgreSQL Load Process")
    print("="*60)
    
    try:
        # Step 1: Find latest CSV
        print("\n📍 Step 1: Locating CSV file...")
        if csv_path is None:
            latest_csv = get_latest_csv("/opt/airflow/data")
        else:
            latest_csv = csv_path
        
        # latest_csv = get_latest_csv("/opt/airflow/data")
        
        # Step 2: Read CSV
        print("\n📖 Step 2: Reading CSV file...")
        df = pd.read_csv(latest_csv)
        df_full = pd.read_csv(latest_csv)
        print(f"✅ Loaded {len(df)} rows from CSV")
        
        # Step 3: Data processing
        print("\n🔧 Step 3: Processing data...")
        df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date
        print("✅ Date columns processed")

        # Step 4: Connect to database
        print("\n🗄️  Step 4: Connecting to PostgreSQL...")
        conn = get_pg_conn()
        cursor = conn.cursor()

        # Step 5: Create tables
        print("\n📋 Step 5: Creating tables...")
        create_tables(cursor)

        # Step 6: Load data
        print("\n📥 Step 6: Loading data...")
        load_main_table(cursor, df)
        load_category_table(cursor, df_full)
        load_source_table(cursor, df_full)

        # Step 7: Commit and close
        print("\n💾 Step 7: Committing changes...")
        conn.commit()
        print("✅ Changes committed")
        
        cursor.close()
        conn.close()

        print("\n" + "="*60)
        print("✅ PostgreSQL load complete!")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ Error during PostgreSQL load: {str(e)}")
        raise


if __name__ == "__main__":
    run_load_to_postgres()
