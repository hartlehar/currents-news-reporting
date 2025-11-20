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
            source_id INTEGER,
            url TEXT,
            image TEXT,
            language TEXT,
            published DATE,
            FOREIGN KEY (source_id) REFERENCES NewsSource(id)
        )
    """)

    # Insert data - use INSERT OR IGNORE to avoid duplicates
    articles_list = []
    for _, row in df.iterrows():
        articles_list.append((
            row.get("id"),
            row.get("title", ""),
            row.get("description", ""),
            row.get("author", ""),
            None,  # source_id will be updated later
            row.get("url", ""),
            row.get("image", ""),
            row.get("language", ""),
            row.get("published", None)
        ))
    
    cursor.executemany(
        """INSERT OR IGNORE INTO NewsArticles 
           (id, title, description, author, source_id, url, image, language, published)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        articles_list
    )


# ------------------------------------------------------------------------------
#  Load Category Table (One-to-Many)
# ------------------------------------------------------------------------------

def load_category_table(cursor, df_full):
    """
    Create and populate the NewsCategory table.
    Each news can have multiple categories.
    """

    df = df_full.copy()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsCategory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT UNIQUE
        )
    """)

    # Parse categories safely
    unique_cats = set()
    for _, row in df.iterrows():
        if pd.notna(row.get("category")):
            try:
                cats = ast.literal_eval(str(row["category"]))
                if isinstance(cats, list):
                    unique_cats.update([str(c).strip() for c in cats if c])
            except (ValueError, SyntaxError):
                # If parsing fails, treat as string
                cat_str = str(row["category"]).strip()
                if cat_str:
                    unique_cats.add(cat_str)

    # Insert categories with IGNORE to avoid duplicates
    for cat in unique_cats:
        cursor.execute("INSERT OR IGNORE INTO NewsCategory (category) VALUES (?);", (cat,))

    # Create association table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticleCategory (
            news_id TEXT,
            category_id INTEGER,
            UNIQUE(news_id, category_id),
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id),
            FOREIGN KEY (category_id) REFERENCES NewsCategory(id)
        )
    """)

    # Insert associations
    for _, row in df.iterrows():
        news_id = row.get("id")
        if pd.notna(row.get("category")):
            try:
                cats = ast.literal_eval(str(row["category"]))
                if isinstance(cats, list):
                    categories = cats
                else:
                    categories = [cats]
            except (ValueError, SyntaxError):
                categories = [str(row["category"]).strip()]

            for cat in categories:
                cat = str(cat).strip()
                if cat:
                    cursor.execute("SELECT id FROM NewsCategory WHERE category = ?;", (cat,))
                    result = cursor.fetchone()
                    if result:
                        cursor.execute(
                            "INSERT OR IGNORE INTO NewsArticleCategory (news_id, category_id) VALUES (?, ?);",
                            (news_id, result[0])
                        )


# ------------------------------------------------------------------------------
#  Load Source Table (Extract Domain)
# ------------------------------------------------------------------------------

def load_source_table(cursor, df_full):
    """
    Create and populate a table mapping news_id to its source domain.
    """

    df = df_full.copy()

    # Create source table with UNIQUE constraint
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsSource (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT UNIQUE NOT NULL
        )
    """)

    # Extract and clean domains
    sources_dict = {}  # {source_name: id}
    
    for _, row in df.iterrows():
        url = row.get("url", "")
        if pd.notna(url) and url:
            # Extract domain
            match = re.search(r"https?://([^/]+)", str(url))
            if match:
                domain = match.group(1).lower()
                
                # Clean domain
                domain = re.sub(r"^www\.", "", domain)
                domain = re.sub(r"\.(com|co|org|net|io|uk|de|fr|it|es|br|ru|cn|jp|au|ca).*", "", domain)
                
                if domain and domain not in sources_dict:
                    # Insert with IGNORE to handle duplicates
                    cursor.execute("INSERT OR IGNORE INTO NewsSource (source) VALUES (?);", (domain,))
                    
                    # Get the id
                    cursor.execute("SELECT id FROM NewsSource WHERE source = ?;", (domain,))
                    result = cursor.fetchone()
                    if result:
                        sources_dict[domain] = result[0]

    # Update NewsArticles with source_id
    for _, row in df.iterrows():
        news_id = row.get("id")
        url = row.get("url", "")
        
        if pd.notna(url) and url:
            match = re.search(r"https?://([^/]+)", str(url))
            if match:
                domain = match.group(1).lower()
                domain = re.sub(r"^www\.", "", domain)
                domain = re.sub(r"\.(com|co|org|net|io|uk|de|fr|it|es|br|ru|cn|jp|au|ca).*", "", domain)
                
                if domain in sources_dict:
                    cursor.execute(
                        "UPDATE NewsArticles SET source_id = ? WHERE id = ?;",
                        (sources_dict[domain], news_id)
                    )


# ------------------------------------------------------------------------------
#  Main Process
# ------------------------------------------------------------------------------
def main(data_folder="data", db_path="data/news.db"):
    """
    Main function to convert CSV to SQLite database.
    """
    print("🔍 Looking for latest CSV in", data_folder, "...")
    latest_csv = get_latest_csv(data_folder)
    print(f"📄 Latest CSV detected: {latest_csv}")

    # Load data
    print("📖 Loading CSV data...")
    df_full = pd.read_csv(latest_csv)
    print(f"✅ Loaded {len(df_full)} rows")

    # Prepare dataframe for main table
    df = df_full.copy()
    
    # Handle published date
    if "published" in df.columns:
        df["published"] = pd.to_datetime(df["published"], errors="coerce").dt.date
    
    # Drop unnecessary columns for main table
    df = df.drop(columns=["category", "url"], errors="ignore")

    # Connect to SQLite
    print(f"🔗 Connecting to SQLite: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("🛠️ Creating database tables...")

    try:
        load_main_table(cursor, df)
        print("   ✅ NewsArticles table created")
        
        load_category_table(cursor, df_full)
        print("   ✅ NewsCategory tables created")
        
        load_source_table(cursor, df_full)
        print("   ✅ NewsSource table created")

        conn.commit()
        
        # Verify
        print("\n📊 Database statistics:")
        cursor.execute("SELECT COUNT(*) FROM NewsArticles;")
        article_count = cursor.fetchone()[0]
        print(f"   📰 Articles: {article_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsCategory;")
        category_count = cursor.fetchone()[0]
        print(f"   📂 Categories: {category_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsSource;")
        source_count = cursor.fetchone()[0]
        print(f"   🌐 Sources: {source_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsArticleCategory;")
        assoc_count = cursor.fetchone()[0]
        print(f"   🔗 Associations: {assoc_count}\n")
        
        print("✅ Database successfully built from latest CSV!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()