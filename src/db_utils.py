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
#  Load Articles Table (with incremental update)
# ------------------------------------------------------------------------------

def load_main_table(cursor, df):
    """
    Create and populate the NewsArticles table.
    Supports incremental updates - only inserts new articles.
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

    # Get existing IDs to avoid duplicates
    cursor.execute("SELECT id FROM NewsArticles")
    existing_ids = {row[0] for row in cursor.fetchall()}
    
    print(f"   📊 Existing articles in DB: {len(existing_ids)}")

    # Insert only new data
    articles_list = []
    new_count = 0
    for _, row in df.iterrows():
        article_id = row.get("id")
        
        # Skip if already exists
        if article_id in existing_ids:
            continue
        
        articles_list.append((
            article_id,
            row.get("title", ""),
            row.get("description", ""),
            row.get("author", ""),
            None,  # source_id will be updated later
            row.get("url", ""),
            row.get("image", ""),
            row.get("language", ""),
            row.get("published", None)
        ))
        new_count += 1
    
    if articles_list:
        cursor.executemany(
            """INSERT OR IGNORE INTO NewsArticles 
               (id, title, description, author, source_id, url, image, language, published)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            articles_list
        )
        print(f"   ✅ Inserted {new_count} new articles")
    else:
        print(f"   ℹ️  No new articles to insert (all data already in DB)")


# ------------------------------------------------------------------------------
#  Load Category Table (One-to-Many) with incremental update
# ------------------------------------------------------------------------------

def load_category_table(cursor, df_full):
    """
    Create and populate the NewsCategory table.
    Each news can have multiple categories.
    Supports incremental updates.
    """

    df = df_full.copy()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsCategory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT UNIQUE
        )
    """)

    # Create association table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsArticleCategory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id TEXT,
            category_id INTEGER,
            UNIQUE(news_id, category_id),
            FOREIGN KEY (news_id) REFERENCES NewsArticles(id),
            FOREIGN KEY (category_id) REFERENCES NewsCategory(id)
        )
    """)

    # Get existing article IDs to find new ones
    cursor.execute("SELECT id FROM NewsArticles")
    all_article_ids = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT DISTINCT news_id FROM NewsArticleCategory")
    categorized_articles = {row[0] for row in cursor.fetchall()}
    
    # Only process new articles
    new_articles = all_article_ids - categorized_articles

    # Parse categories safely and insert
    unique_cats = set()
    articles_to_categorize = []
    
    for _, row in df.iterrows():
        article_id = row.get("id")
        
        # Only process new articles
        if article_id not in new_articles:
            continue
        
        if pd.notna(row.get("category")):
            try:
                cats = ast.literal_eval(str(row["category"]))
                if isinstance(cats, list):
                    categories = [str(c).strip() for c in cats if c]
                else:
                    categories = [str(cats).strip()]
            except (ValueError, SyntaxError):
                categories = [str(row["category"]).strip()]
            
            for cat in categories:
                cat = cat.strip()
                if cat:
                    unique_cats.add(cat)
                    articles_to_categorize.append((article_id, cat))

    # Insert new categories
    if unique_cats:
        for cat in unique_cats:
            cursor.execute("INSERT OR IGNORE INTO NewsCategory (category) VALUES (?);", (cat,))
        print(f"   ✅ Inserted {len(unique_cats)} categories")

    # Insert associations for new articles
    if articles_to_categorize:
        for article_id, cat in articles_to_categorize:
            cursor.execute("SELECT id FROM NewsCategory WHERE category = ?;", (cat,))
            result = cursor.fetchone()
            if result:
                cursor.execute(
                    "INSERT OR IGNORE INTO NewsArticleCategory (news_id, category_id) VALUES (?, ?);",
                    (article_id, result[0])
                )
        print(f"   ✅ Inserted {len(articles_to_categorize)} article-category associations")
    else:
        print(f"   ℹ️  No new categories to assign")


# ------------------------------------------------------------------------------
#  Load Source Table (Extract Domain) with incremental update
# ------------------------------------------------------------------------------

def load_source_table(cursor, df_full):
    """
    Create and populate a table mapping news_id to its source domain.
    Supports incremental updates.
    """

    df = df_full.copy()

    # Create source table with UNIQUE constraint
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsSource (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT UNIQUE NOT NULL
        )
    """)

    # Get articles that need source assignment
    cursor.execute("SELECT id FROM NewsArticles WHERE source_id IS NULL")
    articles_without_source = {row[0] for row in cursor.fetchall()}

    if not articles_without_source:
        print(f"   ℹ️  All articles already have sources assigned")
        return

    # Extract and clean domains
    sources_dict = {}  # {source_name: id}
    
    # Get existing sources
    cursor.execute("SELECT id, source FROM NewsSource")
    for source_id, source_name in cursor.fetchall():
        sources_dict[source_name] = source_id
    
    # Process new articles
    updates = []
    new_sources = set()
    
    for _, row in df.iterrows():
        news_id = row.get("id")
        
        # Skip if not in articles without source
        if news_id not in articles_without_source:
            continue
        
        url = row.get("url", "")
        if pd.notna(url) and url:
            # Extract domain
            match = re.search(r"https?://([^/]+)", str(url))
            if match:
                domain = match.group(1).lower()
                
                # Clean domain
                domain = re.sub(r"^www\.", "", domain)
                domain = re.sub(r"\.(com|co|org|net|io|uk|de|fr|it|es|br|ru|cn|jp|au|ca).*", "", domain)
                
                if domain:
                    new_sources.add(domain)
                    updates.append((news_id, domain))

    # Insert new sources
    for domain in new_sources:
        if domain not in sources_dict:
            cursor.execute("INSERT OR IGNORE INTO NewsSource (source) VALUES (?);", (domain,))
            cursor.execute("SELECT id FROM NewsSource WHERE source = ?;", (domain,))
            result = cursor.fetchone()
            if result:
                sources_dict[domain] = result[0]

    if new_sources:
        print(f"   ✅ Inserted {len(new_sources)} new sources")

    # Update articles with source_id
    if updates:
        for news_id, domain in updates:
            if domain in sources_dict:
                cursor.execute(
                    "UPDATE NewsArticles SET source_id = ? WHERE id = ?;",
                    (sources_dict[domain], news_id)
                )
        print(f"   ✅ Updated {len(updates)} articles with sources")


# ------------------------------------------------------------------------------
#  Main Process
# ------------------------------------------------------------------------------
def main(data_folder="data", db_path="data/news.db"):
    """
    Main function to convert CSV to SQLite database.
    Supports incremental updates - only adds new data.
    """
    print("🔍 Looking for latest CSV in", data_folder, "...")
    latest_csv = get_latest_csv(data_folder)
    print(f"📋 Latest CSV detected: {latest_csv}")

    # Load data
    print("📥 Loading CSV data...")
    df_full = pd.read_csv(latest_csv)
    print(f"✅ Loaded {len(df_full)} rows from CSV")

    # Handle published date
    if "published" in df_full.columns:
        df_full["published"] = pd.to_datetime(df_full["published"], errors="coerce").dt.date

    # Connect to SQLite
    print(f"🗄️ Connecting to SQLite: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("🛠️ Updating database tables (incremental mode)...\n")

    try:
        load_main_table(cursor, df_full)
        load_category_table(cursor, df_full)
        load_source_table(cursor, df_full)

        conn.commit()
        
        # Verify
        print("\n📊 Database statistics:")
        cursor.execute("SELECT COUNT(*) FROM NewsArticles;")
        article_count = cursor.fetchone()[0]
        print(f"   📰 Articles: {article_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsCategory;")
        category_count = cursor.fetchone()[0]
        print(f"   🏷️  Categories: {category_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsSource;")
        source_count = cursor.fetchone()[0]
        print(f"   🌐 Sources: {source_count}")
        
        cursor.execute("SELECT COUNT(*) FROM NewsArticleCategory;")
        assoc_count = cursor.fetchone()[0]
        print(f"   🔗 Associations: {assoc_count}\n")
        
        print("✅ Database successfully updated from latest CSV!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()