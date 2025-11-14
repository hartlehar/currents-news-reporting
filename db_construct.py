import pandas as pd
import sqlite3
import re

# === Load and clean data ===
df = pd.read_csv("Aug_Oct_News.csv")
df = df.drop(columns=["category", "url"])
df["published"] = pd.to_datetime(df["published"]).dt.date

# === Connect to SQLite and create the NewsArticles table ===
conn = sqlite3.connect("news.db")
cursor = conn.cursor()

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

df.to_sql("NewsArticles", conn, if_exists="replace", index=False)

# === NewsCategory table ===
origin = pd.read_csv("Aug_Oct_News.csv")
df = origin.copy()
df["category"] = df["category"].replace(r"[\[\]']", '', regex=True).str.split(r",\s*")
df = df.explode("category", ignore_index=True)
df_category = df[["id", "category"]].rename(columns={"id": "news_id"})

cursor.execute("""
CREATE TABLE IF NOT EXISTS NewsCategory (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id TEXT NOT NULL,
    category TEXT,
    FOREIGN KEY (news_id) REFERENCES NewsArticles(id)
)
""")

df_category.to_sql("TempCategory", conn, if_exists="replace", index=False)
cursor.execute("""
INSERT INTO NewsCategory (news_id, category)
SELECT news_id, category FROM TempCategory;
""")
cursor.execute("DROP TABLE IF EXISTS TempCategory;")

# === NewsSource table ===
df_source = origin[["id", "url"]].rename(columns={"id": "news_id"})
df_source["source"] = df_source["url"].fillna("").apply(
    lambda x: re.search(r"https?://([^/]+)/", x).group(1) if re.search(r"https?://([^/]+)/", x) else None
)
df_source["source"] = (
    df_source["source"]
    .str.replace(r"^www\.", "", regex=True)
    .str.replace(r"\.(com|co|org).*", "", regex=True)
)
df_source = df_source.drop(columns=["url"])

cursor.execute("""
CREATE TABLE IF NOT EXISTS NewsSource (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_id TEXT NOT NULL,
    source TEXT NOT NULL,
    FOREIGN KEY (news_id) REFERENCES NewsArticles(id)
)
""")

df_source.to_sql("TempSource", conn, if_exists="replace", index=False)
cursor.execute("""
INSERT INTO NewsSource (news_id, source)
SELECT news_id, source FROM TempSource
WHERE source IS NOT NULL AND TRIM(source) != '';
""")
cursor.execute("DROP TABLE IF EXISTS TempSource;")

# === Finalize ===
conn.commit()
conn.close()