import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Add project root to Python path
sys.path.insert(0, '/opt/airflow')

# Now imports from src/ work correctly
from src.news_api_utils import fetch_news_to_csv
from src.db_to_postgres import run_load_to_postgres
from src.db_utils import main as csv_to_sqlite


# -------------------------------------------------------------------
# Task 1: Fetch news from API and save CSV into /opt/airflow/data
# -------------------------------------------------------------------
def task_fetch_news(**context):
    """
    Fetch news from Currents API using a dynamic date range.
    The execution_date provided by Airflow determines which date
    range to query.
    """
    api_key = os.getenv("CURRENTS_API_KEY")
    if not api_key:
        raise ValueError("❌ Missing CURRENTS_API_KEY in environment!")

    # Get execution date from Airflow context
    execution_date = context['execution_date']
    
    # Fetch news for the previous day (daily pipeline)
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = execution_date.strftime("%Y-%m-%d")
    
    # Get keyword from Airflow Variable (default: technology)
    keyword = Variable.get("news_keyword", "technology")
    
    print(f"🌐 Fetching news articles...")
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"🔑 Keyword: {keyword}")

    fetch_news_to_csv(
        api_key=api_key,
        start=start_date,
        end=end_date,
        keyword=keyword,
        output_csv="news_output.csv"
    )

    print("✅ News fetched and saved to CSV.")


# -------------------------------------------------------------------
# Task 2: Load CSV into PostgreSQL
# -------------------------------------------------------------------
def task_load_postgres(**context):
    """
    Load the CSV file into PostgreSQL.
    Uses execution context for logging and run tracking.
    """
    run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    
    print(f"🗄️ Loading CSV data into PostgreSQL...")
    print(f"📍 DAG Run ID: {run_id}")
    print(f"📅 Execution Date: {execution_date}")
    
    run_load_to_postgres()
    
    print("✅ PostgreSQL ETL complete!")
    

# -------------------------------------------------------------------
# Task 3: Convert CSV to SQLite
# -------------------------------------------------------------------
def task_csv_to_sqlite(**context):
    """
    Convert the CSV file into an SQLite database using db_utils.py.
    Creates the NewsArticles, NewsCategory, NewsArticleCategory,
    and NewsSource tables.
    """
    print("\n" + "="*70)
    print("TASK 3: CONVERT CSV TO SQLITE")
    print("="*70 + "\n")
    
    execution_date = context['execution_date']
    data_folder = "/opt/airflow/data"
    db_path = os.path.join(data_folder, "news.db")
    
    print(f"💾 Converting CSV to SQLite...")
    print(f"📅 Execution Date: {execution_date}")
    print(f"📁 Data folder: {data_folder}")
    print(f"🗄️ SQLite DB path: {db_path}\n")
    
    try:
        csv_to_sqlite(
            data_folder=data_folder,
            db_path=db_path
        )
        
        # Validate DB creation
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)
            print(f"\n✅ SQLite conversion complete!")
            print(f"📊 SQLite DB size: {db_size} bytes\n")
        else:
            raise FileNotFoundError(f"❌ SQLite DB not created at {db_path}")
        
    except Exception as e:
        print(f"❌ SQLite conversion failed: {str(e)}\n")
        raise


# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
with DAG(
    dag_id="news_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["news", "etl", "postgres", "sqlite"],
    default_view="graph",
    doc_md="""
    # News Pipeline DAG
    
    This DAG fetches news articles from the Currents API, loads them into PostgreSQL,
    and converts the CSV file into an SQLite database for Shiny dashboard use.
    
    ## Schedule
    - Runs daily at 00:00 UTC
    
    ## Tasks
    1. **fetch_news** – Fetch news from the previous day and save as CSV  
    2. **load_postgres** – Load the CSV into PostgreSQL  
    3. **csv_to_sqlite** – Convert the CSV to SQLite for Shiny
    
    ## Configuration
    - Set Airflow Variable `news_keyword` to choose the search keyword  
    - Environment variable `CURRENTS_API_KEY` is required  
    
    ## Output
    - CSV: `/opt/airflow/data/news_output.csv`
    - PostgreSQL tables: NewsArticles, NewsCategory, NewsSource
    - SQLite database: `/opt/airflow/data/news.db`
    
    ## Data Flow
    API → CSV → PostgreSQL  
                 → SQLite → Shiny dashboard
    
    ## Notes
    - SQLite and PostgreSQL are created independently  
    - CSV must be available for SQLite conversion  
    - If PostgreSQL load fails, the pipeline continues to SQLite
    """,
) as dag:

    fetch_news = PythonOperator(
        task_id="fetch_news",
        python_callable=task_fetch_news,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load_postgres,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    csv_to_sqlite_task = PythonOperator(
        task_id="csv_to_sqlite",
        python_callable=task_csv_to_sqlite,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # Workflow: fetch → load_postgres → csv_to_sqlite
    fetch_news >> load_postgres >> csv_to_sqlite_task
