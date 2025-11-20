import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# 添加项目根目录到 Python 路径
sys.path.insert(0, '/opt/airflow')

# 现在可以导入 src 模块
from src.news_api_utils import fetch_news_to_csv
from src.db_to_postgres import run_load_to_postgres
from src.db_utils import main as csv_to_sqlite


# -------------------------------------------------------------------
# Task 1: Fetch news from API and save CSV into /opt/airflow/data
# -------------------------------------------------------------------
def task_fetch_news(**context):
    """
    Fetch news from Currents API with dynamic date range.
    Uses execution_date to determine which period to fetch.
    """
    print("\n" + "="*70)
    print("TASK 1: FETCH NEWS FROM API")
    print("="*70 + "\n")
    
    api_key = os.getenv("CURRENTS_API_KEY")
    if not api_key:
        raise ValueError("❌ Missing CURRENTS_API_KEY in environment!")

    # Get execution date from Airflow context
    execution_date = context['execution_date']
    
    # Fetch news for the previous day (since we run daily)
    # This allows time for news to be processed
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = execution_date.strftime("%Y-%m-%d")
    
    # Get configurable keyword from Airflow Variables (default: "technology")
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

    print("✅ News fetched and saved to CSV.\n")


# -------------------------------------------------------------------
# Task 2: Load CSV into PostgreSQL
# -------------------------------------------------------------------
def task_load_postgres(**context):
    """
    Load CSV data into PostgreSQL.
    Uses execution context for logging and tracking.
    """
    print("\n" + "="*70)
    print("TASK 2: LOAD CSV INTO POSTGRESQL")
    print("="*70 + "\n")
    
    run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    
    print(f"🗄️ Loading CSV data into PostgreSQL...")
    print(f"📍 DAG Run ID: {run_id}")
    print(f"📅 Execution Date: {execution_date}\n")
    
    try:
        run_load_to_postgres()
        print("✅ PostgreSQL ETL complete!\n")
    except Exception as e:
        print(f"⚠️ PostgreSQL load warning: {str(e)}")
        print("⏭️ Continuing to SQLite conversion...\n")
        # 不中断流程，继续到 SQLite


# -------------------------------------------------------------------
# Task 3: Convert CSV to SQLite (New!)
# -------------------------------------------------------------------
def task_csv_to_sqlite(**context):
    """
    Convert CSV data to SQLite database using db_utils.py.
    Creates NewsArticles, NewsCategory, NewsArticleCategory, and NewsSource tables.
    """
    print("\n" + "="*70)
    print("TASK 3: CONVERT CSV TO SQLITE")
    print("="*70 + "\n")
    
    execution_date = context['execution_date']
    
    print(f"💾 Converting CSV to SQLite...")
    print(f"📅 Execution Date: {execution_date}\n")
    
    try:
        csv_to_sqlite(
            data_folder="/opt/airflow/data",
            db_path="/opt/airflow/data/news.db"
        )
        
        print("\n✅ SQLite conversion complete!\n")
        
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
    
    This DAG fetches news articles from Currents API, loads them into PostgreSQL, 
    and converts the CSV to SQLite for use with Shiny.
    
    ## Schedule
    - Runs daily at 00:00 UTC
    
    ## Tasks
    1. **fetch_news**: Fetches news articles from API (previous day)
    2. **load_postgres**: Loads CSV data into PostgreSQL database
    3. **csv_to_sqlite**: Converts CSV to SQLite for Shiny dashboard
    
    ## Configuration
    - Set Airflow Variable `news_keyword` to change search keyword (default: "technology")
    - API key must be provided via CURRENTS_API_KEY environment variable
    
    ## Output
    - CSV: /opt/airflow/data/news_output.csv
    - PostgreSQL: airflow database (NewsArticles table)
    - SQLite: /opt/airflow/data/news.db
    
    ## Data Flow
    API → CSV → PostgreSQL
              → SQLite → Shiny
    """,
) as dag:

    fetch_news = PythonOperator(
        task_id="fetch_news",
        python_callable=task_fetch_news,
        provide_context=True,  # Enable context parameter passing
        retries=2,  # Retry up to 2 times on failure
        retry_delay=timedelta(minutes=5),  # Wait 5 minutes between retries
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load_postgres,
        provide_context=True,  # Enable context parameter passing
        retries=1,  # Retry once on failure
        retry_delay=timedelta(minutes=2),
    )

    csv_to_sqlite_task = PythonOperator(
        task_id="csv_to_sqlite",
        python_callable=task_csv_to_sqlite,
        provide_context=True,  # Enable context parameter passing
        retries=1,  # Retry once on failure
        retry_delay=timedelta(minutes=2),
    )

    # Workflow: fetch → load_postgres → csv_to_sqlite
    # 即使 load_postgres 失败，也会继续到 csv_to_sqlite
    fetch_news >> load_postgres >> csv_to_sqlite_task