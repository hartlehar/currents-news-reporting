import sys
import os
from datetime import datetime, timedelta

# ---------------------------------------------
# Fix PYTHONPATH before importing anything else
# ---------------------------------------------
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.news_api_utils import fetch_news_to_csv
from src.db_to_postgres import run_load_to_postgres



# -------------------------------------------------------------------
# Task 1: Fetch news from API and save CSV into /opt/airflow/data
# -------------------------------------------------------------------
def task_fetch_news(**context):
    """
    Fetch news from Currents API with dynamic date range.
    Uses execution_date to determine which period to fetch.
    """
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
        output_csv="/opt/airflow/data/news_output.csv"
    )

    print("✅ News fetched and saved to CSV.")


# -------------------------------------------------------------------
# Task 2: Load CSV into PostgreSQL
# -------------------------------------------------------------------
def task_load_postgres(**context):
    """
    Load CSV data into PostgreSQL.
    Uses execution context for logging and tracking.
    """
    run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    
    print(f"🗄️ Loading CSV data into PostgreSQL...")
    print(f"📍 DAG Run ID: {run_id}")
    print(f"📅 Execution Date: {execution_date}")
    
    run_load_to_postgres()
    
    print("✅ PostgreSQL ETL complete!")


# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
with DAG(
    dag_id="news_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["news", "etl", "postgres"],
    default_view="graph",
    doc_md="""
    # News Pipeline DAG
    
    This DAG fetches news articles from Currents API and loads them into PostgreSQL.
    
    ## Schedule
    - Runs daily at 00:00 UTC
    
    ## Tasks
    1. **fetch_news**: Fetches news articles from API (previous day)
    2. **load_postgres**: Loads CSV data into PostgreSQL database
    
    ## Configuration
    - Set Airflow Variable `news_keyword` to change search keyword (default: "technology")
    - API key must be provided via CURRENTS_API_KEY environment variable
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

    # Workflow: fetch → load
    fetch_news >> load_postgres