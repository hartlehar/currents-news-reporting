import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 添加项目根目录到 Python 路径
sys.path.insert(0, '/opt/airflow')

# 现在可以导入 src 模块
from src.news_api_utils import fetch_news_to_csv
from src.db_to_postgres import run_load_to_postgres


# -------------------------------------------------------------------
# Task 1: Fetch news from API and save CSV into /opt/airflow/data
# -------------------------------------------------------------------
def task_fetch_news():
    api_key = os.getenv("CURRENTS_API_KEY")
    if not api_key:
        raise ValueError("❌ Missing CURRENTS_API_KEY in environment!")

    print("🌐 Fetching news articles...")

    fetch_news_to_csv(
        api_key=api_key,
        start="2024-01-01",
        end="2024-01-10",
        keyword="technology",
        output_csv="news_output.csv"
    )

    print("✅ News fetched and saved to CSV.")


# -------------------------------------------------------------------
# Task 2: Load CSV into PostgreSQL
# -------------------------------------------------------------------
def task_load_postgres():
    print("🗄️ Loading CSV data into PostgreSQL...")
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
) as dag:

    fetch_news = PythonOperator(
        task_id="fetch_news",
        python_callable=task_fetch_news,
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=task_load_postgres,
    )

    # Workflow: fetch → load
    fetch_news >> load_postgres