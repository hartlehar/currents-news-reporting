# 📰 Currents News Reporting - Data Engineering Project

## Overview
This project builds an **end-to-end data pipeline** using **Apache Airflow** and **Docker**.  
It fetches live news from the [Currents API](https://currentsapi.services/en), stores it in a database,  
and performs automated analysis to identify top sources and trends.

Developed for **Northwestern University - Fall 2025 Data Engineering Final Project**.

---

## 🧠 Project Objectives
- Read data from an API (Currents API)
- Store the API data in a database
- Build a DAG to orchestrate the pipeline using Apache Airflow
- Automate tasks and measure pipeline performance
- Perform exploratory data analysis (EDA) on the news dataset

---

## 📦 File Structure
currents-news-reporting/
├── dags/
│   └── news_pipeline_dag.py
├── docker-compose.yaml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md


Step 1: Collect News Data
- Run news_api_utils.py to fetch news data and save it as a CSV file in the specified directory.
- When prompted, you will see:
```bash
    API_KEY = input("Please enter your API key: ").strip()
    start = input("Please enter your start date in YYYY-MM-DD format: ").strip()
    end = input("Please enter your end date in YYYY-MM-DD format: ").strip()
    output_name = input("Please enter your output csv file name (must end in .csv): ").strip()
    keyword = input("Please enter your keyword: ").strip()
```
- Example Input: 
```bash
Please enter your API key: 123456789abcdef
Please enter your start date in YYYY-MM-DD format: 2025-08-01
Please enter your end date in YYYY-MM-DD format: 2025-08-02
Please enter your output csv file name (must end in .csv): world_news.csv
Please enter your keyword: world
```

Step 2: Create the Database
- Run db_utils.py to create the SQLite database in the specified directory.
- The database will include the following tables:
    1. NewsArticles — Stores article-level information
    2. NewsCategory — Stores categorized tags for each article
    3. NewsSource — Stores information about article source

After running the script, the database file and all tables will be created automatically.