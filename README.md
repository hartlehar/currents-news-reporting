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

```
currents-news-reporting/
│
currents-news-reporting/
├── docker-compose.yml        # Docker 编排
├── Dockerfile                # Airflow 镜像
├── Dockerfile.shiny          # Shiny 镜像
├── requirements.txt          # Python 依赖
├── .env.example              # 环境变量模板
├── .gitignore                # Git 忽略
├── README.md                 # 本文件
│
├── airflow/
│   └── dags/
│       └── news_pipeline_dag.py
│
├── src/
│   ├── __init__.py
│   ├── news_api_utils.py
│   └── db_to_postgres.py
│
├── shiny/
│   └── app.R
│
├── data/
│   ├── csv/
│   └── logs/
│
└── logs/
    └── (airflow logs)
```

<<<<<<< Updated upstream
## System Architecture
```
            ┌──────────────┐
            │ Currents API │
            └──────┬───────┘
                   ↓
        ┌───────────────────────┐
        │ Data Extraction (Python) │
        └──────────┬────────────┘
                   ↓
  ┌───────────────────────────────────┐
  │ Cleaning & Transformation (Pandas)│
  └──────────────────┬────────────────┘
                     ↓
          ┌─────────────────┐
          │ SQL Database    │
          │  (SQLite)       │
          └─────────────────┘
                     ↓
      ┌──────────────────────────┐
      │    EDA, Visualization    │
      └──────────────────────────┘
```
=======
=======
http://127.0.0.1:8080


# 构建镜像
docker-compose build

# 启动容器
docker-compose up -d

# 查看状态
docker-compose ps

# 初始化 Airflow 数据库
docker-compose exec airflow-webserver airflow db init

# 创建管理员用户
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
>>>>>>> Stashed changes
