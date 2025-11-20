# 📰 Currents News Reporting - Data Engineering Pipeline

**MLDS 400 Final Project**

A complete data engineering workflow for news data: **Currents API → Airflow ETL → CSV → PostgreSQL + SQLite → Shiny Dashboard**

All services containerized with Docker Compose.

---

## 🎯 Project Overview

This project demonstrates a full data pipeline:

1. **Data Collection:** Fetch news articles from Currents API
2. **ETL Processing:** Apache Airflow orchestrates data pipeline with 3 tasks
3. **Data Storage:** Store in both PostgreSQL (analytical) and SQLite (dashboard)
4. **Data Visualization:** Interactive Shiny dashboard for exploring news data

**Key Technologies:**
- Apache Airflow (orchestration)
- PostgreSQL (relational database)
- SQLite (lightweight database)
- Shiny (interactive dashboard)
- Docker (containerization)

---

## 📁 Project Structure

```
currents-news-reporting/
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Airflow image
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
├── README.md                  # This file
│
├── airflow/
│   └── dags/
│       └── news_pipeline_dag.py    # Main ETL DAG (3 tasks)
│
├── src/
│   ├── __init__.py
│   ├── news_api_utils.py           # Currents API integration
│   ├── db_to_postgres.py           # PostgreSQL ETL
│   └── db_utils.py                 # SQLite conversion
│
├── R_app/
│   ├── app.R                       # Shiny dashboard
│   ├── Dockerfile.shine            # Shiny image
│   └── install_packages.R          # R dependencies
│
└── data/
    ├── news_output.csv             # Generated CSV (645+ articles)
    ├── news.db                     # SQLite database (Shiny uses)
    └── logs/                       # Processing logs
```

---

## 🚀 Quick Start

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API key:
# CURRENTS_API_KEY=your_api_key_here
```

**Required Variables:**
```env
CURRENTS_API_KEY=your_key_here          # Get from https://currentsapi.services/
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 2. Start Services

```bash
docker-compose up -d
sleep 30
docker-compose ps
```

**Expected Status:**
```
airflow-postgres    Healthy
airflow-webserver   Up
airflow-scheduler   Up
```

### 3. Initialize Airflow

```bash
docker-compose exec -T airflow-webserver airflow db init

docker-compose exec -T airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 4. Access Airflow

🔗 **http://localhost:8080**
- Username: `admin`
- Password: `admin`

### 5. Start Shiny Dashboard

```bash
docker build -t news-shiny -f R_app/Dockerfile.shine .
docker run -p 3838:3838 -v $(pwd)/data:/data news-shiny
```

🔗 **http://localhost:3838**

---

## 🔄 Airflow DAG Operations

### View DAGs

```bash
# List all DAGs
docker-compose exec -T airflow-webserver airflow dags list

# Get DAG details
docker-compose exec -T airflow-webserver airflow dags info news_pipeline

# List tasks in DAG
docker-compose exec -T airflow-webserver airflow tasks list news_pipeline
```

### Trigger DAG

**Via Web UI:**
1. Go to http://localhost:8080
2. Find "news_pipeline" DAG
3. Click play button or "Trigger DAG"

**Via CLI:**
```bash
# Simple trigger
docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline

# With execution date
docker-compose exec -T airflow-webserver \
  airflow dags trigger news_pipeline --exec-date 2025-11-19

# With configuration
docker-compose exec -T airflow-webserver \
  airflow dags trigger news_pipeline \
  --conf '{"keyword":"technology"}'
```

### Monitor Execution

```bash
# Watch scheduler logs
docker-compose logs -f airflow-scheduler

# Filter by DAG
docker-compose logs airflow-scheduler | grep news_pipeline

# Filter by task
docker-compose logs airflow-scheduler | grep csv_to_sqlite

# Last 50 lines
docker-compose logs -f airflow-scheduler --tail 50
```

### Test Tasks

```bash
# Test single task (dry run)
docker-compose exec -T airflow-webserver \
  airflow tasks test news_pipeline fetch_news 2025-11-19

# Test all tasks in DAG
docker-compose exec -T airflow-webserver \
  airflow dags test news_pipeline 2025-11-19
```

### Manage DAG

```bash
# Pause DAG (stop scheduling)
docker-compose exec -T airflow-webserver airflow dags pause news_pipeline

# Unpause DAG (resume scheduling)
docker-compose exec -T airflow-webserver airflow dags unpause news_pipeline

# Clear task instances (allow re-run)
docker-compose exec -T airflow-webserver \
  airflow tasks clear news_pipeline -t csv_to_sqlite

# Clear all tasks
docker-compose exec -T airflow-webserver \
  airflow tasks clear news_pipeline
```

### Set Variables

```bash
# Via CLI
docker-compose exec -T airflow-webserver \
  airflow variables set news_keyword "python"

# Get variable
docker-compose exec -T airflow-webserver \
  airflow variables get news_keyword
```

**Or via Web UI:**
1. Admin → Variables
2. Click + (create)
3. Key: `news_keyword`
4. Value: `python`
5. Save

### Enter Container Shell

```bash
# SSH into webserver
docker-compose exec airflow-webserver bash

# Check API key loaded
docker-compose exec -T airflow-webserver \
  printenv | grep CURRENTS_API_KEY

# View DAG file
docker-compose exec -T airflow-webserver \
  cat airflow/dags/news_pipeline_dag.py

# Check data directory
docker-compose exec -T airflow-webserver \
  ls -lh /opt/airflow/data/
```

---

## 📊 DAG Tasks (Pipeline Flow)

```
fetch_news → load_postgres → csv_to_sqlite
```

### Task 1: fetch_news (API → CSV)
- Calls Currents API for news articles
- Saves to `/opt/airflow/data/news_output.csv`
- Time: ~2 minutes
- Retries: 2

### Task 2: load_postgres (CSV → PostgreSQL)
- Reads CSV file
- Creates/updates PostgreSQL tables
- Time: ~1 minute
- Retries: 1
- Note: Failure doesn't interrupt pipeline

### Task 3: csv_to_sqlite (CSV → SQLite)
- Converts CSV to SQLite database
- Generates `/data/news.db` (used by Shiny)
- Time: ~30 seconds
- Retries: 1

---

## 🗄️ PostgreSQL Operations

### Access Database

```bash
# Enter PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow
```

### Query Data

```bash
# Inside psql:
SELECT COUNT(*) FROM newsarticles;
SELECT * FROM newsarticles LIMIT 5;
SELECT category, COUNT(*) FROM newscategory GROUP BY category;
SELECT source, COUNT(*) FROM newssource GROUP BY source;
\q  # Exit

# Or execute directly
docker-compose exec -T postgres \
  psql -U airflow -d airflow \
  -c "SELECT COUNT(*) FROM newsarticles;"
```

### Backup Database

```bash
# Backup to SQL file
docker-compose exec -T postgres \
  pg_dump -U airflow airflow > backup_$(date +%Y%m%d_%H%M%S).sql

# Export to CSV
docker-compose exec -T postgres \
  psql -U airflow -d airflow \
  -c "COPY newsarticles TO STDOUT WITH CSV HEADER;" > articles.csv
```

---

## 📊 Data Verification

```bash
# Check CSV
ls -lh data/news_output.csv
wc -l data/news_output.csv

# Check SQLite
ls -lh data/news.db

# SQLite row count
docker-compose exec -T airflow-webserver \
  sqlite3 data/news.db "SELECT COUNT(*) FROM NewsArticles;"

# PostgreSQL row count
docker-compose exec -T postgres \
  psql -U airflow -d airflow -c "SELECT COUNT(*) FROM newsarticles;"

# View recent articles
docker-compose exec -T airflow-webserver \
  sqlite3 data/news.db \
  "SELECT title, published FROM NewsArticles ORDER BY published DESC LIMIT 5;"
```

---

## 🔧 Docker Commands

```bash
# View container status
docker-compose ps

# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres

# Restart services
docker-compose restart
docker-compose restart airflow-scheduler

# Stop services (keep data)
docker-compose stop

# Stop and remove (keep data)
docker-compose down

# Full cleanup (delete everything!)
docker-compose down -v
```

---

## 🐛 Troubleshooting

### DAG Not Showing?

```bash
# Check Python syntax
docker-compose exec -T airflow-webserver \
  python3 -m py_compile airflow/dags/news_pipeline_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
sleep 30
```

### Task Failed?

```bash
# Check logs
docker-compose logs airflow-scheduler | grep ERROR

# Re-run task
docker-compose exec -T airflow-webserver \
  airflow tasks clear news_pipeline -t csv_to_sqlite
```

### API Key Missing?

```bash
# Verify .env
cat .env | grep CURRENTS_API_KEY

# Check in container
docker-compose exec -T airflow-webserver \
  printenv | grep CURRENTS_API_KEY

# Restart to reload
docker-compose restart airflow-webserver airflow-scheduler
```

### PostgreSQL Error?

```bash
# Test connection
docker-compose exec -T postgres pg_isready

# Restart
docker-compose restart postgres
sleep 10
```

---

## 📋 Database Schema

### newsarticles
```sql
id TEXT PRIMARY KEY
title TEXT
description TEXT
author TEXT
image TEXT
language TEXT
published DATE
```

### newscategory
```sql
id SERIAL PRIMARY KEY
news_id TEXT REFERENCES newsarticles(id)
category TEXT
```

### newssource
```sql
id SERIAL PRIMARY KEY
news_id TEXT REFERENCES newsarticles(id)
source TEXT
```

---

## 🎯 Typical Workflow

```bash
# 1. Start everything
docker-compose up -d && sleep 30

# 2. Check status
docker-compose ps

# 3. Trigger DAG
docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline

# 4. Watch logs
docker-compose logs -f airflow-scheduler

# 5. Verify data
docker-compose exec -T airflow-webserver \
  sqlite3 data/news.db "SELECT COUNT(*) FROM NewsArticles;"

# 6. Start Shiny (another terminal)
docker build -t news-shiny -f R_app/Dockerfile.shine .
docker run -p 3838:3838 news-shiny

# 7. Access dashboards
# Airflow: http://localhost:8080
# Shiny: http://localhost:3838
```

---

## 📝 Quick Reference

| Task | Command |
|------|---------|
| List DAGs | `airflow dags list` |
| Trigger DAG | `airflow dags trigger news_pipeline` |
| View logs | `docker-compose logs -f airflow-scheduler` |
| Test task | `airflow tasks test news_pipeline fetch_news 2025-11-19` |
| Clear task | `airflow tasks clear news_pipeline -t csv_to_sqlite` |
| Pause DAG | `airflow dags pause news_pipeline` |
| Unpause DAG | `airflow dags unpause news_pipeline` |
| Enter psql | `psql -U airflow -d airflow` |

---

## 🔗 Links

- **Currents API:** https://currentsapi.services/
- **Airflow Docs:** https://airflow.apache.org/docs/
- **PostgreSQL Docs:** https://www.postgresql.org/docs/

---

## 📚 What You Learn

This project covers:
- ✅ ETL pipeline design
- ✅ Airflow orchestration
- ✅ Docker containerization
- ✅ PostgreSQL database operations
- ✅ Data visualization with Shiny
- ✅ API integration
- ✅ Data processing and transformation

---

**Final Project Ready! 🚀**