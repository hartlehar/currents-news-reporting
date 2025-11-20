# 📰 Currents News Reporting

**MLDS 400 Final Project**

Complete news pipeline: **API → Airflow ETL (3 tasks) → PostgreSQL + SQLite → Shiny Dashboard**

---

## 📋 Project Overview

This project demonstrates a full data engineering workflow:

1. **Data Collection** - Fetch news from Currents API
2. **ETL Pipeline** - Airflow orchestrates 3-task pipeline
3. **Data Storage** - PostgreSQL (analytics) + SQLite (dashboard)
4. **Visualization** - Interactive Shiny dashboard

**Key Technologies:**
- Apache Airflow (workflow orchestration)
- PostgreSQL (relational database)
- SQLite (lightweight database)
- R Shiny (interactive dashboard)
- Docker (containerization)

**Pipeline Architecture:**
```
┌─────────────┐
│ Currents    │
│    API      │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────┐
│   Airflow DAG (3 tasks)      │
├──────────────────────────────┤
│ 1. fetch_news (API → CSV)    │
│ 2. load_postgres (CSV → PG)  │
│ 3. csv_to_sqlite (CSV → DB)  │
└──────┬─────────────┬──────────┘
       │             │
       ▼             ▼
   ┌────────┐   ┌──────────┐
   │PostgreSQL  │ SQLite   │
   │(Analytics) │(Shiny)   │
   └────────┘   └──────────┘
                     │
                     ▼
              ┌────────────────┐
              │ Shiny Dashboard│
              │  (Interactive) │
              └────────────────┘
```

---

## 📁 Project Structure

```
currents-news-reporting/
├── .env.example              # Environment template
├── docker-compose.yml        # Container orchestration
├── Dockerfile                # Airflow image definition
├── requirements.txt          # Python dependencies
├── README.md                 # This file
│
├── airflow/dags/
│   └── news_pipeline_dag.py  # Main ETL DAG (3 tasks)
│
├── src/
│   ├── news_api_utils.py     # Currents API functions
│   ├── db_to_postgres.py     # PostgreSQL ETL
│   └── db_utils.py           # SQLite conversion
│
├── R_app/
│   ├── app.R                 # Shiny dashboard
│   ├── Dockerfile.shine      # Shiny container
│   └── install_packages.R    # R dependencies
│
└── data/
    ├── news_output.csv       # Generated CSV (645+ articles)
    └── news.db               # Generated SQLite database
```

---

## ✅ Prerequisites

- Docker & Docker Compose installed
- Currents API Key (free at https://currentsapi.services/)
- ~2GB disk space for data

---

## 🚀 Quick Start

### 1. Setup

```bash
cp .env.example .env
# Edit: CURRENTS_API_KEY=your_key_here
```

**Required Variables:**
```env
CURRENTS_API_KEY=your_key_here
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 2. Start Services

```bash
docker-compose up -d
sleep 30
```

**Expected Output:**
```
NAME                    STATUS
airflow-postgres        Up (healthy)
airflow-webserver       Up
airflow-scheduler       Up
```

### 3. Initialize Airflow

**Wait 30 seconds after docker-compose up, then:**

```bash
# Initialize database
docker-compose exec -T airflow-webserver airflow db init

# Create admin user
docker-compose exec -T airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 4. Access Airflow

**http://localhost:8080** (admin/admin)

1. View DAGs → Should see `news_pipeline`
2. Click play button to trigger DAG
3. Monitor execution in logs

**Or via CLI:**
```bash
docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline
docker-compose logs -f airflow-scheduler
```

### 5. Start Shiny Dashboard

```bash
docker build -t news-shiny -f R_app/Dockerfile.shine .
docker run -p 3838:3838 -v $(pwd)/data:/data news-shiny
```

**Access:** http://localhost:3838

---

## 📊 DAG Tasks

```
fetch_news → load_postgres → csv_to_sqlite
```

| Task | Purpose | Output | Time |
|------|---------|--------|------|
| **fetch_news** | Fetch from API | news_output.csv | 2 min |
| **load_postgres** | Load to PostgreSQL | PostgreSQL DB | 1 min |
| **csv_to_sqlite** | Convert to SQLite | news.db | 30 sec |

---

## 📊 Shiny Dashboard

**Features:**
- 📅 Date/Category/Source filters
- 🔍 Keyword search
- 📈 Charts (daily articles, top sources, top categories)
- 📋 Article table with pagination

**Access:** http://localhost:3838

---

## 🛠️ Common Commands

### Airflow

```bash
# List DAGs
docker-compose exec -T airflow-webserver airflow dags list

# Monitor logs
docker-compose logs -f airflow-scheduler

# Trigger DAG
docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline

# Pause/unpause
docker-compose exec -T airflow-webserver airflow dags pause news_pipeline
docker-compose exec -T airflow-webserver airflow dags unpause news_pipeline

# Clear task (re-run)
docker-compose exec -T airflow-webserver \
  airflow tasks clear news_pipeline -t csv_to_sqlite
```

### PostgreSQL

```bash
# Enter database
docker-compose exec postgres psql -U airflow -d airflow

# Count articles
SELECT COUNT(*) FROM newsarticles;

# Exit
\q
```

### Shiny

```bash
# View logs
docker logs news-shiny-app

# Restart
docker restart news-shiny-app

# Stop
docker stop news-shiny-app
```

---

## ✅ Verify Everything

```bash
# Check containers
docker-compose ps

# Check CSV
wc -l data/news_output.csv

# Check SQLite
docker-compose exec -T airflow-webserver \
  sqlite3 data/news.db "SELECT COUNT(*) FROM NewsArticles;"

# Check PostgreSQL
docker-compose exec -T postgres \
  psql -U airflow -d airflow -c "SELECT COUNT(*) FROM newsarticles;"
```

---

## 🐛 Troubleshooting

**DAG not showing?**
```bash
docker-compose restart airflow-scheduler && sleep 30
```

**Shiny error?**
```bash
docker logs news-shiny-app
docker restart news-shiny-app
```

**API key missing?**
```bash
cat .env | grep CURRENTS_API_KEY
docker-compose restart
```

**Can't connect to PostgreSQL?**
```bash
docker-compose restart postgres && sleep 15
```

---

## 🔗 Links

- **Currents API:** https://currentsapi.services/
- **Airflow Docs:** https://airflow.apache.org/docs/
- **Airflow UI:** http://localhost:8080
- **Shiny Dashboard:** http://localhost:3838

---

**Ready to submit! 🚀**