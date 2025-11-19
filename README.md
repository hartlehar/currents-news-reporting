# 📰 Currents News Reporting - Airflow News Data Pipeline

A complete data engineering project that fetches news data from Currents API using Apache Airflow and stores it in PostgreSQL.

## 🎯 Project Overview

- **Data Source**: [Currents API](https://currentsapi.services/)
- **Data Processing**: Apache Airflow ETL tasks scheduling
- **Data Storage**: PostgreSQL database
- **Deployment**: Docker Compose containerization

## 📁 Project Structure

```
currents-news-reporting/
├── docker-compose.yml          # Docker orchestration file
├── Dockerfile                  # Airflow image configuration
├── requirements.txt            # Python dependencies
├── .env.example               # Environment variables template
├── .gitignore                 # Git ignore rules
├── README.md                  # Project documentation
│
├── airflow/
│   └── dags/
│       └── news_pipeline_dag.py    # Airflow DAG definition
│
├── src/
│   ├── __init__.py                 # Python package init
│   ├── news_api_utils.py           # API call functions
│   └── db_to_postgres.py           # Database loading functions
│
├── data/
│   ├── csv/                    # CSV file storage
│   └── logs/                   # Log files
│
└── logs/
    └── (Airflow logs)
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Valid Currents API Key (free registration at https://currentsapi.services/)

### Step 1: Environment Configuration

```bash
# Copy environment variables template
cp .env.example .env

# Edit .env and add your API Key
nano .env
```

**Key Configuration Items:**
```
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
CURRENTS_API_KEY=your_real_api_key_here  # Required!
```

### Step 2: Start Services

```bash
# Build images and start containers
docker-compose up -d

# Wait 30 seconds for services to start
sleep 30

# Check container status
docker-compose ps
```

**Expected Output:**
```
NAME                  STATUS
airflow-postgres      Healthy
airflow-redis         Healthy
airflow-webserver     Up
airflow-scheduler     Up
airflow-worker        Up
```

### Step 3: Initialize Airflow

```bash
# Initialize Airflow database
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

### Step 4: Access Airflow UI

Open your browser:

```
http://127.0.0.1:8080
```

**Login Credentials:**
- Username: `admin`
- Password: `admin`

## 📊 Running the DAG

### Trigger via UI

1. Visit http://127.0.0.1:8080
2. Click "DAGs" in the left menu
3. Find `news_pipeline` DAG
4. Click the three-dot menu and select "Trigger DAG"
5. Confirm by clicking "Trigger"

### Trigger via Command Line

```bash
# Trigger DAG run
docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline

# View Scheduler logs
docker-compose logs -f airflow-scheduler

# View Worker logs
docker-compose logs -f airflow-worker
```

## 🗄️ Verify Data

After the DAG completes, check if data was successfully stored:

```bash
# Enter PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# View all tables
\dt

# Check number of news articles
SELECT COUNT(*) FROM newsarticles;

# View first 5 news articles
SELECT id, title, published FROM newsarticles LIMIT 5;

# View news sources statistics
SELECT source, COUNT(*) as count FROM newssource GROUP BY source ORDER BY count DESC;

# View news categories statistics
SELECT category, COUNT(*) as count FROM newscategory GROUP BY category ORDER BY count DESC;

# Exit PostgreSQL
\q
```

## 📝 DAG Explanation

### DAG: news_pipeline

**Schedule**: Daily at 00:00 UTC  
**Task Flow**: `fetch_news` → `load_postgres`

#### Task 1: fetch_news
- Fetches news from Currents API for specified date range
- Saves data as CSV file to `/opt/airflow/data/`

#### Task 2: load_postgres
- Reads CSV file
- Creates or updates database tables
- Loads data into PostgreSQL

**Configurable Parameters** (edit in `airflow/dags/news_pipeline_dag.py`):
```python
start="2024-01-01"      # Start date
end="2024-01-10"        # End date
keyword="technology"    # Search keyword
```

## 🔧 Common Commands

### Container Management

```bash
# View all containers
docker-compose ps

# View detailed logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# Restart services
docker-compose restart

# Stop services (preserve data)
docker-compose stop

# Stop and remove containers (preserve data)
docker-compose down

# Complete cleanup (WARNING: deletes database data!)
docker-compose down -v
```

### Airflow Commands

```bash
# List all DAGs
docker-compose exec -T airflow-webserver airflow dags list

# List tasks in a DAG
docker-compose exec -T airflow-webserver airflow tasks list news_pipeline

# Get DAG information
docker-compose exec -T airflow-webserver airflow dags info news_pipeline

# Enter Airflow container bash
docker-compose exec airflow-webserver bash

# Test a specific task
docker-compose exec -T airflow-webserver airflow tasks test news_pipeline fetch_news 2024-01-01
```

### PostgreSQL Operations

```bash
# Enter PostgreSQL interactive client
docker-compose exec postgres psql -U airflow -d airflow

# Execute SQL query without interactive mode
docker-compose exec -T postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM newsarticles;"

# Export data as CSV
docker-compose exec -T postgres psql -U airflow -d airflow -c \
  "COPY (SELECT * FROM newsarticles) TO STDOUT WITH CSV HEADER;" > export.csv
```

## 🐛 Troubleshooting

### Issue 1: Cannot Access Airflow UI

```bash
# Check if webserver container is running
docker-compose ps | grep webserver

# View webserver logs
docker-compose logs airflow-webserver | tail -50

# Restart webserver
docker-compose restart airflow-webserver

# Wait 15 seconds and retry
sleep 15
```

### Issue 2: DAG Import Error

```bash
# Ensure src/__init__.py exists
touch src/__init__.py

# Restart scheduler
docker-compose restart airflow-scheduler

# Wait 30 seconds
sleep 30
```

### Issue 3: Task Execution Failure

```bash
# View detailed error logs
docker-compose logs airflow-worker | tail -100

# Or check logs in Airflow UI by clicking on the task
```

### Issue 4: PostgreSQL Connection Error

```bash
# Check postgres container status
docker-compose exec postgres pg_isready

# View postgres logs
docker-compose logs postgres

# Restart postgres
docker-compose restart postgres

# Wait 10 seconds
sleep 10
```

### Issue 5: API Key Error

```bash
# Verify environment variables are passed correctly
docker-compose exec -T airflow-webserver printenv | grep CURRENTS_API_KEY

# Should show: CURRENTS_API_KEY=your_actual_api_key

# If empty, check .env file
cat .env | grep CURRENTS_API_KEY

# Update .env and restart containers
docker-compose restart
```

## 📊 Database Schema

### newsarticles Table
Stores main news article information

```sql
CREATE TABLE newsarticles (
    id TEXT PRIMARY KEY,           -- Article ID
    title TEXT,                    -- Title
    description TEXT,              -- Description
    author TEXT,                   -- Author
    image TEXT,                    -- Image URL
    language TEXT,                 -- Language
    published DATE                 -- Publication date
);
```

### newscategory Table
Article categories (many-to-many relationship)

```sql
CREATE TABLE newscategory (
    id SERIAL PRIMARY KEY,
    news_id TEXT REFERENCES newsarticles(id),
    category TEXT                  -- Category tag
);
```

### newssource Table
News sources (many-to-many relationship)

```sql
CREATE TABLE newssource (
    id SERIAL PRIMARY KEY,
    news_id TEXT REFERENCES newsarticles(id),
    source TEXT                    -- Source website domain
);
```

## 🔄 Modifying the DAG

To modify DAG parameters or logic:

1. **Edit file**: `airflow/dags/news_pipeline_dag.py`
2. **Save file**: Scheduler automatically detects changes within 30 seconds
3. **No restart needed**: DAG automatically reloads

When modifying source code in `src/` directory:

1. **Edit file**: Modify `src/news_api_utils.py` or `src/db_to_postgres.py`
2. **Save file**: Changes loaded automatically in container
3. **Re-run DAG**: Next execution uses new code

## 📈 Extension Ideas

### Add More Data Sources
Create new API call functions in `src/` and add new tasks to DAG

### Add Data Validation
Add data quality checks before loading to PostgreSQL

### Add Notifications
Configure Airflow email alerts for task failures

### Add Visualization
Integrate Superset or Grafana for data visualization

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [Currents API Documentation](https://currentsapi.services/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 📝 License

MIT License

## 👨‍💻 Contributing

Issues and pull requests are welcome!

## ⚠️ Important Notes

- **Production**: Change PostgreSQL password to a strong one
- **Data Backup**: Regularly backup PostgreSQL data
- **API Quota**: Be aware of Currents API free tier limits
- **Log Management**: Periodically clean Airflow logs to save disk space

## 🆘 Need Help?

If you encounter issues:

1. Check task logs in Airflow UI
2. Review `docker-compose logs` output
3. Verify `.env` file configuration
4. Ensure all containers are running properly
5. Refer to the troubleshooting section above
