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

