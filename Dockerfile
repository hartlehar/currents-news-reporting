FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    git \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow

EXPOSE 8080