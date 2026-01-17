FROM apache/airflow:3.1.6

USER root

# System dependencies (only when necessary)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Python dependencies (pinned versions)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
