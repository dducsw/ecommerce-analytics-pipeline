# E-Commerce Analytics Data Pipeline

## 🚀 Overview
The **E-Commerce Analytics Pipeline** is an automated data engineering pipeline designed to ingest, transform, and analyze e-commerce data. 

The system processes raw data, performs complex transformations, and prepares optimized data models for Business Intelligence (BI) dashboards to track critical metrics such as monthly sales growth and top-performing brands.

## 🏗️ Architecture & Technologies
This project leverages modern Data Engineering tools and best practices:
*   **Data Orchestration:** [Apache Airflow](https://airflow.apache.org/) (Running on CeleryExecutor with Redis and PostgreSQL)
*   **Data Transformation:** [dbt (Data Build Tool)](https://www.getdbt.com/) (Integrated with Airflow using the `astronomer-cosmos` provider)
*   **Data Warehouse:** Google BigQuery
*   **Containerization:** Docker & Docker Compose
*   **CI/CD:** GitHub Actions (Automated testing and continuous integration)
*   **BI & Dashboards:** Looker / Looker Studio (Configurations located in the `dashboards` directory)

## 📁 Repository Structure

```text
ecommerce-analytics-pipeline/
├── airflow/           # Airflow DAGs (e.g., dbt_pipeline_cosmos.py), configurations, and Dockerfile
├── dbt/               # dbt project containing models, macros, and analyses (e.g., monthly_sales_growth.sql)
├── datagen/           # Scripts for generating and simulating input e-commerce data
├── data/              # Local storage for mock CSV data files
├── dashboards/        # Looker/BI dashboard documentation and related configurations
├── credentials/       # Storage for Google Cloud credential files (e.g., gcp-key.json) 
├── tests/             # Unit tests for the pipeline components
├── .github/           # GitHub Actions CI/CD workflows
├── docker-compose.yml # Docker Compose configuration for local development
├── QUICKSTART.md      # Detailed instructions for running, troubleshooting, and debugging services locally
└── README.md          # Project documentation (This file)
```

## ⚙️ Prerequisites
To run this project on your local machine, ensure you have the following installed:
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
- At least 4GB of RAM allocated to Docker (8GB+ recommended)
- A Google Cloud Service Account Key with BigQuery Admin and Storage Admin roles. This file must be placed at `credentials/gcp-key.json`.

## 🏃‍♂️ Quick Start Guide

**Step 1. Prepare Credentials**
Ensure your Google Cloud Service Account JSON key is securely placed at `credentials/gcp-key.json`.

**Step 2. Initialize Airflow Environment**
Build the Docker images and initialize the Airflow database:
```bash
docker-compose build
docker-compose up airflow-init
```

**Step 3. Start the Pipeline Services**
Bring up all services (Airflow Webserver, Scheduler, Workers, Postgres, Redis, and dbt container):
```bash
docker-compose up -d
```

**Step 4. Access the UI**
- **Airflow Web UI:** Navigate to [http://localhost:8080](http://localhost:8080)
  - **Default Username:** `airflow`
  - **Default Password:** `airflow`
- Enable your specific DAGs in the Airflow UI to trigger the dbt models and start transforming your BigQuery data.

> 📘 **Note:** Please refer to the [QUICKSTART.md](QUICKSTART.md) file for more detailed CLI commands, such as running dbt tests, viewing container logs, and resetting the environment.

## 📊 Analytics Highlights
The project includes several analytical queries and metrics prepared for presentation, such as:
- **Monthly Revenue & Growth:** Tracking revenue trends over time (`monthly_sales_growth.sql`).
- **Brand Performance:** Identifying the highest converting and top-selling brands (`top_performing_brands.sql`).
- **Daily KPIs:** Fact tables summarizing essential daily metrics (`fct_daily_kpis.sql`).

These dbt outputs seamlessly feed into BI tools to provide actionable business insights.
