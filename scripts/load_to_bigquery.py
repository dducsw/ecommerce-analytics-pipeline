"""
Script to load data from Postgres to BigQuery
Usage: python load_to_bigquery.py --table users
"""

import argparse
import os
import pandas as pd
import psycopg2
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5433,  # Sửa port để khớp với docker-compose
    "database": "thelook_ecommerce",  # Đúng tên database chứa dữ liệu
    "user": "airflow",
    "password": "airflow",
}

# BigQuery configuration
GCP_KEY_PATH = "credentials/gcp-key.json"
BQ_PROJECT_ID = "thelook-dwh"  # Replace with your GCP project ID
BQ_DATASET_ID = "raw"  # Raw layer: thelook-dwh.raw.<table>

# Table mappings from Postgres to BigQuery
TABLE_MAPPINGS = {
    "users": {
        "postgres_schema": "public",
        "postgres_table": "users",
        "bq_table": "users",
    },
    "orders": {
        "postgres_schema": "public",
        "postgres_table": "orders",
        "bq_table": "orders",
    },
    "order_items": {
        "postgres_schema": "public",
        "postgres_table": "order_items",
        "bq_table": "order_items",
    },
    "products": {
        "postgres_schema": "public",
        "postgres_table": "products",
        "bq_table": "products",
    },
    "events": {
        "postgres_schema": "public",
        "postgres_table": "events",
        "bq_table": "events",
    },
    "inventory_items": {
        "postgres_schema": "public",
        "postgres_table": "inventory_items",
        "bq_table": "inventory_items",
    },
    "distribution_centers": {
        "postgres_schema": "public",
        "postgres_table": "distribution_centers",
        "bq_table": "distribution_centers",
    },
}


def get_postgres_connection():
    """Create and return a Postgres connection"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print(f"Connected to Postgres database: {POSTGRES_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"Error connecting to Postgres: {e}")
        raise


def get_bigquery_client():
    """Create and return a BigQuery client"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCP_KEY_PATH
        )
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        print(f"Connected to BigQuery project: {client.project}")
        return client
    except Exception as e:
        print(f"Error connecting to BigQuery: {e}")
        raise


def read_table_from_postgres(table_name):
    """Read table data from Postgres and return as DataFrame"""
    if table_name not in TABLE_MAPPINGS:
        raise ValueError(
            f"Table '{table_name}' not found in mappings. Available tables: {list(TABLE_MAPPINGS.keys())}"
        )

    mapping = TABLE_MAPPINGS[table_name]
    postgres_schema = mapping["postgres_schema"]
    postgres_table = mapping["postgres_table"]

    conn = get_postgres_connection()

    try:
        # Read data from Postgres
        query = f"SELECT * FROM {postgres_schema}.{postgres_table}"
        print(f"Reading data from {postgres_schema}.{postgres_table}...")

        df = pd.read_sql_query(query, conn)
        print(f"Read {len(df)} rows from Postgres")

        return df
    except Exception as e:
        print(f"Error reading from Postgres: {e}")
        raise
    finally:
        conn.close()


def load_to_bigquery(df, table_name, write_mode="WRITE_TRUNCATE"):
    """Load DataFrame to BigQuery

    Args:
        df: pandas DataFrame to load
        table_name: name of the table (key in TABLE_MAPPINGS)
        write_mode: 'WRITE_TRUNCATE' (replace), 'WRITE_APPEND' (append), 'WRITE_EMPTY' (fail if exists)
    """
    if table_name not in TABLE_MAPPINGS:
        raise ValueError(f"Table '{table_name}' not found in mappings")

    mapping = TABLE_MAPPINGS[table_name]
    bq_table = mapping["bq_table"]

    client = get_bigquery_client()

    # Full table ID: project.dataset.table
    table_id = f"{client.project}.{BQ_DATASET_ID}.{bq_table}"

    print(f"Loading data to BigQuery table: {table_id}...")

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,  # Auto-detect schema from DataFrame
    )

    try:
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        # Get table info
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows to {table_id}")
        print(f"  Schema: {len(table.schema)} columns")

    except Exception as e:
        print(f"Error loading to BigQuery: {e}")
        raise


def create_bigquery_dataset_if_not_exists():
    """Create BigQuery dataset if it doesn't exist"""
    client = get_bigquery_client()
    dataset_id = f"{client.project}.{BQ_DATASET_ID}"

    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except:
        print(f"Creating dataset {dataset_id}...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Change to your preferred location
        client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")


def main():
    parser = argparse.ArgumentParser(description="Load data from Postgres to BigQuery")
    parser.add_argument(
        "--table",
        required=True,
        help=f"Table name to load. Options: {list(TABLE_MAPPINGS.keys())}",
    )
    parser.add_argument(
        "--mode",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
        help="Write mode: WRITE_TRUNCATE (replace), WRITE_APPEND (append), WRITE_EMPTY (fail if exists)",
    )
    parser.add_argument(
        "--create-dataset",
        action="store_true",
        help="Create BigQuery dataset if not exists",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("  Postgres → BigQuery Data Loader")
    print("=" * 60)

    # Create dataset if requested
    if args.create_dataset:
        create_bigquery_dataset_if_not_exists()

    # Read from Postgres
    df = read_table_from_postgres(args.table)

    # Display sample data
    print("\nSample data (first 5 rows):")
    print(df.head())
    print(f"\nColumns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")

    # Load to BigQuery
    load_to_bigquery(df, args.table, write_mode=args.mode)

    print("\n" + "=" * 60)
    print("  Load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
