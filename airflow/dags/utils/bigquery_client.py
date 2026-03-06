# BigQuery helper functions
# Provides: get_bigquery_client, ensure_dataset_exists, load_dataframe_to_bq

import logging

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from .table_config import GCP_KEY_PATH, BQ_DATASET_ID, BQ_LOCATION, TABLE_MAPPINGS

logger = logging.getLogger(__name__)


def get_bigquery_client():
    """Create and return a BigQuery client using the service account JSON key."""
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    logger.info("Connected to BigQuery project: %s", client.project)
    return client


def ensure_dataset_exists(client=None):
    """Create the BigQuery dataset if it does not already exist."""
    if client is None:
        client = get_bigquery_client()

    dataset_id = f"{client.project}.{BQ_DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        logger.info("Dataset %s already exists.", dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset, timeout=30)
        logger.info("Dataset %s created.", dataset_id)


def load_dataframe_to_bq(df, table_name, write_mode="WRITE_APPEND", client=None):
    """
    Load a pandas DataFrame into a BigQuery table.

    Args:
        df         : pandas DataFrame to load
        table_name : key in TABLE_MAPPINGS
        write_mode : WRITE_TRUNCATE (replace all), WRITE_APPEND (add rows), WRITE_EMPTY (fail if exists)
        client     : BigQuery client (optional, creates a new one if not provided)
    """
    if table_name not in TABLE_MAPPINGS:
        raise ValueError(
            f"Table '{table_name}' not found. Available: {list(TABLE_MAPPINGS.keys())}"
        )

    if df.empty:
        logger.warning("DataFrame for '%s' is empty. Skipping load.", table_name)
        return

    if client is None:
        client = get_bigquery_client()

    bq_table_name = TABLE_MAPPINGS[table_name]["bq_table"]
    table_id = f"{client.project}.{BQ_DATASET_ID}.{bq_table_name}"

    logger.info("Loading %d rows into %s (mode: %s)", len(df), table_id, write_mode)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to finish

    table = client.get_table(table_id)
    logger.info("Done. %s now has %d rows.", table_id, table.num_rows)
    return table.num_rows
