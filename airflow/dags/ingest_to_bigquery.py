"""
Airflow DAGs: load data from Postgres to BigQuery

DAG 1 - pg_to_bq_full_load:
    Triggered manually. Truncates and reloads all tables into BigQuery.

DAG 2 - pg_to_bq_daily_incremental:
    Runs daily. Appends new rows for event-based tables.
    Static tables (products, distribution_centers) are always fully reloaded.
"""

import logging
import sys
import os
from datetime import datetime

from airflow.decorators import dag, task

sys.path.insert(0, os.path.dirname(__file__))

from utils.table_config import TABLE_MAPPINGS, ALWAYS_FULL_LOAD_TABLES
from utils.postgres_client import read_table_full, read_table_incremental
from utils.bigquery_client import get_bigquery_client, ensure_dataset_exists, load_dataframe_to_bq

logger = logging.getLogger(__name__)


# DAG 1: Full load (manual trigger only)
@dag(
    dag_id="pg_to_bq_full_load",
    description="Full load: truncate and reload all Postgres tables into BigQuery.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "postgres", "bigquery", "full-load"],
)
def full_load_dag():

    @task()
    def create_dataset():
        """Create BigQuery dataset if it does not exist yet."""
        client = get_bigquery_client()
        ensure_dataset_exists(client)
        return "ok"

    @task()
    def full_load_table(table_name, _dep=None):
        """Read all rows from Postgres and overwrite the BigQuery table."""
        logger.info("Full load started for table: %s", table_name)
        client = get_bigquery_client()
        df = read_table_full(table_name)
        load_dataframe_to_bq(df, table_name, write_mode="WRITE_TRUNCATE", client=client)
        logger.info("Full load done for table: %s", table_name)

    dataset_ready = create_dataset()
    for table in TABLE_MAPPINGS:
        full_load_table.override(task_id=f"full_load__{table}")(table, _dep=dataset_ready)


full_load_dag()


# DAG 2: Daily incremental load
@dag(
    dag_id="pg_to_bq_daily_incremental",
    description="Daily incremental load: append new rows for event tables, full reload for static tables.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["ingestion", "postgres", "bigquery", "incremental", "daily"],
)
def incremental_dag():

    @task()
    def create_dataset():
        """Create BigQuery dataset if it does not exist yet."""
        client = get_bigquery_client()
        ensure_dataset_exists(client)
        return "ok"

    @task()
    def incremental_load_table(table_name, data_interval_start, data_interval_end, _dep=None):
        """
        Load new rows for one table.
        - Event tables (has incremental_key): append rows created within the interval.
        - Static tables (no incremental_key): fully reload the table.

        Args:
            data_interval_start : start of the DAG run interval, e.g. '2026-03-04T00:00:00+00:00'
            data_interval_end   : end of the DAG run interval,   e.g. '2026-03-05T00:00:00+00:00'
        """
        from datetime import datetime, timezone

        # Parse ISO format datetime strings from Airflow macros
        start_dt = datetime.fromisoformat(data_interval_start).replace(tzinfo=None)
        end_dt = datetime.fromisoformat(data_interval_end).replace(tzinfo=None)

        client = get_bigquery_client()

        if table_name in ALWAYS_FULL_LOAD_TABLES:
            logger.info("Static table '%s': full reload.", table_name)
            df = read_table_full(table_name)
            write_mode = "WRITE_TRUNCATE"
        else:
            logger.info(
                "Event table '%s': loading rows from %s to %s.",
                table_name, start_dt, end_dt
            )
            df = read_table_incremental(table_name, start_dt=start_dt, end_dt=end_dt)
            write_mode = "WRITE_APPEND"

        load_dataframe_to_bq(df, table_name, write_mode=write_mode, client=client)

    dataset_ready = create_dataset()
    for table in TABLE_MAPPINGS:
        # data_interval_start and data_interval_end are Airflow macros
        # For a @daily DAG: start = 2026-03-04T00:00:00+00:00, end = 2026-03-05T00:00:00+00:00
        incremental_load_table.override(task_id=f"incremental__{table}")(
            table_name=table,
            data_interval_start="{{ data_interval_start | ts }}",
            data_interval_end="{{ data_interval_end | ts }}",
            _dep=dataset_ready,
        )


incremental_dag()
