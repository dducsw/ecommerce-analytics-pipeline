# PostgreSQL helper functions
# Provides: get_postgres_connection, read_table_full, read_table_incremental

import logging
from datetime import datetime

import pandas as pd
import psycopg2

from .table_config import POSTGRES_CONFIG, TABLE_MAPPINGS

logger = logging.getLogger(__name__)


def get_postgres_connection():
    """Open and return a Postgres connection. Remember to close it after use."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    logger.info("Connected to Postgres database: %s", POSTGRES_CONFIG["database"])
    return conn


def read_table_full(table_name):
    """Read all rows from a Postgres table. Returns a pandas DataFrame."""
    if table_name not in TABLE_MAPPINGS:
        raise ValueError(f"Table '{table_name}' not found. Available: {list(TABLE_MAPPINGS.keys())}")

    config = TABLE_MAPPINGS[table_name]
    schema = config["postgres_schema"]
    table = config["postgres_table"]
    query = f'SELECT * FROM "{schema}"."{table}"'

    logger.info("Full load: reading all rows from %s.%s", schema, table)

    conn = get_postgres_connection()
    try:
        df = pd.read_sql_query(query, conn)
        logger.info("Read %d rows from %s.%s", len(df), schema, table)
        return df
    finally:
        conn.close()


def read_table_incremental(table_name, start_dt, end_dt):
    """
    Read rows from a Postgres table where incremental_key is between start_dt and end_dt.
    If the table has no incremental_key, fall back to reading all rows.

    Args:
        table_name : table name defined in TABLE_MAPPINGS
        start_dt   : start datetime (inclusive)
        end_dt     : end datetime (exclusive)
    """
    if table_name not in TABLE_MAPPINGS:
        raise ValueError(f"Table '{table_name}' not found. Available: {list(TABLE_MAPPINGS.keys())}")

    config = TABLE_MAPPINGS[table_name]
    incremental_key = config["incremental_key"]

    if incremental_key is None:
        logger.warning("Table '%s' has no incremental_key. Falling back to full load.", table_name)
        return read_table_full(table_name)

    schema = config["postgres_schema"]
    table = config["postgres_table"]
    query = (
        f'SELECT * FROM "{schema}"."{table}"'
        f' WHERE "{incremental_key}" >= %(start_dt)s'
        f' AND "{incremental_key}" < %(end_dt)s'
    )
    params = {"start_dt": start_dt, "end_dt": end_dt}

    logger.info(
        "Incremental load: reading %s.%s where %s in [%s, %s)",
        schema, table, incremental_key, start_dt, end_dt
    )

    conn = get_postgres_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        logger.info("Read %d rows from %s.%s", len(df), schema, table)
        return df
    finally:
        conn.close()
