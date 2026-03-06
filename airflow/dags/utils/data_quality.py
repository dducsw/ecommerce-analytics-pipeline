"""
Data quality checks for dbt pipeline
"""

import logging
from .postgres_client import get_postgres_connection

logger = logging.getLogger(__name__)


def check_table_row_count(schema, table, min_rows=1):
    """Check if table has enough rows"""
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        count = cursor.fetchone()[0]

        if count < min_rows:
            raise ValueError(
                f"{schema}.{table} has only {count} rows (minimum: {min_rows})"
            )

        logger.info(f"✓ {schema}.{table}: {count:,} rows")
        return count
    finally:
        conn.close()


def check_marts_tables():
    """Check all marts tables have data"""
    marts_tables = [
        "dim_users",
        "dim_products",
        "dim_dates",
        "fct_orders",
        "fct_order_items",
    ]

    results = {}
    for table in marts_tables:
        try:
            count = check_table_row_count("public_marts", table)
            results[table] = count
        except Exception as e:
            logger.error(f"✗ {table} failed: {e}")
            raise

    return results


def check_null_values(schema, table, columns):
    """Check if critical columns have null values"""
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        for col in columns:
            cursor.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{col}" IS NULL'
            )
            null_count = cursor.fetchone()[0]

            if null_count > 0:
                logger.warning(f"⚠ {schema}.{table}.{col}: {null_count} null values")
            else:
                logger.info(f"✓ {schema}.{table}.{col}: no nulls")
    finally:
        conn.close()
