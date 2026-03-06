"""
CLI wrapper: load data from Postgres to BigQuery.
Logic has been refactored into utils/ for reuse in Airflow DAGs.

Usage:
    python load_to_bigquery.py --table users
    python load_to_bigquery.py --table orders --mode WRITE_APPEND
    python load_to_bigquery.py --table all --create-dataset
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from utils.table_config import TABLE_MAPPINGS
from utils.postgres_client import read_table_full
from utils.bigquery_client import (
    get_bigquery_client,
    ensure_dataset_exists,
    load_dataframe_to_bq,
)


def main():
    parser = argparse.ArgumentParser(description="Load data from Postgres to BigQuery")
    parser.add_argument(
        "--table",
        required=True,
        help=f"Table name to load, or 'all'. Options: {list(TABLE_MAPPINGS.keys())}",
    )
    parser.add_argument(
        "--mode",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
        help="Write mode (default: WRITE_TRUNCATE)",
    )
    parser.add_argument(
        "--create-dataset",
        action="store_true",
        help="Create BigQuery dataset if not exists",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Postgres → BigQuery Data Loader (CLI)")
    print("=" * 60)

    client = get_bigquery_client()

    if args.create_dataset:
        ensure_dataset_exists(client)

    tables = list(TABLE_MAPPINGS.keys()) if args.table == "all" else [args.table]

    for table in tables:
        print(f"\n>>> Loading table: {table}")
        df = read_table_full(table)
        print(df.head())
        load_dataframe_to_bq(df, table, write_mode=args.mode, client=client)

    print("\n" + "=" * 60)
    print("  Load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
