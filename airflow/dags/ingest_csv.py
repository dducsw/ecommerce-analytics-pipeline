#!/usr/bin/env python3
"""
CSV Data Ingestion Script for TheLook E-Commerce Database

This script loads all CSV files from the data directory into PostgreSQL.
Run this BEFORE using the data generator for initial full load.

Usage:
    # Local environment
    python ingest_csv.py --host localhost --port 5432 --user airflow --password airflow --db-name airflow --schema public

    # Docker environment  
    docker compose exec dbt python /app/ingest_csv.py --host postgres --user airflow --password airflow --db-name airflow --schema public
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List
import pandas as pd
from sqlalchemy import (
    create_engine,
    text,
    MetaData,
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    DateTime,
    TIMESTAMP,
)
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def ensure_database_exists(
    host: str, port: int, user: str, password: str, db_name: str
):
    """Ensure the target database exists, create if not"""
    # Connect to default 'postgres' database to check/create target database
    default_conn_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    )
    default_engine = create_engine(
        default_conn_string, isolation_level="AUTOCOMMIT", echo=False
    )

    try:
        with default_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": db_name},
            )
            exists = result.scalar() is not None

            if exists:
                logger.info(f"✓ Database '{db_name}' already exists")
            else:
                logger.info(f"Creating database '{db_name}'...")
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                logger.info(f"✓ Database '{db_name}' created successfully")
    finally:
        default_engine.dispose()


def get_db_engine(
    host: str, port: int, user: str, password: str, db_name: str
) -> Engine:
    """Create SQLAlchemy engine for PostgreSQL connection"""
    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    )
    logger.info(f"Connecting to database: {host}:{port}/{db_name}")
    return create_engine(connection_string, echo=False)


def create_tables(engine: Engine, schema: str):
    """Create all required tables in the database"""
    logger.info(f"Creating tables in schema: {schema}")

    with engine.connect() as conn:
        # Create schema if not exists
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()

        # Distribution Centers
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.distribution_centers (
                id BIGINT PRIMARY KEY,
                name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.distribution_centers")

        # Products
        # updated_at is used by dbt SCD2 snapshot (snap_products.sql)
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.products (
                id BIGINT PRIMARY KEY,
                cost DOUBLE PRECISION,
                category TEXT,
                name TEXT,
                brand TEXT,
                retail_price DOUBLE PRECISION,
                department TEXT,
                sku TEXT,
                distribution_center_id BIGINT,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.products")

        # Users
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.users (
                id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                age INT,
                gender TEXT,
                state TEXT,
                street_address TEXT,
                postal_code TEXT,
                city TEXT,
                country TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                traffic_source TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.users")

        # Orders
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.orders (
                order_id BIGINT PRIMARY KEY,
                user_id BIGINT,
                status TEXT,
                gender TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                returned_at TIMESTAMP WITHOUT TIME ZONE,
                shipped_at TIMESTAMP WITHOUT TIME ZONE,
                delivered_at TIMESTAMP WITHOUT TIME ZONE,
                num_of_item INT
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.orders")

        # Inventory Items
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.inventory_items (
                id BIGINT PRIMARY KEY,
                product_id BIGINT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                sold_at TIMESTAMP WITHOUT TIME ZONE,
                cost DOUBLE PRECISION,
                product_category TEXT,
                product_name TEXT,
                product_brand TEXT,
                product_retail_price DOUBLE PRECISION,
                product_department TEXT,
                product_sku TEXT,
                product_distribution_center_id BIGINT
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.inventory_items")

        # Order Items
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.order_items (
                id BIGINT PRIMARY KEY,
                order_id BIGINT,
                user_id BIGINT,
                product_id BIGINT,
                inventory_item_id BIGINT,
                status TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                shipped_at TIMESTAMP WITHOUT TIME ZONE,
                delivered_at TIMESTAMP WITHOUT TIME ZONE,
                returned_at TIMESTAMP WITHOUT TIME ZONE,
                sale_price DOUBLE PRECISION
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.order_items")

        # Events
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {schema}.events (
                id BIGINT PRIMARY KEY,
                user_id BIGINT,
                sequence_number INT,
                session_id TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                ip_address TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                browser TEXT,
                traffic_source TEXT,
                uri TEXT,
                event_type TEXT
            )
        """
            )
        )
        logger.info(f"✓ Created table: {schema}.events")

        conn.commit()
        logger.info("All tables created successfully")


def load_csv_to_table(
    engine: Engine,
    csv_path: Path,
    table_name: str,
    schema: str,
    chunksize: int = 10000,
    column_mapping: Dict[str, str] = None,
):
    """Load CSV file into PostgreSQL table with chunking for large files"""
    logger.info(f"Loading {csv_path.name} -> {schema}.{table_name}")

    try:
        # Read entire CSV into memory first (more stable than streaming)
        logger.info(f"  Reading CSV file into memory...")
        df = pd.read_csv(csv_path)
        total_rows = len(df)
        logger.info(f"  Total rows to load: {total_rows:,}")

        # Apply column mapping if provided
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # Replace empty strings with None for proper NULL handling
        df = df.replace({"": None})

        # Auto-populate updated_at if the column is not in the CSV
        # products uses NOW() as default since it has no created_at column
        if "updated_at" not in df.columns:
            if table_name == "products":
                from datetime import datetime

                df["updated_at"] = datetime.now()
            elif "created_at" in df.columns and table_name in [
                "users",
                "orders",
                "order_items",
            ]:
                df["updated_at"] = df["created_at"]

        # Load data in chunks (same method as load_remaining_tables.py)
        progress_report_interval = 100000 if table_name == "events" else 50000

        for i in range(0, total_rows, chunksize):
            chunk = df.iloc[i : i + chunksize]

            # Load chunk into database (NO method="multi" to avoid PostgreSQL issues)
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
            )

            loaded = min(i + chunksize, total_rows)

            # Report progress at intervals or when complete
            if i % progress_report_interval == 0 or loaded == total_rows:
                percentage = loaded * 100 // total_rows
                logger.info(
                    f"  Progress: {loaded:,}/{total_rows:,} rows ({percentage}%)"
                )

        logger.info(f"✓ Loaded {total_rows:,} rows from {csv_path.name}")

        return total_rows

    except Exception as e:
        logger.error(f"✗ Failed to load {csv_path.name}: {str(e)}")
        raise


def verify_data(engine: Engine, schema: str):
    """Verify loaded data by counting rows in each table"""
    logger.info("\n" + "=" * 60)
    logger.info("DATA VERIFICATION - Row Counts")
    logger.info("=" * 60)

    tables = [
        "distribution_centers",
        "products",
        "users",
        "orders",
        "inventory_items",
        "order_items",
        "events",
    ]

    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            count = result.scalar()
            logger.info(f"{schema}.{table:25s} : {count:>10,} rows")

    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest TheLook E-Commerce CSV data into PostgreSQL"
    )
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--user", default="airflow", help="PostgreSQL user")
    parser.add_argument("--password", default="airflow", help="PostgreSQL password")
    parser.add_argument("--db-name", default="airflow", help="Database name")
    parser.add_argument("--schema", default="public", help="Schema name")
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Directory containing CSV files",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=10000,
        help="Number of rows per chunk for large files",
    )

    args = parser.parse_args()

    # Setup
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("THELOOK E-COMMERCE CSV INGESTION")
    logger.info("=" * 60)
    logger.info(f"Data directory: {data_dir.absolute()}")
    logger.info(f"Target schema: {args.schema}")
    logger.info(f"Chunk size: {args.chunksize:,} rows")
    logger.info("=" * 60 + "\n")

    try:
        # Ensure database exists
        ensure_database_exists(
            args.host, args.port, args.user, args.password, args.db_name
        )

        # Connect to database
        engine = get_db_engine(
            args.host, args.port, args.user, args.password, args.db_name
        )

        # Create tables
        create_tables(engine, args.schema)
        logger.info("")

        # Load data in dependency order
        logger.info("Starting data ingestion...")
        logger.info("-" * 60)

        tables_to_load = [
            ("distribution_centers", "thelook_ecommerce.distribution_centers.csv"),
            ("products", "thelook_ecommerce.products.csv"),
            ("users", "thelook_ecommerce.users.csv"),
            ("orders", "thelook_ecommerce.orders.csv"),
            ("inventory_items", "thelook_ecommerce.inventory_items.csv"),
            ("order_items", "thelook_ecommerce.order_items.csv"),
            ("events", "thelook_ecommerce.events.csv"),
        ]

        successful_loads = []
        failed_loads = []

        for table_name, csv_file in tables_to_load:
            try:
                csv_path = data_dir / csv_file
                if not csv_path.exists():
                    logger.warning(f"⚠ CSV file not found: {csv_file}, skipping...")
                    continue

                rows = load_csv_to_table(
                    engine,
                    csv_path,
                    table_name,
                    args.schema,
                    args.chunksize,
                )
                successful_loads.append((table_name, rows))

            except Exception as e:
                logger.error(f"✗ Failed to load {table_name}: {str(e)}")
                failed_loads.append((table_name, str(e)))
                # Continue with next table instead of failing completely
                continue

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        logger.info(
            f"Successfully loaded: {len(successful_loads)}/{len(tables_to_load)} tables"
        )
        if failed_loads:
            logger.warning(f"Failed tables: {len(failed_loads)}")
            for table, error in failed_loads:
                logger.warning(f"  - {table}: {error}")
        logger.info("=" * 60)

        # Verify loaded data
        verify_data(engine, args.schema)

        if failed_loads:
            logger.warning("\n⚠ CSV ingestion completed with errors")
            logger.warning("Some tables failed to load. Check logs above for details.")
        else:
            logger.info("\n✓ CSV ingestion completed successfully!")

        logger.info(f"\nNext steps:")
        logger.info(
            f"1. Verify data: psql -U {args.user} -d {args.db_name} -c 'SELECT COUNT(*) FROM {args.schema}.products;'"
        )
        logger.info(f"2. Run data generator to simulate real-time events")
        logger.info(f"3. Create DBT models for data transformation")

        # Exit with error code if any tables failed
        if failed_loads:
            sys.exit(1)

    except Exception as e:
        logger.error(f"\n✗ Ingestion failed: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
