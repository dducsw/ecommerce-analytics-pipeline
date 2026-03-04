# Postgres connection settings
# Use 'postgres' as host when running inside Docker, 'localhost' when running locally
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "thelook_ecommerce",
    "user": "airflow",
    "password": "airflow",
}

# BigQuery settings
GCP_KEY_PATH = "/opt/airflow/credentials/gcp-key.json"
BQ_PROJECT_ID = "thelook-dwh"
BQ_DATASET_ID = "raw"
BQ_LOCATION = "US"

# TABLE_MAPPINGS defines which Postgres tables to load and how
# incremental_key: the date column used to filter rows in incremental load
# if incremental_key is None, the table will always be fully reloaded
TABLE_MAPPINGS = {
    "users": {
        "postgres_schema": "public",
        "postgres_table": "users",
        "bq_table": "users",
        "incremental_key": "created_at",
    },
    "orders": {
        "postgres_schema": "public",
        "postgres_table": "orders",
        "bq_table": "orders",
        "incremental_key": "created_at",
    },
    "order_items": {
        "postgres_schema": "public",
        "postgres_table": "order_items",
        "bq_table": "order_items",
        "incremental_key": "created_at",
    },
    "events": {
        "postgres_schema": "public",
        "postgres_table": "events",
        "bq_table": "events",
        "incremental_key": "created_at",
    },
    "inventory_items": {
        "postgres_schema": "public",
        "postgres_table": "inventory_items",
        "bq_table": "inventory_items",
        "incremental_key": "created_at",
    },
    # products and distribution_centers have no reliable date column
    # so they will always be fully reloaded
    "products": {
        "postgres_schema": "public",
        "postgres_table": "products",
        "bq_table": "products",
        "incremental_key": None,
    },
    "distribution_centers": {
        "postgres_schema": "public",
        "postgres_table": "distribution_centers",
        "bq_table": "distribution_centers",
        "incremental_key": None,
    },
}

# Build a set of table names that must always be fully reloaded
ALWAYS_FULL_LOAD_TABLES = {
    name for name, config in TABLE_MAPPINGS.items() if config["incremental_key"] is None
}
