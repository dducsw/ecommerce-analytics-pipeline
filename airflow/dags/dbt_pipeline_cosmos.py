import os
from pathlib import Path
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

# Default to Docker path, but allow override for CI/local testing
DBT_PROJECT_PATH = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")

# Only pass manifest_path when the file actually exists.
# In CI (without GCP credentials), manifest.json is not compiled, so we
# omit it and let Cosmos parse the project from source instead of raising
# CosmosValueError at import time.
_manifest_path = Path(DBT_PROJECT_PATH) / "target" / "manifest.json"
_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    **({"manifest_path": str(_manifest_path)} if _manifest_path.exists() else {}),
)

_profile_config = ProfileConfig(
    profile_name="ecommerce_analytics",
    target_name="dev",  # This matches the BigQuery target in profiles.yml
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

_execution_config = ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")

# DAG 1: Daily light marts pipeline
#   - Runs: core, sales (light), product
#   - Excludes: tag:heavy (fact_sales, cohort_retention)
#   - Staging & intermediate are VIEWS, computed auto at query time
#   - Schedule: daily
dbt_daily_marts = DbtDag(
    dag_id="dbt_daily_marts",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    default_args={"retries": 2},
    tags=["dbt", "cosmos", "marts", "daily"],
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    render_config=RenderConfig(
        select=["tag:marts"],
        exclude=["tag:heavy"],  # Bỏ qua fact_sales, cohort_retention
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,
    },
)


# DAG 2: Weekly heavy models pipeline
#   - Runs: fact_sales, cohort_retention (tag:heavy)
#   - Incremental + window functions → chạy cuối tuần để giảm tải
#   - full_refresh=False mặc định; có thể đổi True để rebuild mỗi tháng
#   - Schedule: mỗi chủ nhật 3:00 AM (sau khi light marts xong)

dbt_weekly_heavy = DbtDag(
    dag_id="dbt_weekly_heavy",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * 0",  # Chủ nhật 03:00 AM
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,  # Giới hạn thấp hơn vì model nặng
    default_args={"retries": 1},
    tags=["dbt", "cosmos", "heavy", "weekly"],
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    render_config=RenderConfig(
        select=["tag:heavy"],  # Chỉ chạy fact_sales, cohort_retention
    ),
    operator_args={
        "install_deps": True,
        "full_refresh": False,  # Đổi True nếu muốn rebuild toàn bộ mỗi tuần
    },
)
