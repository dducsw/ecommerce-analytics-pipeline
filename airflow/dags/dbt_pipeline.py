from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dbt_project_dir = '/opt/airflow/dbt'  # Sửa lại path này cho đúng với môi trường của bạn

dag = DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='Run dbt models as part of Airflow pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {dbt_project_dir} && dbt run',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {dbt_project_dir} && dbt test',
    dag=dag
)

dbt_run >> dbt_test
