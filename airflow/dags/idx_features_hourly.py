from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="idx_features_hourly",
    start_date=days_ago(1),
    schedule="10 * * * *",  # tiap jam menit ke-10
    catchup=False,
    tags=["idx","features"],
):
    build_features = BashOperator(
        task_id="build_features",
        bash_command="python /opt/airflow/etl/build_features.py"
    )
