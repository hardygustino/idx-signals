from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="idx_prices_hourly",
    start_date=days_ago(1),
    schedule="5 * * * *",  # tiap jam menit ke-5
    catchup=False,
    tags=["idx","prices"],
):
    fetch_prices = BashOperator(
        task_id="fetch_prices",
        bash_command="python /opt/airflow/etl/fetch_prices.py"
    )
