"""DAG that always succeeds after a short delay."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_success",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["interlock-demo"],
) as dag:
    BashOperator(
        task_id="succeed",
        bash_command="echo 'Running demo_success…' && sleep 10 && echo 'Done!'",
    )
