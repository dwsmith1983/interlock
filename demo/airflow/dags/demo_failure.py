"""DAG that always fails after a short delay."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_failure",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["interlock-demo"],
) as dag:
    BashOperator(
        task_id="fail",
        bash_command="echo 'Running demo_failure…' && sleep 5 && echo 'About to fail!' && exit 1",
    )
