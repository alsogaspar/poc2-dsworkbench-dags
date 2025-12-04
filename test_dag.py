from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
):

    task_1 = BashOperator(
        task_id="say_hello",
        bash_command="echo '🎉 My first dag is working!'"
    )
