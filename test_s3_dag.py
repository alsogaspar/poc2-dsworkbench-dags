
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def create_content(**context):
    content = "Hello from Airflow!"
    context['ti'].xcom_push(key='file_content', value=content)

def upload_to_s3(**context):
    content = context['ti'].xcom_pull(key='file_content')
    s3 = S3Hook(aws_conn_id='minio_conn')
    s3.load_string(
        string_data=content,
        key='uploads/hello.txt',
        bucket_name='datascience-rqwrwzb9',
        replace=True
    )

with DAG(
    dag_id="create_and_upload_to_s3_xcom",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:

    create_file = PythonOperator(
        task_id="create_file",
        python_callable=create_content
    )

    upload_file = PythonOperator(
        task_id="upload_file",
        python_callable=upload_to_s3
    )

    create_file >> upload_file
