
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime

# DAG definition
with DAG(
    dag_id="create_and_upload_to_s3",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:

    # Step 1: Create a sample file
    create_file = BashOperator(
        task_id="create_file",
        bash_command="echo 'Hello from Airflow!' > /tmp/hello.txt"
    )

    # Step 2: Upload the file to S3/MinIO
    upload_file = LocalToS3Operator(
        task_id="upload_file",
        filename="/tmp/hello.txt",  # Local file path
        key="uploads/hello.txt",    # Path inside the bucket
        bucket_name="datascience-rqwrwzb9",  # Your bucket name
        aws_conn_id="minio_conn"    # Airflow connection for MinIO
    )

    # Task dependency
    create_file >> upload_file
