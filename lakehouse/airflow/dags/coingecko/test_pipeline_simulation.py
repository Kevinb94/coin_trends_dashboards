from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def step_1_fetch():
    print("✅ Step 1: Simulate fetching data from CoinGecko")

def step_2_log_api_response():
    print("✅ Step 2: Simulate logging API response")

def step_3_transform_to_parquet():
    print("✅ Step 3: Simulate transforming data to Parquet")

def step_4_upload_to_minio():
    print("✅ Step 4: Simulate uploading to MinIO")

with DAG(
    dag_id="test_pipeline_simulation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Simulate DAG pipeline steps using print statements",
) as dag:

    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=step_1_fetch,
    )

    log_response = PythonOperator(
        task_id="log_api_response",
        python_callable=step_2_log_api_response,
    )

    transform = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=step_3_transform_to_parquet,
    )

    upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=step_4_upload_to_minio,
    )

    # Define task sequence
    fetch >> log_response >> transform >> upload
