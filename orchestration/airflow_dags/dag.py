from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import pendulum
from datetime import timedelta

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email': ['dhirajdhande112@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

CLUSTER_NAME = 'project-cluster'
REGION = 'us-central1'
PROJECT_ID = 'project-29571d0a-16d0-4c51-be6'
PYSPARK_URI1 = 'gs://gcs-bucket-for-practice/scripts/pyspark/login.py'

PYSPARK_JOB1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI1
    }
}

# DAG definition
with models.DAG(
    dag_id='dataproc_workflow_dag_terradata',
    default_args=default_args,
    description='Dataproc job orchestration DAG',
    start_date = pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    schedule='*/30 * * * *',   # every hour at minute 50
    catchup=False,
    max_active_runs=1,
    tags=['dataproc', 'etl']
) as dag:

    load_data = DataprocSubmitJobOperator(
        task_id="load_teradata_task",
        job=PYSPARK_JOB1,
        region=REGION,
        project_id=PROJECT_ID
    )

    load_data

#gsutil cp orchestration\airflow_dags\dag.py gs://us-central1-managed-airflow-53105cea-bucket/dags                                                        