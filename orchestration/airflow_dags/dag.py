import airflow
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import datetime
import pendulum

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 4, 18, tz="Asia/Kolkata")
}

CLUSTER_NAME = 'teradata-prod'
REGION = 'asia-south1'
PROJECT_ID = 'abcd-dataplatform-prod'
PYSPARK_URI1 = 'gs://abcd-teradata-prod/LOGIN_TABLE.py'

PYSPARK_JOB1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI1,
        "jar_file_uris": ["gs://abcd-teradata-prod/teradata_jdbc_driver.jar"]
    }
}

with models.DAG(
    'LOGIN_TABLE',
    default_args=default_args,
    description='A simple DAG to create a Dataproc workflow',
    schedule_interval='00 07 * * *',
    # start_date=datetime.datetime.now()
) as dag:

    load_data = DataprocSubmitJobOperator(
        task_id="load_teradata_task",
        job=PYSPARK_JOB1,
        region=REGION,
        project_id=PROJECT_ID
    )

    load_data