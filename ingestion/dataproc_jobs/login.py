# from distutils.errors import DistutilsFileError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from google.cloud import secretmanager
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
import requests

def creat_spark_session():
    return SparkSession.builder \
            .appName("CloudSQL-MySQL-Read") \
            .config("spark.jars.packages","mysql:mysql-connector-java:8.0.33") \
            .getOrCreate()
    #.config("spark.jars", f"gs://{gcs_bucket}/mysql-connector-java-8.0.33.jar") \

class helperClass:

    def fetch_secret_value(project_id: str, secret_id: str, version_number: str):
        try:
            client = secretmanager.SecretManagerServiceClient()
            parent = f"projects/{project_id}/"
            request_data = {"name": parent + f"secrets/{secret_id}/versions/{version_number}"}
            print(request_data)
            response = client.access_secret_version(request=request_data)
            return response.payload.data.decode("UTF-8")

        except Exception as e:
            print(e)
            raise

def fetch_conf():
    # fetch secret values
    secret_list = ["abcd_td_prod_id", "abcd_td_prod_pass", "abcd_td_prod_srvr"]
    project_id="project-29571d0a-16d0-4c51-be6"
    secret_value_mapping = dict()
    for secret_name in secret_list:
        version = 1
        secret_value_mapping[secret_name] = helperClass.fetch_secret_value(
            project_id=project_id,
            secret_id=secret_name,
            version_number=version
        )
    config = {
        "user": secret_value_mapping["abcd_td_prod_id"],
        "password": secret_value_mapping["abcd_td_prod_pass"],
        "host_name": secret_value_mapping["abcd_td_prod_srvr"],
        "port": "3306",
        "source_db": "teradata_customer_db",
        "project_id": project_id,
        "gcs_bucket": "gcs-bucket-for-practice",
        "dataset_id": "abfssl_teradata_raw"
    }
    return config

def fetch_from_teradata(spark, user, password, host_name, source_db, source_table,port):
    sql = f"select * from {source_db}.{source_table}"
    jdbc_url = f"jdbc:mysql://{host_name}:{port}/{source_db}"

    return  spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f'({sql}) as src') \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

def load_into_bq(df_td, project_id, dataset_id, table_id, gcs_bucket):
    df_td_with_timestamps = df_td \
        .withColumn("created_at", from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
        .withColumn("modified_at", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

    # iso_format = "yyyy-MM-dd'T'HH:mm:ss"
    # df_td_with_timestamps = df_td_with_timestamps \
    #     .withColumn("created_at", date_format(col("created_at"), iso_format)) \
    #     .withColumn("modified_at", date_format(col("modified_at"), iso_format))

    df_td_with_timestamps.write.format("bigquery") \
        .option("table", f"{project_id}:{dataset_id}.{table_id}") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .mode("overwrite") \
        .save()

def create_audit_entry(source_name, bq_dataset_name, bq_table_name,
                       run_start_date, run_end_time, gcp_service_name,
                       records_inserted, records_updated, records_deleted,
                       ingestion_time, gcs_bucket, conf, spark):

    schema = StructType([
        StructField("SOURCE_NAME", StringType(), True),
        StructField("BQ_DATASET_NAME", StringType(), True),
        StructField("BQ_TABLE_NAME", StringType(), True),
        StructField("RUN_START_TIME", TimestampType(), True),
        StructField("RUN_END_TIME", TimestampType(), True),
        StructField("GCP_SERVICE_NAME", StringType(), True),
        StructField("RECORDS_INSERTED", IntegerType(), True),
        StructField("RECORDS_UPDATED", IntegerType(), True),
        StructField("RECORDS_DELETED", IntegerType(), True),
        StructField("INGESTION_TIME", TimestampType(), True)
    ])

    data = [(source_name, bq_dataset_name, bq_table_name,
             run_start_date, run_end_time, gcp_service_name,
             records_inserted, records_updated, records_deleted,
             ingestion_time)]

    df = spark.createDataFrame(data, schema)

    df.write \
        .format("bigquery") \
        .option("table", f"{conf['project_id']}:abcd_prod_data_logging_auditing.abcd_prod_data_audit") \
        .option("temporaryGcsBucket", gcs_bucket) \
        .mode("append") \
        .save()

def email_access_token():
    try:
        access_token_url = "https://abfss-prod-api.auth.ap-south-1.amazoncognito.com/oauth2/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": "5td5tck1fjderq8ga7dig1h345",
            "scope": "netcore/auth"
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "gzip,deflate,br",
            "Connection": "keep-alive",
            "Authorization": "Basic NXRkNXRjazFmamRlcnE4Z2E3ZGlnMWgzNDU6MXI4Ymc5NTFpZXU2YnV0bGhjdGNmYWxsMXJjZjBiZXEzM3NvMHZvcTh2MjFkZzdkZHVqOQ=="
        }

        response = requests.post(access_token_url, headers=headers, data=data)

        # Better than json.loads(response.text)
        response_json = response.json()

        access_token = response_json.get("access_token")

        if not access_token:
            raise ValueError(f"No access_token in response: {response_json}")

        return access_token

    except Exception as e:
        raise Exception(f"Error in email_access_token function: {str(e)}")

def push_email_notification(flag, response_status_code=0, error=""):

    external_data = {
        "from": {
            "email": "transactions.abcd@digital.abcd.org",
            "name": "Notification"
        },
        "subject": "[PROD ALERT] : Deredata Failure Notification",
        "content": [
            {
                "type": "html",
                "value": f"""Hi All, <br><br>
                This is to notify that the dataproc job responsible for transitioning data from teadata to raw layer for the teradata has failed with below error : <br><br> <b>{error}</b>
                <br><br> Thanks, <br>GCP Tech Team"""
            }
        ],
        "personalizations": [
            {
                "to": [
                    {"email": "xyz@abcd.com", "name": "xyz"},
                    {"email": "xyz1@abcd.com", "name": "abcd"}
                ]
            }
        ]
    }

    internal_data = {
        "from": {
            "email": "transactions.abcd@digital.abcd.org",
            "name": "Notification"
        },
        "subject": "[PROD ALERT] : Teradata Failure Notification",
        "content": [
            {
                "type": "html",
                "value": f"""Hi All, <br><br>
                This is to notify that the dataproc job responsible for transitioning data from teadata to raw layer for the teradata has failed with below error : <br><br> <b>{error}</b>
                <br><br> Thanks, <br>GCP Tech Team"""
            }
        ],
        "personalizations": [
            {
                "to": [
                    {"email": "xyz@abcd.com", "name": "xyz"},
                    {"email": "xyz1@abcd.com", "name": "xyz1"}
                ]
            }
        ]
    }

    try:
        push_email_access_token = email_access_token()

        email_url = 'https://api.abcd.com/netcore/email'

        headers = {
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate,br',
            'channel': '1',
            'auth-token': push_email_access_token,
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'User-Agent': 'request',
        }

        data = external_data if flag == 'external' else internal_data

        response = requests.post(email_url, headers=headers, json=data)
        print(response.text)

    except Exception as e:
        raise Exception(f"Error in push_email_notification function: {str(e)}")

def main():
    try:
        teradata_tables = ['customers', 'addresses', 'customer_preferences']
        conf = fetch_conf()
        spark = creat_spark_session()

        for source_table in teradata_tables:
            df_td = fetch_from_teradata(spark, conf['user'],conf['password'], conf['host_name'], conf['source_db'], source_table, conf['port'])

            start_time = datetime.now()
            total_records = int(df_td.count())

            load_into_bq(df_td, conf['project_id'], conf['dataset_id'], source_table, conf['gcs_bucket'])

            end_time = datetime.now()
            create_audit_entry(
                "TERADATA",
                "abfssl_teradata_raw",
                source_table,
                start_time,
                end_time,
                "DATAPROC",
                total_records,
                None,
                None,
                datetime.now(),
                conf['gcs_bucket'],
                conf,
                spark
            )

    except Exception as e:
        push_email_notification(flag='internal', error=str(e))
    spark.stop()

if __name__ == "__main__":
    main()

# gsutil cp infrastructure/dataproc/init.sh gs://gcs-bucket-for-practice/config/
# gsutil cp ingestion/dataproc_jobs/login.py gs://gcs-bucket-for-practice/scripts/pyspark/

# gcloud dataproc clusters create project-cluster `
#   --region=us-central1 `
#   --zone=us-central1-f `
#   --single-node `
#   --master-machine-type=e2-standard-2 `
#   --master-boot-disk-size=50GB `
#   --subnet=default `
#   --public-ip-address `
#   --initialization-actions=gs://gcs-bucket-for-practice/config/init.sh

# gcloud dataproc clusters list --region=us-central1 

# gcloud dataproc jobs submit pyspark `
#   gs://gcs-bucket-for-practice/scripts/pyspark/login.py `
#   --cluster=project-cluster `
#   --region=us-central1 `


#== Uplad dag file in respective bucket ===================================================================================
# gsutil cp orchestration\airflow_dags\dag.py gs://us-central1-managed-airflow-53105cea-bucket/dags                                                        
#===========================================================================================================================

#== some extra commands ===================================================================================================
# gcloud dataproc clusters create project-cluster `
#   --region=us-central1 `
#   --master-machine-type=e2-standard-2 `
#   --worker-machine-type=e2-standard-2 `
#   --num-workers=2 `
#   --master-boot-disk-size=50GB `
#   --worker-boot-disk-size=50GB `
#   --initialization-actions=gs://gcs-bucket-for-practice/config/init.sh

# gcloud dataproc clusters delete project-cluster --region=us-central1

# gcloud compute ssh project-cluster-m --zone=us-central1-f
# curl ifconfig.me

# pip3 install google-cloud-secret-manager requests

# == Zip the dependancies=================================================
# pip install google-cloud-secret-manager requests -t deps/
# cd deps
# Compress-Archive -Path * -DestinationPath ../deps.zip
# gsutil cp deps.zip gs://gcs-bucket-for-practice/config/

# gcloud dataproc jobs submit pyspark `
#   gs://gcs-bucket-for-practice/scripts/pyspark/login.py `
#   --cluster=project-cluster `
#   --region=us-central1 `
#   --py-files=gs://gcs-bucket-for-practice/config/deps.zip
#=========================================================================

# gcloud secrets add-iam-policy-binding YOUR_SECRET_NAME `
#   --member="serviceAccount:993103366834-compute@developer.gserviceaccount.com" `
#   --role="roles/secretmanager.secretAccessor"

# == get service account =========================================
# gcloud dataproc clusters describe project-cluster \
#   --region=us-central1 \
#   --format="value(config.gceClusterConfig.serviceAccount)"
# =================================================================

# gsutil cp infrastructure/dataproc/requirements.txt gs://gcs-bucket-for-practice/config/
# ===========================================================================================================================================
