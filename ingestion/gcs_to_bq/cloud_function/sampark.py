import os
from datetime import datetime
import pandas as pd
import pytz
import gcsfs
from google.cloud import storage, bigquery
from notification import push_email_notification

# -------------------------------
# Utility Functions
# -------------------------------

def get_current_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now().replace(tzinfo=pytz.utc).astimezone(ist)


def get_table_id_from_filename(file_path):
    name_split = file_path.split('.')
    table_id = name_split[0].lower().strip()
    return table_id


def read_csv_from_gcs(uri, project_id):
    fs = gcsfs.GCSFileSystem(project_id)
    with fs.open(uri) as file:
        df = pd.read_csv(file, encoding='latin-1')
    return df


def transform_dataframe(df, curr_time):
    df['created_at'] = curr_time
    df['modified_at'] = curr_time

    # Rename column if exists
    if 'Application Id/Proposer number' in df.columns:
        df.rename(columns={'Application Id/Proposer number': 'Application Id'}, inplace=True)

    return df


def load_to_bigquery(df, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0
    )

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded data into {dataset_id}.{table_id}")


def insert_audit_log(start_time, end_time, table_id, row_count):
    client = bigquery.Client()

    dataset_id = 'abcd_prod_data_logging_auditing'
    table_id_audit = 'abcd_prod_data_audit'

    table_ref = client.dataset(dataset_id).table(table_id_audit)
    table = bigquery.Table(table_ref)
    client.create_table(table, exists_ok=True)

    rows_to_insert = [
        ('Sampark', 'abcd_sampark_raw', table_id, start_time, end_time,
         'Cloud Function', row_count, None, None, None)
    ]

    if row_count != -1:
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Audit insert errors: {errors}")
        else:
            print(f"Audit log inserted for {table_id}")


def move_file(bucket_name, destination_bucket_name, file_path):
    gcs_client = storage.Client()

    source_bucket = gcs_client.get_bucket(bucket_name)
    destination_bucket = gcs_client.get_bucket(destination_bucket_name)

    blob = source_bucket.blob(file_path)
    source_bucket.copy_blob(blob, destination_bucket, blob.name)
    source_bucket.delete_blob(blob.name)

    print(f"Moved file {file_path} to processed bucket")


# -------------------------------
# Main Cloud Function
# -------------------------------

def hello_gcs(event, context):
    try:
        start_time = get_current_ist_time()

        # Env variables
        bucket_name = os.environ.get('bucket_name')
        destination_bucket_name = os.environ.get('bucket_name_processed')
        dataset_id = os.environ.get('dataset')

        # File details
        gcs_file_path = event['name']
        print(f"GCS file path: {gcs_file_path}")

        table_id = get_table_id_from_filename(gcs_file_path)
        print(f"Table ID: {table_id}")

        uri = f'gs://{bucket_name}/{gcs_file_path}'
        print(f"File URI: {uri}")

        # Read + Transform
        df = read_csv_from_gcs(uri, 'abcd-dataplatform-prod')
        df = transform_dataframe(df, start_time)

        # Load to BigQuery
        load_to_bigquery(df, dataset_id, table_id)

        # Audit log
        row_count = df.shape[0]
        end_time = get_current_ist_time()
        insert_audit_log(start_time, end_time, table_id, row_count)

        # Move file
        move_file(bucket_name, destination_bucket_name, gcs_file_path)

        return {}

    except Exception as e:
        push_email_notification(flag='internal', error=str(e))
        print(f"Error: {str(e)}")
        return {'error': str(e)}

# uri = f"gs://{bucket_name}/{gcs_file_path}"

# job_config = bigquery.LoadJobConfig(
#     source_format=bigquery.SourceFormat.CSV,
#     autodetect=True
# )

# load_job = bq_client.load_table_from_uri(
#     uri,
#     table_ref,
#     job_config=job_config
# )

# load_job.result()