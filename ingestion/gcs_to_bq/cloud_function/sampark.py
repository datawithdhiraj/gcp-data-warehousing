import os
import csv
from datetime import datetime
from google.cloud import storage, bigquery
import pandas as pd
import pytz
import gcsfs
from notification import push_email_notification


def hello_gcs(event, context):
    try:

        ist = pytz.timezone('Asia/Kolkata')
        curr_time = datetime.now().replace(tzinfo=pytz.utc).astimezone(ist)
        start_time = datetime.now().replace(tzinfo=pytz.utc).astimezone(ist)

        bq_client = bigquery.Client()
        gcs_client = storage.Client()

        bucket_name = os.environ.get('bucket_name')
        destination_bucket_name = os.environ.get('bucket_name_processed')

        gcs_file_path = event['name']
        print("gcs file path: " + gcs_file_path)

        name_split = gcs_file_path.split('.')  # file name , .csv
        table_id = name_split[0].lower()
        table_id = table_id.rstrip("_raw")

        print("table id: " + table_id)

        dataset_id = os.environ.get('dataset')

        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 0

        uri = f'gs://{bucket_name}/{gcs_file_path}'
        print(uri)

        fs = gcsfs.GCSFileSystem('abcd-dataplatform-prod')
        print(curr_time)

        with fs.open(uri) as file:
            df = pd.read_csv(file, encoding='latin-1')
            df['created_at'] = curr_time
            df['modified_at'] = curr_time
            df.rename(columns={'Application Id/Proposer number': 'Application Id'}, inplace=True)

        load_job = bq_client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        load_job.result()

        table = bq_client.get_table(table_ref)
        print(f"Loaded data into {dataset_id}.{table_id}")

        row_count = df.shape[0]

        client = bigquery.Client()
        dataset__id = 'abcd_prod_data_logging_auditing'
        table__id = 'abcd_prod_data_audit'

        table_ref = client.dataset(dataset__id).table(table__id)

        table = bigquery.Table(table_ref)
        table = client.create_table(table, exists_ok=True)

        end_time = datetime.utcnow()

        rows_to_insert = [
            ('Sampark', 'abcd_sampark_raw', table_id, start_time, end_time,
             'Cloud Function', row_count, None, None, None)
        ]

        if row_count != -1:
            errors = client.insert_rows(table, rows_to_insert)
            if errors:
                print(f"Errors: {errors}")
            print('row_count', row_count, 'start_time', start_time, 'end_time', end_time)

        source_bucket = gcs_client.get_bucket(bucket_name)
        destination_bucket = gcs_client.get_bucket(destination_bucket_name)

        blob = source_bucket.blob(gcs_file_path)
        source_bucket.copy_blob(blob, destination_bucket, blob.name)
        source_bucket.delete_blob(blob.name)

        return {}

    except Exception as e:
        push_email_notification(flag='internal', error=str(e))
        print(f"An error occurred: {str(e)}")
        return {'error': str(e)}