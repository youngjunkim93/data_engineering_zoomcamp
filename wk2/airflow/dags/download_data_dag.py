import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET_SOURCE = {"yellow": "https://s3.amazonaws.com/nyc-tlc",
                  "green": "https://nyc-tlc.s3.amazonaws.com",
                  "fhv": "https://nyc-tlc.s3.amazonaws.com"}


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": "2019-01-01",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
        dag_id="download_data_dag",
        schedule_interval="@monthly",
        default_args=default_args,
        catchup=True,
        max_active_runs=3,
        tags=['de-zoomcamp'],
) as dag:
    range_indicator = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}'
    for cab_type in ["yellow", "green", "fhv"]:
        dataset_file = f"{cab_type}_tripdata_{range_indicator}.csv"
        dataset_url = f"{DATASET_SOURCE[cab_type]}/trip+data/{dataset_file}"
        parquet_file = dataset_file.replace('.csv', '.parquet')

        download_dataset_task = BashOperator(
            task_id=f"{cab_type}_download_dataset_task",
            bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
        )

        format_to_parquet_task = PythonOperator(
            task_id=f"{cab_type}_format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"{cab_type}_local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{parquet_file}",
                "local_file": f"{path_to_local_home}/{parquet_file}",
            },
        )

        delete_temp_file_task = BashOperator(
            task_id=f"{cab_type}_delete_temp_file_task",
            bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_temp_file_task