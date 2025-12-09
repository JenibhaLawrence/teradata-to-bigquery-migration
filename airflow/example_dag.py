from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "gcs_to_bq_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_to_bq",
        configuration={
            "load": {
                "sourceUris": ["gs://my-bucket/input/data.csv"],
                "destinationTable": {
                    "projectId": "jeni-td-bq-migration",
                    "datasetId": "mydataset_1",
                    "tableId": "sampletable_1",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "skipLeadingRows": 1,
            }
        },
    )
