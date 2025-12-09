from google.cloud import bigquery

client = bigquery.Client(project="jeni-td-bq-migration")

table_id = "jeni-td-bq-migration.mydataset_1.sampletable_1"
uri = "gs://my-bucket/input/data.csv"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    write_disposition="WRITE_APPEND",
)

load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()

print("Loaded data into BigQuery:", table_id)
