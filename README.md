# teradata-to-bigquery-migration
Sample components from Teradata → BigQuery migration

flowchart TD
    A[Linux Server<br>Source CSV Files] --> B[Shell Script<br>Data Transfer]
    B --> C[GCS Bucket<br>(jeni-td-bq-migration)]
    C --> D[Cloud Function<br>Triggered on Upload]
    D --> E[BigQuery Staging Table<br>mydataset_1.sampletable_1]
    E --> F[BigQuery Main Table<br>mydataset_1.sampletable_1]
