# teradata-to-bigquery-migration
Sample components from Teradata → BigQuery migration


#Architecture Diagram

    flowchart TD
    A[Linux Server - Source CSV Files] --> B[Shell Script - Transfers Files]
    B --> C[GCS Bucket - jeni-td-bq-migration]
    C --> D[Cloud Function - Triggered on Upload]
    D --> E[BigQuery Staging Table - mydataset_1.sampletable_1]
    E --> F[BigQuery Main Table - mydataset_1.sampletable_1]

  #  Seqence Diagram

  
sequenceDiagram
    autonumber
    participant L as Linux Server
    participant S as Shell Script
    participant G as GCS Bucket
    participant CF as Cloud Function
    participant BQ as BigQuery

    L->>S: Generate CSV files
    S->>G: Upload CSV to GCS
    G->>CF: Trigger on file upload
    CF->>BQ: Load data into Staging Table (mydataset_1.sampletable_1)
    BQ->>BQ: Transform and insert into Main Table

    Folder structure Diagram

    flowchart TD
    A[Project Root] --> B[scripts]
    A --> C[cloudrun]
    A --> D[cloudfunctions]
    A --> E[bigquery]
    A --> F[README.md]

    B --> B1[shell_script_upload.sh]
    C --> C1[main.py]
    C --> C2[requirements.txt]
    D --> D1[main.py]
    D --> D2[requirements.txt]
    E --> E1[schema_staging.json]
    E --> E2[schema_main.json]

    
