# GCP Data warehouse Project 

## Overview
This project ingests data from multiple sources into BigQuery raw layer using:
- Dataproc (batch ingestion)
- Datastream (CDC)
- Cloud Functions (GCS trigger)

## Architecture
Terradata(simulated with MySQL) → Dataproc → BigQuery  
AWS RDS Mysql(simulated with cloud sql -MySQL) → Datastream → BigQuery  
on prem Mysql(simulated with MySQL) → Datastream → BigQuery  
GCS → Cloud Function → BigQuery  

```
MySQL (4 DBs)
├── customer_db
├── payments_db
├── app_analytics_db
└── sampark_db
        ↓
Datastream / Dataproc / GCS
        ↓
BigQuery RAW
        ↓
Transform (SQL / dbt)
        ↓
Analytics Layer
        ↓
Dashboards (Looker / Tableau)
```


## How CI CD works For Scheduling
```
Developer (VS Code)
        ↓
GitHub Repo
        ↓
CI/CD Pipeline (GitHub Actions / Cloud Build)
        ↓
GCS Bucket (Composer DAG folder & GCS pyspark Script bucket)
        ↓
Cloud Composer (Airflow)
        ↓
Schedules & Runs DAGs
```

## Setup for dataproc (teradata => BQ)
1. install gclod sdk
2. create virtual env, then gcloud init
3. Install dependencies: Pip install -r requirements.txt
4. Run setup_databases.sql and load_sample_data.sql in mysql client
5. Run infrastructure\bq_layer_design\row_layer\setup_row_layer.sql in BQ (it will setup row layer)
6. Go to the login.py script and run powersell command (metioned at last of script) inside the terminal 
7. by doing this testing of pyspark script will be done
8. Now schedulde this script by cloud composer using dag : orchestration\airflow_dags\dag.py


## Setup for datastream (mysql => BQ)
1. Run infrastructure\bq_layer_design\transformed_layer\setup_transformed_layer.sql in BQ
2. Run infrastructure\bq_layer_design\curated_layer\setup_curated_layer.sql in BQ
3. add the select scrits inside of infrastructure\bq_layer_design\bq_scheduler_scripts as schedulde queris in BQ

## Setup for cloud run (GCS => BQ)
1. Create cloud function with required variables in sampark.py
2. Replace the default scripts of cloud function by scripts in ingestion\gcs_to_bq\cloud_function
