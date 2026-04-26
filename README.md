# GCP Data Pipeline Project

## Overview
This project ingests data from multiple sources into BigQuery raw layer using:
- Dataproc (batch ingestion)
- Datastream (CDC)
- Cloud Functions (GCS trigger)

## Architecture
MySQL → Dataproc → BigQuery  
MySQL → Datastream → BigQuery  
MySQL → Datastream → BigQuery  
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

## Setup
1. Install dependencies: Pip install -r requirements.txt
2. Run setup_databases.sql and load_sample_data.sql in mysql client
2. Configure GCP credentials
3. Deploy using CI/CD



