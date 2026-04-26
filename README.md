# GCP Data Pipeline Project

## Overview
This project ingests data from multiple sources into BigQuery raw layer using:
- Dataproc (batch ingestion)
- Datastream (CDC)
- Cloud Functions (GCS trigger)

## Architecture
MySQL → Dataproc → BigQuery  
MySQL → Datastream → BigQuery  
GCS → Cloud Function → BigQuery  

## Setup
1. Install dependencies
2. Configure GCP credentials
3. Deploy using CI/CD