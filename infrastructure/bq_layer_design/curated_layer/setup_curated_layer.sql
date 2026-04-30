CREATE DATABASE IF NOT EXISTS 'abcd_curated';

CREATE TABLE IF NOT EXISTS `abcd_curated.customer_summary` (
  customer_id INT64,
  first_name STRING,
  last_name STRING,
  total_transactions INT64,
  total_spent NUMERIC,
  total_sessions INT64,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);

CREATE TABLE IF NOT EXISTS `abcd_curated.campaign_summary` (
  campaign_id INT64,
  campaign_name STRING,
  channel STRING,
  total_revenue NUMERIC,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);