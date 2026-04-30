CREATE SCHEMA IF NOT EXISTS 'abfssl_teradata_transformed';
CREATE TABLE IF NOT EXISTS `abfssl_teradata_transformed.customers` (
  customer_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  date_of_birth DATE,
  gender STRING,
  status STRING,
  created_at DATETIME,
  updated_at DATETIME,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);

-- -----------------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS `abfssl_teradata_transformed..addresses` (
--   address_id INT64,
--   customer_id INT64,
--   city STRING,
--   state STRING,
--   country STRING,
--   pincode STRING,
--   address_type STRING,
--   created_at DATETIME,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time)


-- --------------------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS `abfssl_teradata_transformed.customer_preferences` (
--   customer_id INT64,
--   preferred_language STRING,
--   notification_opt_in BOOL,
--   favorite_category STRING,
--   updated_at DATETIME,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time)
-- ==========================================================================================================================================================
CREATE SCHEMA IF NOT EXISTS 'abcd_payments_transformed';
CREATE TABLE IF NOT EXISTS `abcd_payments_transformed.transactions` (
  transaction_id INT64,
  customer_id INT64,
  amount NUMERIC,
  currency STRING,
  transaction_date DATETIME,
  status STRING,
  payment_method_id INT64,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `abcd_payments_transformed.payment_methods` (
--   payment_method_id INT64,
--   customer_id INT64,
--   method_type STRING,
--   provider STRING,
--   created_at DATETIME,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time)

-- CREATE TABLE IF NOT EXISTS `abcd_payments_transformed.refunds` (
--   refund_id INT64,
--   transaction_id INT64,
--   refund_amount NUMERIC,
--   refund_reason STRING,
--   refund_status STRING,
--   created_at DATETIME,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time)

-- =================================================================================================================================================
CREATE SCHEMA IF NOT EXISTS 'abcd_mobile_transformed';
CREATE TABLE IF NOT EXISTS `abcd_mobile_transformed.user_sessions` (
  session_id INT64,
  customer_id INT64,
  session_start DATETIME,
  session_end DATETIME,
  session_duration_seconds INT64,
  device_type STRING,
  app_version STRING,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `transform_layer.user_events` (
--   event_id INT64,
--   session_id INT64,
--   customer_id INT64,
--   event_type STRING,
--   event_timestamp DATETIME,
--   screen_name STRING,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `transform_layer.category_engagement` (
--   engagement_id INT64,
--   customer_id INT64,
--   category_name STRING,
--   time_spent_seconds INT64,
--   engagement_date DATE,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);

-- =====================================================================================================

CREATE SCHEMA IF NOT EXISTS 'abcd_sampark_transform';
CREATE TABLE IF NOT EXISTS `abcd_sampark_transform.campaigns` (
  campaign_id INT64,
  campaign_name STRING,
  channel STRING,
  start_date DATE,
  end_date DATE,
  campaign_duration_days INT64,
  budget NUMERIC,
  status STRING,
  ingestion_time TIMESTAMP
)
PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `abcd_sampark_transform.ads` (
--   ad_id INT64,
--   campaign_id INT64,
--   creative_type STRING,
--   headline STRING,
--   created_at DATETIME,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `abcd_sampark_transform.impressions` (
--   impression_id INT64,
--   ad_id INT64,
--   customer_id INT64,
--   impression_time DATETIME,
--   platform STRING,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `abcd_sampark_transform.clicks` (
--   click_id INT64,
--   ad_id INT64,
--   customer_id INT64,
--   click_time DATETIME,
--   device_type STRING,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);

-- CREATE TABLE IF NOT EXISTS `abcd_sampark_transform.conversions` (
--   conversion_id INT64,
--   customer_id INT64,
--   campaign_id INT64,
--   conversion_time DATETIME,
--   conversion_type STRING,
--   revenue NUMERIC,
--   ingestion_time TIMESTAMP
-- )
-- PARTITION BY DATE(ingestion_time);
