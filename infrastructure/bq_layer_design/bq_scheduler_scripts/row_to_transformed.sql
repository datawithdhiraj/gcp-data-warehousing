SELECT
  customer_id,
  INITCAP(first_name) AS first_name,
  INITCAP(last_name) AS last_name,
  LOWER(email) AS email,
  phone,
  date_of_birth,
  gender,
  status,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() AS ingestion_time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM `abfssl_teradata_raw.customers`
)
WHERE rn = 1;

-- SELECT
--   address_id,
--   customer_id,
--   UPPER(city) AS city,
--   UPPER(state) AS state,
--   UPPER(country) AS country,
--   pincode,
--   address_type,
--   created_at,
--   CURRENT_TIMESTAMP() AS ingestion_time
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY created_at DESC) AS rn
--   FROM `abfssl_teradata_raw.addresses`
-- )
-- WHERE rn = 1
--   AND pincode IS NOT NULL;

-- SELECT
--   customer_id,
--   preferred_language,
--   notification_opt_in,
--   UPPER(favorite_category) AS favorite_category,
--   updated_at,
--   CURRENT_TIMESTAMP() AS ingestion_time
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
--   FROM `abfssl_teradata_raw.customer_preferences`
-- )
-- WHERE rn = 1;


-- ============================================================================================================================================
SELECT
  transaction_id,
  customer_id,
  amount,
  UPPER(currency) AS currency,
  transaction_date,
  UPPER(status) AS status,
  payment_method_id,
  CURRENT_TIMESTAMP() AS ingestion_time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date DESC) AS rn
  FROM `abcd_payments_row.aws_rds_mysql_payments_db_transactions`
)
WHERE rn = 1
  AND amount IS NOT NULL;

-- SELECT
--   payment_method_id,
--   customer_id,
--   UPPER(method_type) AS method_type,
--   INITCAP(provider) AS provider,
--   created_at,
--   CURRENT_TIMESTAMP() AS ingestion_time
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY created_at DESC) AS rn
--   FROM `abcd_payments_row.payment_methods`
-- )
-- WHERE rn = 1;


-- SELECT
--   refund_id,
--   transaction_id,
--   refund_amount,
--   INITCAP(refund_reason) AS refund_reason,
--   UPPER(refund_status) AS refund_status,
--   created_at,
--   CURRENT_TIMESTAMP() AS ingestion_time
-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY created_at DESC) AS rn
--   FROM `abcd_payments_row.refunds`
-- )
-- WHERE rn = 1
--   AND refund_amount > 0;
-- ============================================================================================================

SELECT
  session_id,
  customer_id,
  session_start,
  session_end,

  -- Derived column (important)
  TIMESTAMP_DIFF(
    TIMESTAMP(session_end),
    TIMESTAMP(session_start),
    SECOND
  ) AS session_duration_seconds,

  UPPER(device_type) AS device_type,
  app_version,
  CURRENT_TIMESTAMP() AS ingestion_time

FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_start DESC) AS rn
  FROM `abcd_mobile_row.on_prem_app_db_user_sessions`
)
WHERE rn = 1
  AND session_end IS NOT NULL;

--   SELECT
--   event_id,
--   session_id,
--   customer_id,
--   UPPER(event_type) AS event_type,
--   event_timestamp,
--   INITCAP(screen_name) AS screen_name,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn
--   FROM `abcd_mobile_row.user_events`
-- )
-- WHERE rn = 1;


-- SELECT
--   engagement_id,
--   customer_id,
--   INITCAP(category_name) AS category_name,
--   time_spent_seconds,
--   date AS engagement_date,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY engagement_id ORDER BY date DESC) AS rn
--   FROM `abcd_mobile_row.category_engagement`
-- )
-- WHERE rn = 1
--   AND time_spent_seconds > 0;

-- ===================================================================================================

SELECT
  campaign_id,
  INITCAP(campaign_name) AS campaign_name,
  UPPER(channel) AS channel,
  start_date,
  end_date,

  -- Derived metric
DATE_DIFF(DATE(end_date), DATE(start_date), DAY) AS campaign_duration_days,

  budget,
  UPPER(status) AS status,
  CURRENT_TIMESTAMP() AS ingestion_time

FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY end_date DESC) AS rn
  FROM `abcd_sampark_row.campaigns`
)
WHERE rn = 1;

-- SELECT
--   ad_id,
--   campaign_id,
--   UPPER(creative_type) AS creative_type,
--   INITCAP(headline) AS headline,
--   created_at,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY created_at DESC) AS rn
--   FROM `abcd_sampark_row.ads`
-- )
-- WHERE rn = 1;

-- SELECT
--   impression_id,
--   ad_id,
--   customer_id,
--   impression_time,
--   UPPER(platform) AS platform,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY impression_id ORDER BY impression_time DESC) AS rn
--   FROM `abcd_sampark_row.impressions`
-- )
-- WHERE rn = 1;

-- SELECT
--   click_id,
--   ad_id,
--   customer_id,
--   click_time,
--   UPPER(device_type) AS device_type,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY click_time DESC) AS rn
--   FROM `abcd_sampark_row.clicks`
-- )
-- WHERE rn = 1;

-- SELECT
--   conversion_id,
--   customer_id,
--   campaign_id,
--   conversion_time,
--   UPPER(conversion_type) AS conversion_type,
--   revenue,
--   CURRENT_TIMESTAMP() AS ingestion_time

-- FROM (
--   SELECT *,
--          ROW_NUMBER() OVER (PARTITION BY conversion_id ORDER BY conversion_time DESC) AS rn
--   FROM `abcd_sampark_row.conversions`
-- )
-- WHERE rn = 1
--   AND revenue >= 0;