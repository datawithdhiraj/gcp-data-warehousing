SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  COUNT(DISTINCT t.transaction_id) AS total_transactions,
  SUM(t.amount) AS total_spent,
  COUNT(DISTINCT s.session_id) AS total_sessions,
  CURRENT_TIMESTAMP() AS ingestion_time
FROM `abfssl_teradata_transformed.customers` c
LEFT JOIN `abcd_payments_transformed.transactions` t
  ON c.customer_id = t.customer_id
LEFT JOIN `abcd_mobile_transformed.user_sessions` s
  ON c.customer_id = s.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name;



SELECT
  camp.campaign_id,
  camp.campaign_name,
  camp.channel,
  SUM(t.amount) AS total_revenue,
  CURRENT_TIMESTAMP() AS ingestion_time
FROM `abcd_sampark_transform.campaigns` camp
LEFT JOIN `abcd_payments_transformed.transactions` t
  ON camp.campaign_id = t.payment_method_id
GROUP BY
  camp.campaign_id,
  camp.campaign_name,
  camp.channel;