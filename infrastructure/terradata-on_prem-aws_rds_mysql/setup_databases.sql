-- =========================
-- DROP DATABASES (CLEANUP)
-- =========================
DROP DATABASE IF EXISTS teradata_customer_db;
DROP DATABASE IF EXISTS aws_rds_mysql_payments_db;
DROP DATABASE IF EXISTS on_prem_app_db;

-- =========================
-- CREATE DATABASES
-- =========================
CREATE DATABASE teradata_customer_db;
CREATE DATABASE aws_rds_mysql_payments_db;
CREATE DATABASE on_prem_app_db;

-- =========================
-- CUSTOMER DB
-- =========================
USE teradata_customer_db;

CREATE TABLE customers (
customer_id INT PRIMARY KEY,
first_name VARCHAR(50),
last_name VARCHAR(50),
email VARCHAR(100),
phone VARCHAR(20),
date_of_birth DATE,
gender VARCHAR(10),
created_at DATETIME,
updated_at DATETIME,
status VARCHAR(20)
);

CREATE TABLE addresses (
address_id INT PRIMARY KEY,
customer_id INT,
city VARCHAR(50),
state VARCHAR(50),
country VARCHAR(50),
pincode VARCHAR(10),
address_type VARCHAR(20),
created_at DATETIME
);

CREATE TABLE customer_preferences (
preference_id INT PRIMARY KEY,
customer_id INT,
preferred_language VARCHAR(20),
notification_opt_in BOOLEAN,
favorite_category VARCHAR(50),
updated_at DATETIME
);

-- =========================
-- PAYMENTS DB
-- =========================
USE aws_rds_mysql_payments_db;

CREATE TABLE payment_methods (
payment_method_id INT PRIMARY KEY,
customer_id INT,
method_type VARCHAR(20),
provider VARCHAR(50),
created_at DATETIME
);

CREATE TABLE transactions (
transaction_id INT PRIMARY KEY,
customer_id INT,
amount DECIMAL(10,2),
currency VARCHAR(10),
transaction_date DATETIME,
status VARCHAR(20),
payment_method_id INT
);

CREATE TABLE refunds (
refund_id INT PRIMARY KEY,
transaction_id INT,
refund_amount DECIMAL(10,2),
refund_reason VARCHAR(100),
refund_status VARCHAR(20),
created_at DATETIME
);

-- =========================
-- APP ANALYTICS DB
-- =========================
USE on_prem_app_db;

CREATE TABLE user_sessions (
session_id INT PRIMARY KEY,
customer_id INT,
session_start DATETIME,
session_end DATETIME,
device_type VARCHAR(20),
app_version VARCHAR(10)
);

CREATE TABLE user_events (
event_id INT PRIMARY KEY,
session_id INT,
customer_id INT,
event_type VARCHAR(50),
event_timestamp DATETIME,
screen_name VARCHAR(50)
);

CREATE TABLE category_engagement (
engagement_id INT PRIMARY KEY,
customer_id INT,
category_name VARCHAR(50),
time_spent_seconds INT,
date DATE
);
