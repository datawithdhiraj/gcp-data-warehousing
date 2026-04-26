-- =========================
-- CUSTOMER DATA
-- =========================
USE teradata_customer_db;

INSERT INTO customers VALUES
(1,'Amit','Sharma','[amit@gmail.com](mailto:amit@gmail.com)','9999999991','1990-01-01','M',NOW(),NOW(),'active'),
(2,'Priya','Patil','[priya@gmail.com](mailto:priya@gmail.com)','9999999992','1995-05-10','F',NOW(),NOW(),'active'),
(3,'Rahul','Verma','[rahul@gmail.com](mailto:rahul@gmail.com)','9999999993','1988-03-15','M',NOW(),NOW(),'inactive');

INSERT INTO addresses VALUES
(1,1,'Mumbai','MH','India','400001','home',NOW()),
(2,2,'Pune','MH','India','411001','home',NOW());

INSERT INTO customer_preferences VALUES
(1,1,'English',TRUE,'Sports',NOW()),
(2,2,'Hindi',TRUE,'Fashion',NOW());

-- =========================
-- PAYMENTS DATA
-- =========================
USE aws_rds_mysql_payments_db;

INSERT INTO payment_methods VALUES
(1,1,'card','Visa',NOW()),
(2,2,'upi','GPay',NOW());

INSERT INTO transactions VALUES
(1,1,500.00,'INR',NOW(),'success',1),
(2,2,1200.00,'INR',NOW(),'success',2),
(3,1,300.00,'INR',NOW(),'failed',1);

INSERT INTO refunds VALUES
(1,3,300.00,'failed payment','processed',NOW());

-- =========================
-- APP ANALYTICS DATA
-- =========================
USE on_prem_app_db;

INSERT INTO user_sessions VALUES
(1,1,NOW(),NOW(),'android','1.0'),
(2,2,NOW(),NOW(),'ios','1.1');

INSERT INTO user_events VALUES
(1,1,1,'click',NOW(),'home'),
(2,2,2,'view',NOW(),'product');

INSERT INTO category_engagement VALUES
(1,1,'Sports',1200,CURDATE()),
(2,2,'Fashion',800,CURDATE());
