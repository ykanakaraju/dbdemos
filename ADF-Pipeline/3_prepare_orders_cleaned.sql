-- Databricks notebook source
USE CATALOG adfdb;
USE adfdb_silver;

CREATE OR REPLACE TABLE orders_cleaned
AS
  SELECT 
      order_id, 
      quantity, 
      o.customer_id, 
      c.profile:first_name as f_name, 
      c.profile:last_name as l_name,
      cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, 
      o.books,
      c.profile:address:country as country
  FROM 
    adfdb_bronze.orders_raw o
  JOIN 
    adfdb_bronze.customers_raw c
  ON 
    o.customer_id = c.customer_id

-- COMMAND ----------

SELECT * FROM orders_cleaned

-- COMMAND ----------


