-- Databricks notebook source
USE CATALOG adfdb;
USE adfdb_gold;

CREATE OR REPLACE TABLE cn_daily_customer_books
AS
  SELECT 
    customer_id, 
    f_name, 
    l_name, 
    date_trunc("DD", order_timestamp) order_date, 
    sum(quantity) books_counts
  FROM 
    adfdb_silver.orders_cleaned
  WHERE 
    country = "China"
  GROUP BY 
    customer_id, 
    f_name, 
    l_name, 
    date_trunc("DD", order_timestamp)

-- COMMAND ----------

SELECT * FROM cn_daily_customer_books

-- COMMAND ----------

CREATE OR REPLACE TABLE fr_daily_customer_books
AS
  SELECT 
    customer_id, 
    f_name, 
    l_name, 
    date_trunc("DD", order_timestamp) order_date, 
    sum(quantity) books_counts
  FROM 
    adfdb_silver.orders_cleaned
  WHERE 
    country = "France"
  GROUP BY 
    customer_id, 
    f_name, 
    l_name, 
    date_trunc("DD", order_timestamp)

-- COMMAND ----------

SELECT * FROM fr_daily_customer_books
