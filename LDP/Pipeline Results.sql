-- Databricks notebook source
SHOW CATALOGS

-- COMMAND ----------

USE CATALOG bookstore_catalog;
USE default;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM orders_raw

-- COMMAND ----------

SELECT * FROM cn_daily_customer_books

-- COMMAND ----------

SELECT * FROM fr_daily_customer_books

-- COMMAND ----------

SELECT * FROM orders_cleaned

-- COMMAND ----------

SELECT * FROM books_bronze

-- COMMAND ----------

SELECT * FROM books_silver

-- COMMAND ----------


