-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS movielens_dev 
MANAGED LOCATION 's3://databricks-storage-3827294427167860/unity-catalog/3827294427167860'

-- COMMAND ----------

USE CATALOG movielens_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_landing
MANAGED LOCATION "s3://cts-databricks-demo-data/landing/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_bronze
MANAGED LOCATION "s3://cts-databricks-demo-data/bronze/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_silver
MANAGED LOCATION "s3://cts-databricks-demo-data/silver/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_gold
MANAGED LOCATION "s3://cts-databricks-demo-data/gold/"

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------


