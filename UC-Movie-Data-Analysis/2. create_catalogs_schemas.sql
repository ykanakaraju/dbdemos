-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS movielens_dev;

-- COMMAND ----------

USE CATALOG movielens_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_landing
MANAGED LOCATION "abfss://landing@movielenssa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_bronze
MANAGED LOCATION "abfss://bronze@movielenssa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_silver
MANAGED LOCATION "abfss://silver@movielenssa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_gold
MANAGED LOCATION "abfss://gold@movielenssa.dfs.core.windows.net/"

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------


