-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS adfdb;
USE CATALOG adfdb;

-- COMMAND ----------

DROP SCHEMA IF EXISTS adfdb_bronze CASCADE;
DROP SCHEMA IF EXISTS adfdb_silver CASCADE;
DROP SCHEMA IF EXISTS adfdb_gold CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS adfdb_bronze
MANAGED LOCATION "abfss://bookstore-bronze@adfdatasa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS adfdb_silver
MANAGED LOCATION "abfss://bookstore-silver@adfdatasa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS adfdb_gold
MANAGED LOCATION "abfss://bookstore-gold@adfdatasa.dfs.core.windows.net/"
