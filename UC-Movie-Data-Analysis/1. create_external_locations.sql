-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_landing
URL "abfss://landing@movielenssa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `movielenssa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_bronze
URL "abfss://bronze@movielenssa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `movielenssa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_silver
URL "abfss://silver@movielenssa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `movielenssa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_gold
URL "abfss://gold@movielenssa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `movielenssa-cred`);

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------

DESC EXTERNAL LOCATION movielens_landing

-- COMMAND ----------

DROP EXTERNAL LOCATION movielens_gold

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display( dbutils.fs.ls("abfss://landing@movielenssa.dfs.core.windows.net/") )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display( dbutils.fs.ls("abfss://gold@movielenssa.dfs.core.windows.net/") )

-- COMMAND ----------


