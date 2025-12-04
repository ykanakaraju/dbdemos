-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS movies_landing
URL "s3://cts-databricks-demo-data/landing/"
WITH (STORAGE CREDENTIAL `ctsdatabricksdemorolecred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movies_bronze
URL "s3://cts-databricks-demo-data/bronze/"
WITH (STORAGE CREDENTIAL `ctsdatabricksdemorolecred`);

-- COMMAND ----------

DESC EXTERNAL LOCATION movielens_bronze;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movies_silver
URL "s3://cts-databricks-demo-data/silver/"
WITH (STORAGE CREDENTIAL `ctsdatabricksdemorolecred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movies_gold
URL "s3://cts-databricks-demo-data/gold/"
WITH (STORAGE CREDENTIAL `ctsdatabricksdemorolecred`);

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------


