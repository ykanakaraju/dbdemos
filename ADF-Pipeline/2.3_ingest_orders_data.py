# Databricks notebook source
dbutils.widgets.text("date", "2025-06-01", "Date")

# COMMAND ----------

date_folder = dbutils.widgets.get("date")
landing_path = "abfss://bookstore-landing@adfdatasa.dfs.core.windows.net/"
datasets_path = landing_path + date_folder + "/orders-parquet/"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adfdb;
# MAGIC USE adfdb_bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adfdb_bronze.orders_raw (
# MAGIC   order_id STRING,
# MAGIC   order_timestamp BIGINT,
# MAGIC   customer_id STRING,
# MAGIC   quantity INT,
# MAGIC   total INT,
# MAGIC   books ARRAY<STRUCT<book_id:STRING, quantity:INT, subtotal:BIGINT>>
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

def ingest(source_path, file_name):

  df_source = spark.read.parquet(source_path + file_name)
  df_source.write.insertInto("adfdb_bronze.orders_raw")

  print("ingested the file: " + source_path + file_name)

# COMMAND ----------

for f in dbutils.fs.ls(datasets_path):
  ingest(datasets_path, f.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adfdb_bronze.orders_raw ORDER BY order_id

# COMMAND ----------


