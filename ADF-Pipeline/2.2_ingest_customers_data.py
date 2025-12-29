# Databricks notebook source
dbutils.widgets.text("date", "2025-06-01", "Date")

# COMMAND ----------

date_folder = dbutils.widgets.get("date")

landing_path = "abfss://bookstore-landing@adfdatasa.dfs.core.windows.net/"
datasets_path = landing_path + date_folder + "/customers-json/"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adfdb;
# MAGIC USE adfdb_bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adfdb_bronze.customers_raw (
# MAGIC   customer_id STRING,
# MAGIC   email STRING,
# MAGIC   profile STRING,
# MAGIC   updated STRING
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

def ingest(source_path, file_name):

  from delta.tables import DeltaTable
  delta_target = DeltaTable.forName(spark, "adfdb_bronze.customers_raw")

  df_source = spark.read.json(source_path + file_name)

  delta_target.alias("t")\
  .merge(
    source = df_source.alias("s"),
    condition = 't.customer_id = s.customer_id'
  ) \
  .whenMatchedUpdate(
    condition = 't.email IS NULL AND s.email IS NOT NULL',
    set = {
      "email": "s.email", 
      "updated": "s.updated"
    }
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

  print("ingested the file: " + source_path + file_name)

# COMMAND ----------

for f in dbutils.fs.ls(datasets_path):
  ingest(datasets_path, f.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adfdb_bronze.customers_raw ORDER BY customer_id

# COMMAND ----------


