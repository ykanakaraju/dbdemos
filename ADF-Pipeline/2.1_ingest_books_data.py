# Databricks notebook source
dbutils.widgets.text("date", "2025-06-01", "Date")

# COMMAND ----------

date_folder = dbutils.widgets.get("date")

landing_path = "abfss://bookstore-landing@adfdatasa.dfs.core.windows.net/"
datasets_path = landing_path + date_folder + "/books-json/"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adfdb;
# MAGIC USE adfdb_bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adfdb_bronze.books_raw (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

def ingest(source_path, file_name):

  from delta.tables import DeltaTable
  delta_target = DeltaTable.forName(spark, "adfdb_bronze.books_raw")

  df_source = spark.read.json(source_path + file_name)

  delta_target.alias("t")\
  .merge(
    source = df_source.alias("s"),
    condition = 't.book_id = s.book_id'
  ) \
  .whenMatchedDelete(
    condition = 's.row_status = "DELETE"'
  ) \
  .whenMatchedUpdate(
    condition = 's.row_status = "UPDATE"',
    set = {
      "title": "s.title", 
      "author": "s.author", 
      "category": "s.category", 
      "price": "s.price"
    }
  ) \
  .whenNotMatchedInsert(
    values = {
      "book_id": "s.book_id",
      "title": "s.title", 
      "author": "s.author", 
      "category": "s.category", 
      "price": "s.price"
    }
  ) \
  .execute()

  print("ingested the file: " + source_path + file_name)

# COMMAND ----------

for f in dbutils.fs.ls(datasets_path):
  ingest(datasets_path, f.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adfdb_bronze.books_raw ORDER BY book_id

# COMMAND ----------


