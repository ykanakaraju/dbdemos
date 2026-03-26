# Databricks notebook source
#### Add new data files to source locations

# COMMAND ----------

dbutils.widgets.text("dataset_bookstore", "/Volumes/workspace/default/data/bookstore", "Path")

# COMMAND ----------

dataset_bookstore = dbutils.widgets.get("dataset_bookstore")

# COMMAND ----------

# MAGIC %run ./Includes/copy_utils

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

## %fs ls /Volumes/workspace/default/data/bookstore/books-cdc

# COMMAND ----------

## %fs ls /Volumes/workspace/default/data/bookstore/orders-json-raw

# COMMAND ----------

