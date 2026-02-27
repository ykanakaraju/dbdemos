-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### OPTIMIZE Command
-- MAGIC The OPTIMIZE command rewrites data files to improve data layout for Delta tables.

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------


SELECT * FROM users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

-- OPTIMIZE users 
OPTIMIZE users ZORDER BY (id)

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Z Ordering**
-- MAGIC
-- MAGIC Z Ordering your data reorganizes the data in storage and allows certain queries to read less data, so they run faster. When your data is appropriately ordered, more files can be skipped.
-- MAGIC
-- MAGIC Z Order is particularly important for the ordering of multiple columns. If you only need to order by a single column, then simple sorting suffices. If there are multiple columns, but we always/only query a common prefix of those columns, then hierarchical sorting suffices. Z Ordering is good when querying on one or multiple columns. 

-- COMMAND ----------

LIST 'dbfs:/FileStore/data/shopping/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file = "dbfs:/FileStore/data/shopping/invoices_201_99457.parquet"
-- MAGIC df = spark.read.parquet(file)
-- MAGIC df = df.select("customer_id", "category", "price", "quantity", "invoice_date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df.limit(5))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_union = df
-- MAGIC expected_rows = 10000000    # 10 million
-- MAGIC
-- MAGIC while df_union.count() <= expected_rows:
-- MAGIC     df_union = df_union.union(df_union)
-- MAGIC     print(f"count: {df_union.count()}")
-- MAGIC
-- MAGIC print(f"final count: {df_union.count()}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_union.write.mode("overwrite").saveAsTable("zorder_ex1")

-- COMMAND ----------

LIST "dbfs:/user/hive/warehouse/zorder_ex1"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%time
-- MAGIC spark.sql(
-- MAGIC     """
-- MAGIC     SELECT category, SUM(price * quantity) as total_sales
-- MAGIC     FROM zorder_ex1
-- MAGIC     WHERE customer_id = 201
-- MAGIC     GROUP BY category
-- MAGIC     """
-- MAGIC )

-- COMMAND ----------

OPTIMIZE zorder_ex1 ZORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # from delta.tables import DeltaTable
-- MAGIC
-- MAGIC # table = DeltaTable.forName(spark, "zorder_ex1")
-- MAGIC # table.optimize().executeZOrderBy("customer_id")

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/zorder_ex1'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%time
-- MAGIC spark.sql(
-- MAGIC     """
-- MAGIC     SELECT category, SUM(price * quantity) as total_sales
-- MAGIC     FROM zorder_ex1
-- MAGIC     WHERE customer_id = 201
-- MAGIC     GROUP BY category
-- MAGIC     """
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").partitionBy("invoice_date").saveAsTable("zorder_ex2")

-- COMMAND ----------

show tables

-- COMMAND ----------

DESC DETAIL zorder_ex2

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/zorder_ex2'

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/zorder_ex2/invoice_date=2021-01-01/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC df_new_partition = df \
-- MAGIC     .filter(col("invoice_date") == "2023-01-01") \
-- MAGIC     .withColumn("invoice_date", to_date(lit("2026-02-23")))
-- MAGIC
-- MAGIC display(df_new_partition.limit(5))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_new_partition \
-- MAGIC   .write \
-- MAGIC   .mode("append") \
-- MAGIC   .partitionBy("invoice_date") \
-- MAGIC   .saveAsTable("zorder_ex2")

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/zorder_ex2'

-- COMMAND ----------

SELECT MAX(invoice_date) FROM zorder_ex2;

-- COMMAND ----------

OPTIMIZE zorder_ex2 
WHERE invoice_date = '2026-02-23'       -- {current_day - 1}
ZORDER BY customer_id;

-- COMMAND ----------


