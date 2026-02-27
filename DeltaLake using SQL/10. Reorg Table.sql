-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### REORG TABLE
-- MAGIC
-- MAGIC - Reorganize a Delta Lake table by rewriting files to purge soft-deleted data, such as the column data dropped by `ALTER TABLE DROP COLUMN`.
-- MAGIC
-- MAGIC [Documentation](https://docs.databricks.com/en/sql/language-manual/delta-reorg-table.html)

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/learning-spark-v2/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/learning-spark-v2/flights/

-- COMMAND ----------

-- MAGIC %fs head dbfs:/databricks-datasets/learning-spark-v2/flights/departuredelays.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC file_path = "dbfs:/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
-- MAGIC schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
-- MAGIC w = Window().orderBy(lit('A'))
-- MAGIC
-- MAGIC raw_df = (
-- MAGIC     spark.read
-- MAGIC     .format("csv")
-- MAGIC     .schema(schema)
-- MAGIC     .option("header", "true")
-- MAGIC     .load(file_path)
-- MAGIC     .withColumn("date", date_format(to_timestamp("date", 'MMddHHmm'), "2025-MM-dd HH:mm"))
-- MAGIC     .withColumn("id", row_number().over(w))
-- MAGIC     .select("id", "date", "delay", "distance", "origin", "destination")
-- MAGIC     .repartition(8)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_df.limit(5).display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC   
-- MAGIC (
-- MAGIC     raw_df.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("departure_delays")
-- MAGIC )

-- COMMAND ----------

SELECT * FROM departure_delays LIMIT 10

-- COMMAND ----------

DESC EXTENDED departure_delays

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays

-- COMMAND ----------

DELETE FROM departure_delays WHERE id < 5

-- COMMAND ----------

UPDATE departure_delays SET delay = delay + 1 WHERE id > 1000

-- COMMAND ----------

DESC HISTORY departure_delays

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays

-- COMMAND ----------

ALTER TABLE departure_delays 
SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

-- COMMAND ----------

ALTER TABLE departure_delays 
DROP COLUMNS(distance)

-- COMMAND ----------

SELECT * FROM departure_delays ORDER BY id LIMIT 10

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays

-- COMMAND ----------

DESC HISTORY departure_delays

-- COMMAND ----------

SELECT * FROM departure_delays
ORDER BY id LIMIT 10

-- COMMAND ----------

SELECT * FROM departure_delays VERSION AS OF 1
ORDER BY id LIMIT 10

-- COMMAND ----------

REORG TABLE departure_delays APPLY(PURGE)

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays

-- COMMAND ----------

DESC DETAIL departure_delays

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays/RW/

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays

-- COMMAND ----------

-- VACUUM departure_delays RETAIN 0 HOURS DRY RUN
VACUUM departure_delays RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/departure_delays/gm/

-- COMMAND ----------

DROP TABLE departure_delays

-- COMMAND ----------


