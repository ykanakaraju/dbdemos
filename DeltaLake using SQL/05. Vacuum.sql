-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### VACUUM Command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - VACUUM removes all files from the table directory that are not managed by Delta, as well as data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. 
-- MAGIC - VACUUM will skip all directories that begin with an underscore (_), which includes the _delta_log. Partitioning your table on a column that begins with an underscore is an exception to this rule; 
-- MAGIC - VACUUM scans all valid partitions included in the target Delta table. 
-- MAGIC - Delta table data files are deleted according to the time they have been logically removed from Delta’s transaction log plus retention hours, not their modification timestamps on the storage system. The default threshold is 7 days.

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

VACUUM users;

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

VACUUM users RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM users RETAIN 0 HOURS

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

SELECT * FROM users VERSION AS OF 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Drop the table

-- COMMAND ----------

DROP TABLE users

-- COMMAND ----------

--%fs ls 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------


