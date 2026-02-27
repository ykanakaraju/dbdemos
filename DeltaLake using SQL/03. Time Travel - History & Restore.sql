-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Time Travel 
-- MAGIC **Time Travel feature allows us to rollback a specific version of the table**

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

SELECT * FROM users VERSION AS OF 2

-- COMMAND ----------

SELECT * FROM users@v4

-- COMMAND ----------

SELECT * FROM users TIMESTAMP AS OF '2026-02-23 08:40:00'

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DELETE FROM users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

LIST 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

RESTORE TABLE users TO VERSION AS OF 7

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

RESTORE users TO TIMESTAMP AS OF '2026-02-23 08:40:00'

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------


