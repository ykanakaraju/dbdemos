-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Delta Table Cloning
-- MAGIC
-- MAGIC - You can create a copy of an existing Delta Lake table on Databricks at a specific version using the **`clone`** command. 
-- MAGIC - Clones can be either **deep** or **shallow**.
-- MAGIC
-- MAGIC - A **deep clone** is a clone that **copies the source table data** to the clone target in addition to the metadata of the existing table.
-- MAGIC
-- MAGIC - A **shallow clone** is a clone that **does not copy the data files** to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create.
-- MAGIC
-- MAGIC - Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
-- MAGIC
-- MAGIC - A cloned table has an independent history from its source table. Time travel queries on a cloned table do not work with the same inputs as they work on its source table.
-- MAGIC

-- COMMAND ----------

DROP SCHEMA IF EXISTS demodb;
CREATE SCHEMA demodb;
USE demodb;

-- COMMAND ----------

SELECT current_schema()

-- COMMAND ----------

CREATE TABLE users_managed (id INT, name STRING, age INT, gender STRING);

-- COMMAND ----------

INSERT INTO users_managed 
VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");

-- COMMAND ----------

DESCRIBE EXTENDED users_managed

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_managed

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

UPDATE users_managed 
SET age = age + 1
WHERE gender = 'Male'

-- COMMAND ----------

DELETE FROM  users_managed WHERE id = 8

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

DESCRIBE HISTORY users_managed

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_managed

-- COMMAND ----------

DESC  DETAIL users_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Deep Clone
-- MAGIC
-- MAGIC - Deep clones do not depend on the source from which they were cloned, but are expensive to create because a deep clone copies the data as well as the metadata.

-- COMMAND ----------

CREATE TABLE users_cloned CLONE users_managed

-- COMMAND ----------

SELECT * FROM users_cloned

-- COMMAND ----------

DESCRIBE EXTENDED users_cloned

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_cloned

-- COMMAND ----------

DESCRIBE HISTORY users_cloned

-- COMMAND ----------

UPDATE users_cloned SET age = age + 1 WHERE age  > 35

-- COMMAND ----------

DESC HISTORY users_cloned

-- COMMAND ----------

-- Replace the target table if it already exists
-- CREATE OR REPLACE TABLE users_cloned CLONE users_managed; 

-- Dont do anything if the target table exists
-- CREATE TABLE IF NOT EXISTS users_cloned CLONE users_managed; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Shallow Clone
-- MAGIC
-- MAGIC - Shallow clones reference data files in the source directory. If you run vacuum on the source table, clients can no longer read the referenced data files and a FileNotFoundException is thrown. 

-- COMMAND ----------

CREATE TABLE users_clone_shallow SHALLOW CLONE users_managed;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED users_clone_shallow

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_clone_shallow

-- COMMAND ----------

SELECT * FROM users_clone_shallow

-- COMMAND ----------

DESC HISTORY users_clone_shallow

-- COMMAND ----------

SELECT * FROM users_managed VERSION AS OF 1;

-- COMMAND ----------

CREATE TABLE users_clone_shallow_v1 SHALLOW CLONE users_managed VERSION AS OF 1;

-- COMMAND ----------

SELECT * FROM users_clone_shallow_v1

-- COMMAND ----------


