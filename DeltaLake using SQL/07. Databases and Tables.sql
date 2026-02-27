-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Managed Tables

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

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users_managed

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

-- MAGIC %fs rm -r /FileStore/external/users_external

-- COMMAND ----------

CREATE TABLE users_external ( id INT, name STRING, age INT, gender STRING)
LOCATION '/FileStore/external/users_external'

-- COMMAND ----------

INSERT INTO users_external VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");

-- COMMAND ----------

DESC EXTENDED users_external

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/external/users_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Drop the managed table & external table

-- COMMAND ----------

DROP TABLE users_managed

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE users_external

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM users_external

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/external/users_external

-- COMMAND ----------

CREATE TABLE users_external ( id INT, name STRING, age INT, gender STRING)
LOCATION '/FileStore/external/users_external'

-- COMMAND ----------

SELECT * FROM users_external

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE users_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Schemas/Databases

-- COMMAND ----------

DROP SCHEMA IF EXISTS demodb CASCADE;
CREATE SCHEMA demodb;
-- CREATE DATABASE demodb

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESC DATABASE EXTENDED demodb

-- COMMAND ----------

USE demodb;
SELECT current_database();


-- COMMAND ----------

CREATE TABLE users_managed ( id INT, name STRING, age INT, gender STRING); 

INSERT INTO users_managed VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");


CREATE TABLE users_external ( id INT, name STRING, age INT, gender STRING)
LOCATION '/FileStore/external/users_external';

INSERT INTO users_external VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");


-- COMMAND ----------

SHOW TABLES IN demodb;

-- COMMAND ----------

DESC EXTENDED users_external

-- COMMAND ----------

DROP TABLE users_managed;
DROP TABLE users_external;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/external/users_external

-- COMMAND ----------

-- MAGIC %fs rm -r /FileStore/external/users_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop the schema

-- COMMAND ----------

DROP SCHEMA demodb
-- DROP SCHEMA demodb CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating schemas in eternal location

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/FileStore/schemas/demodb.db

-- COMMAND ----------

-- create a schema @ 'dbfs:/FileStore/schemas/demodb.db'
CREATE SCHEMA demodb 
LOCATION 'dbfs:/FileStore/schemas/demodb.db'

-- COMMAND ----------

DESC DATABASE EXTENDED demodb

-- COMMAND ----------

-- MAGIC %fs rm -r /FileStore/external/users_external

-- COMMAND ----------

USE demodb;

CREATE TABLE users_managed ( id INT, name STRING, age INT, gender STRING); 

INSERT INTO users_managed VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");


CREATE TABLE users_external ( id INT, name STRING, age INT, gender STRING)
LOCATION '/FileStore/external/users_external';

INSERT INTO users_external VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED users_managed

-- COMMAND ----------

DESC EXTENDED users_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleanup

-- COMMAND ----------

DROP SCHEMA demodb CASCADE

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/FileStore/external

-- COMMAND ----------


