-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM smartphones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Stored Views

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

CREATE VIEW smartphones_year_2022_vw
AS 
SELECT * FROM smartphones WHERE year = 2022;

-- COMMAND ----------

SELECT * FROM smartphones_year_2022_vw

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Temporary Views

-- COMMAND ----------

CREATE TEMPORARY VIEW smartphones_brand_apple_vw
AS
SELECT * FROM smartphones WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM smartphones_brand_apple_vw

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Global Temporary Views

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW smartphone_brands_vw
AS
  SELECT brand, count(1) as count
  FROM smartphones
  GROUP BY brand;

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

SELECT * FROM global_temp.smartphone_brands_vw

-- COMMAND ----------

-- DROP VIEW global_temp.smartphone_brands_vw
DROP VIEW smartphones_year_2022_vw

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop Views

-- COMMAND ----------



-- COMMAND ----------


