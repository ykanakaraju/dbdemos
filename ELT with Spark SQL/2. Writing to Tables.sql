-- Databricks notebook source
-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC #spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Tables

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders

-- COMMAND ----------

SELECT * FROM PARQUET.`dbfs:/FileStore/data/bookstore/orders/`

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders/`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

DESC EXTENDED orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders/`

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- DBTITLE 1,OVERWRITE Operation
INSERT OVERWRITE TABLE orders
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders/`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Appending Data

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders-new
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,APPEND operation
INSERT INTO TABLE orders
SELECT * FROM PARQUET.`dbfs:/FileStore/data/bookstore/orders-new/`

-- COMMAND ----------

SELECT COUNT(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`dbfs:/FileStore/data/bookstore/customers-json-new`;

-- COMMAND ----------

-- DBTITLE 1,SOURCE
SELECT * FROM customers_updates;

-- COMMAND ----------

-- DBTITLE 1,TARGET
SELECT * FROM customers

-- COMMAND ----------

MERGE INTO customers AS T
USING customers_updates AS S
ON T.customer_id = S.customer_id
WHEN MATCHED AND T.email IS NULL AND S.email IS NOT NULL THEN 
  UPDATE SET email = S.email, updated = S.updated
WHEN NOT MATCHED THEN 
  INSERT *

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **An other example**

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/FileStore/data/bookstore/books-csv-new`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "dbfs:/FileStore/data/bookstore/books-csv-new",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

-- DBTITLE 1,SOURCE
SELECT * FROM books_updates WHERE category = "Computer Science"

-- COMMAND ----------

-- DBTITLE 1,TARGET
SELECT * FROM books

-- COMMAND ----------

MERGE INTO books AS T
USING books_updates AS S
ON T.book_id = S.book_id AND T.title = S.title
WHEN NOT MATCHED AND S.category = "Computer Science" THEN 
  INSERT *


-- COMMAND ----------

SELECT * FROM books
