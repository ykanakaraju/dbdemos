-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Dataset relations**
-- MAGIC 1. customers.customer_id = orders.customer_id
-- MAGIC 1. orders.books = books.book_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - I made some changes here

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying JSON 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC #spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/customers-json/

-- COMMAND ----------

-- SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_001.json`
-- SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_*.json`
SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/`

-- COMMAND ----------

SELECT 
  _metadata.file_path AS source_file, COUNT(*) as row_count
FROM 
  JSON.`dbfs:/FileStore/data/bookstore/customers-json/`
GROUP BY 
  source_file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying text format

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/FileStore/data/bookstore/customers-json/export_002.json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryfile.`dbfs:/FileStore/data/bookstore/customers-json/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Querying CSV 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/books-csv

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/bookstore/books-csv/export_001.csv

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/FileStore/data/bookstore/books-csv/export_001.csv`

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) USING CSV
OPTIONS( header = "true", delimiter = ";" )
LOCATION 'dbfs:/FileStore/data/bookstore/books-csv'

-- COMMAND ----------

DESC EXTENDED books_csv

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Limitations of Non-Delta Tables

-- COMMAND ----------

UPDATE books_csv SET price = price + 10 WHERE book_id = 'B07'

-- COMMAND ----------

DELETE FROM books_csv WHERE book_id = 'B07'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/`

-- COMMAND ----------

DESC EXTENDED customers

-- COMMAND ----------

DROP TABLE IF EXISTS books;
DROP TABLE IF EXISTS books_csv;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) 
USING CSV
OPTIONS( 
  header = "true", 
  delimiter = ";" ,
  path = 'dbfs:/FileStore/data/bookstore/books-csv'
)

-- COMMAND ----------

CREATE TABLE books AS
SELECT * FROM books_tmp_vw

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

DESC EXTENDED books

-- COMMAND ----------


