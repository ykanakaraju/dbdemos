-- Databricks notebook source
-- MAGIC %fs ls dbfs:/FileStore/data/schema

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE;
CREATE DATABASE demodb;
USE demodb;

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/schema/people.json

-- COMMAND ----------

CREATE OR REPLACE TABLE people (
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Schema Validations Summary
-- MAGIC - `INSERT INTO` AND `INSERT OVERWRITE`
-- MAGIC   - Column matching by position, New columns not allowed
-- MAGIC - `MERGE .. INSERT`
-- MAGIC   - Column matching by name, New columns ignored
-- MAGIC - `DataFrame Append`
-- MAGIC   - Column matching by name, New columns not allowed
-- MAGIC - `Data Type Mismatch`
-- MAGIC   - Not allowed in any case

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### INSERT INTO
-- MAGIC
-- MAGIC - Column matching by position
-- MAGIC - New columns not allowed

-- COMMAND ----------

DESC people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname, lname FROM JSON.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname, lname, dob 
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### INSERT OVERWRITE
-- MAGIC - Column matching by position
-- MAGIC - New columns not allowed

-- COMMAND ----------

INSERT OVERWRITE people
SELECT id, fname, lname, dob FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

DESC HISTORY people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### MERGE .. INSERT

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

SELECT id, fname firstName, lname lastName FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

-- DBTITLE 1,Column matching by position - not allowed
MERGE INTO people T
USING (SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people_2.json`) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

-- DBTITLE 1,Column matching by name - allowed
MERGE INTO people T
USING 
( SELECT id, fname firstName, lname lastName
  FROM json.`dbfs:/FileStore/data/schema/people_2.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,Column matching by name - new columns ignored
SELECT id, fname firstName, lname lastName, dob 
FROM json.`dbfs:/FileStore/data/schema/people_3.json`

-- COMMAND ----------

MERGE INTO people T
USING 
(   SELECT id, fname firstName, lname lastName, dob 
    FROM json.`dbfs:/FileStore/data/schema/people_3.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Dataframe append
-- MAGIC
-- MAGIC - Column matching by position not allowed
-- MAGIC - Column matching by name is allowed
-- MAGIC

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,Matching by position - not allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING"
-- MAGIC people_df =  spark.read.schema(people_schema).json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC display(people_df)

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

-- DBTITLE 1,Matching by name - allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING"
-- MAGIC
-- MAGIC people_df =  (
-- MAGIC   spark
-- MAGIC     .read
-- MAGIC     .schema(people_schema)
-- MAGIC     .json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC     .withColumnRenamed("fname", "firstName")
-- MAGIC     .withColumnRenamed("lname", "lastName")
-- MAGIC )
-- MAGIC display(people_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,New columns not allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING, dob DATE"
-- MAGIC
-- MAGIC people_df =  (
-- MAGIC   spark
-- MAGIC     .read
-- MAGIC     .schema(people_schema)
-- MAGIC     .json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC     .withColumnRenamed("fname", "firstName")
-- MAGIC     .withColumnRenamed("lname", "lastName")
-- MAGIC )
-- MAGIC
-- MAGIC display(people_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cleanup

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------


