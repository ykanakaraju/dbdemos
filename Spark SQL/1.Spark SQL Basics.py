# Databricks notebook source
inputData = "/Volumes/workspace/default/data/users.json"

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/users.json

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a DataFrame**

# COMMAND ----------

#df1 = spark.read.format("json").load(inputData)
#df1 = spark.read.load(inputData, format="json")
df1 = spark.read.json(inputData)

# COMMAND ----------

df1.collect()

# COMMAND ----------

df1.schema

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

## display is a Databricks command
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform the DataFrame using Transformations API**

# COMMAND ----------

df2 = df1 \
    .select("userid", "name", "gender", "age", "phone") \
    .where("age is not null and phone is not null") \
    .orderBy("gender", "age") \
    .groupBy("age").count() \
    .limit(6)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform the DataFrame using SQL API**

# COMMAND ----------

#spark.catalog.currentCatalog()
#spark.catalog.currentDatabase()
spark.catalog.listTables()

# COMMAND ----------

df1.createOrReplaceTempView("users")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

#df3 = spark.table("users")
qry = """select age, count(1) as count
        from users
        where age is not null and phone is not null
        group by age
        order by count"""

df3 = spark.sql(qry)

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Use SQL cell**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select age, count(1) as count
# MAGIC from users
# MAGIC where age is not null and phone is not null
# MAGIC group by age
# MAGIC order by count

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing the DataFrame to a target location**

# COMMAND ----------

df3.write

# COMMAND ----------

outputPath = "/Volumes/workspace/default/data/output/json"

#df3.write.format("json").save(outputPath)
#df3.write.save(outputPath, format="json")
df3.write.json(outputPath)

# COMMAND ----------

#df3.rdd.getNumPartitions()

# COMMAND ----------

display(dbutils.fs.ls(outputPath))

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/output/json/part-00000-tid-1708901214372185859-e8b399ab-d006-4961-b4cd-b9e4edee444f-237-1-c000.json

# COMMAND ----------

# MAGIC %md
# MAGIC **Save Modes**
# MAGIC 	
# MAGIC - Define what should happen when you are writing to an existing directory
# MAGIC   - `ErrorIfExists` (default)
# MAGIC   - `Ignore`
# MAGIC   - `Append`   (appends additional files to the existing directory)
# MAGIC   - `Overwrite` (overwrites old directory)

# COMMAND ----------

#df3.write.mode("ignore").json(outputPath)
#df3.write.json(outputPath, mode="ignore")
#df3.write.json(outputPath, mode="append")
df3.write.json(outputPath, mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(outputPath))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


