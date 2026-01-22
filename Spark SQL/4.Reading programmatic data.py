# Databricks notebook source
# MAGIC %md
# MAGIC **Creating DF from programmatic data**

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (2, "Ramesh", 15),
             (3, "Rajesh", 18),
             (4, "Raghu", 35),
             (5, "Ramya", 25),
             (6, "Radhika", 35),
             (7, "Ravi", 70)]

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = "id int, name string, age int"

# COMMAND ----------

#users_df = spark.createDataFrame(listUsers)
#users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
#users_df = spark.createDataFrame(listUsers).toDF("id", "name", "age")
users_df = spark.createDataFrame(listUsers, schema=schema)


display(users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating DF from RDD**

# COMMAND ----------

users_rdd = sc.parallelize(listUsers)
users_df = users_rdd.toDF()
users_df = users_rdd.toDF(schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating RDD from DF**

# COMMAND ----------

users_df.rdd
