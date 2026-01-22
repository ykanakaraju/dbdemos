# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

input_file = "/Volumes/workspace/default/data/flight-data/json/"
df1 = spark.read.json(input_file)

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **show** - Standard command in spark to print the content of a dataframe

# COMMAND ----------

#df1.show()
#df1.show(30)
#df1.show(30, False)
#df1.show(30, 30)
df1.show(3, False, True)

# COMMAND ----------

df1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **select**

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = df1.select('ORIGIN_COUNTRY_NAME', 'DEST_COUNTRY_NAME', 'count')
display(df2)

# COMMAND ----------

#col("age") + 1
#expr("age + 1")
#df1["DEST_COUNTRY_NAME"]
df1.DEST_COUNTRY_NAME

# COMMAND ----------

df2 = df1.select(
    col("ORIGIN_COUNTRY_NAME").alias("origin"),
    expr("DEST_COUNTRY_NAME").alias("destination"),
    col("count").cast("int"),
    expr("count + 10 as new_count"),
    expr("count > 200 as high_frequency"),
    expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as domestic"),
    current_date().alias("today"),
    lit('India').alias("country"),
    current_timestamp().alias("current_time")
)

display(df2)


# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **where / filter**

# COMMAND ----------

#df3 = df2.where("high_frequency = true and origin = 'United States'")
df3 = df2.filter("high_frequency = true and origin = 'United States'")
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **orderBy / sort**

# COMMAND ----------

#df3 = df2.orderBy("count", "origin")
#df3 = df2.sort("count", "origin")
df3 = df2.sort(desc("count"), asc("origin"))
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **groupBy**
# MAGIC
# MAGIC - Returns a `pyspark.sql.group.GroupedData` object (not a DataFrame)
# MAGIC - Apply aggregation methods to return a DataFrame

# COMMAND ----------

#df3 = df2.groupBy("high_frequency", "domestic").count()
#df3 = df2.groupBy("high_frequency", "domestic").sum("count")
#df3 = df2.groupBy("high_frequency", "domestic").max("count")
#df3 = df2.groupBy("high_frequency", "domestic").avg("count")

df3 = df2 \
      .groupBy("high_frequency", "domestic") \
      .agg(
        count("count").alias("count"),
        sum("count").alias("sum"),
        round(avg("count"), 1).alias("average"),
        max("count").alias("max")
      )

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **selectExpr**

# COMMAND ----------

df2 = df1.selectExpr(
    "ORIGIN_COUNTRY_NAME as origin",
    "DEST_COUNTRY_NAME as destination",
    "cast(count as int)",
    "count + 10 as new_count",
    "count > 200 as high_frequency",
    "DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as domestic",
    "current_date() as today",
    "'India' as country",
    "current_timestamp() as current_time"
)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **withColumn** & **withColumnRenamed**

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df3 = df1 \
    .withColumn("new_count", col("count") + 10) \
    .withColumn("high_frequency", col("count") > 200) \
    .withColumn("domestic", col("DEST_COUNTRY_NAME") == col("ORIGIN_COUNTRY_NAME")) \
    .withColumn("today", current_date()) \
    .withColumn("country", lit("India")) \
    .withColumn("count", col("count") * 100) \
    .withColumn("count", col("count").cast("int")) \
    .withColumnRenamed("DEST_COUNTRY_NAME", "destination") \
    .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin")

display(df3)

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (2, "Rajeev", 15),
             (3, "Rajan", 16),
             (4, "Raghu", 35),
             (5, "Raghav", 45),
             (6, "Ramu", 65),
             (7, "Ravi", 70)]

users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
display(users_df)

# COMMAND ----------

age_group_df = users_df.withColumn("age_group", when(col("age") < 13, "child")
                                             .when(col("age") < 20, "teenager")
                                             .when(col("age") < 60, "adult")
                                             .otherwise("senior"))
display(age_group_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **udf**
# MAGIC - WARNING: avoid using udf if it can be avoided

# COMMAND ----------

def get_age_group(age):
    if age < 13:
        return "child"
    elif age < 20:
        return "teenager"
    elif age < 60:
        return "adult"
    else:
        return "senior"

# COMMAND ----------

get_age_group_udf = udf(get_age_group, StringType())
age_group_df = users_df.withColumn("age_group", get_age_group_udf(col("age")))

display(age_group_df)

# COMMAND ----------

users_df.createOrReplaceTempView("users")

# COMMAND ----------

# MAGIC %sql
# MAGIC show functions

# COMMAND ----------

age_group = spark.udf.register("age_group", get_age_group, StringType())

# COMMAND ----------

age_group_df = users_df.withColumn("age_group", age_group(col("age")))
display(age_group_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, name, age, age_group(age) as age_group from users

# COMMAND ----------

# MAGIC %md
# MAGIC **drop**
# MAGIC - used to exclude columns in the output dataframe

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df3 = df2.drop("current_time", "country", "today", "new_count")
df3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **dropDuplicates**
# MAGIC
# MAGIC - drops duplicate rows/data

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (1, "Raju", 5),
             (3, "Raju", 5),
             (4, "Raghu", 35),
             (4, "Raghu", 35),
             (6, "Raghu", 35),
             (7, "Ravi", 70)]

users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
display(users_df)

# COMMAND ----------

#drop_dups_df = users_df.dropDuplicates()
drop_dups_df = users_df.dropDuplicates(["name", "age"])
display(drop_dups_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **distinct**

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (1, "Raju", 5),
             (3, "Raju", 5),
             (4, "Raghu", 35),
             (4, "Raghu", 35),
             (6, "Raghu", 35),
             (7, "Ravi", 70)]

users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
display(users_df)

# COMMAND ----------

distinct_df = users_df.distinct()
display(distinct_df)

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.count() - df1.distinct().count()

# COMMAND ----------

#df1.select("DEST_COUNTRY_NAME").distinct().count()
df1.dropDuplicates(["DEST_COUNTRY_NAME"]).count()

# COMMAND ----------

# MAGIC %md
# MAGIC **union, intersect, subtract**

# COMMAND ----------

def partitions(df):
    from pyspark.sql.functions import spark_partition_id
    df.withColumn('partition_id', spark_partition_id()).groupBy('partition_id').count().display()

# COMMAND ----------

df2 = df1.where("count > 200").repartition(4)
partitions(df2)

# COMMAND ----------

df3 = df1.where("DEST_COUNTRY_NAME = 'United States'").repartition(3)
partitions(df3)

# COMMAND ----------

df_union = df2.union(df3)
partitions(df_union)

# COMMAND ----------

df_intersect = df2.intersect(df3)
partitions(df_intersect)

# COMMAND ----------

display(df_intersect)

# COMMAND ----------

subtract_df = df2.subtract(df3)
partitions(subtract_df)

# COMMAND ----------

display(subtract_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **repartition**
# MAGIC - Is used to increase or decrease the number of partitions of the output DF
# MAGIC - Causes global shuffle

# COMMAND ----------

partitions(df1)

# COMMAND ----------

df3 = df1.repartition(10)
partitions(df3)

# COMMAND ----------

df4 = df3.repartition(4)
partitions(df4)

# COMMAND ----------

display(df1)

# COMMAND ----------

df5 = df1.repartition(5, col("DEST_COUNTRY_NAME"))
partitions(df5)

# COMMAND ----------

df6 = df5.repartition(5)
partitions(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC **coalesce**
# MAGIC - Is used to only decrease the number of partitions of the output DF
# MAGIC - Causes partition merging

# COMMAND ----------

partitions(df3)

# COMMAND ----------

df7 = df3.coalesce(3)
partitions(df7)

# COMMAND ----------

# MAGIC %md
# MAGIC **Window functions**

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/empdata.csv

# COMMAND ----------

data_file = "/Volumes/workspace/default/data/empdata.csv"

# COMMAND ----------

csv_schema = "id INT, name STRING, dept STRING, salary INT"

# COMMAND ----------

windows_df = spark.read.csv(data_file, schema=csv_schema)
display(windows_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

window_spec = Window.partitionBy("dept")

# COMMAND ----------

type(window_spec)

# COMMAND ----------

window_df_2 = windows_df \
                .withColumn("total_dept_salary", sum(col("salary")).over(window_spec)) \
                .withColumn("avg_dept_salary", round(avg(col("salary")).over(window_spec), 1)) \
                .withColumn("max_dept_salary", max(col("salary")).over(window_spec))

# COMMAND ----------

display(window_df_2)

# COMMAND ----------

window_spec_3 = Window \
    .partitionBy("dept") \
    .orderBy(col("salary")) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------

window_df_3 = windows_df \
                .withColumn("total_salary", sum(col("salary")).over(window_spec_3)) \
                .withColumn("avg_salary", round(avg(col("salary")).over(window_spec_3), 1)) \
                .withColumn("rank", rank().over(window_spec_3)) \
                .withColumn("drank", dense_rank().over(window_spec_3)) \
                .withColumn("row_num", row_number().over(window_spec_3))

# COMMAND ----------

display(window_df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get top 3 employees with highest salary in each department**

# COMMAND ----------

window_spec = Window.partitionBy("dept").orderBy(desc("salary"))

# COMMAND ----------

top_emp_df = windows_df.withColumn("row_num", row_number().over(window_spec)) \
                .where("row_num <= 3") \
                .drop("row_num")

# COMMAND ----------

display(top_emp_df)

# COMMAND ----------


