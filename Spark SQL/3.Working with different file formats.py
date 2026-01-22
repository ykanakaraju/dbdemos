# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with different file formats
# MAGIC
# MAGIC [Documentation](https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html)

# COMMAND ----------

def partitions(df):
    from pyspark.sql.functions import spark_partition_id
    df.withColumn('partition_id', spark_partition_id()).groupBy('partition_id').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### JSON file format

# COMMAND ----------

#inputPath = "/Volumes/workspace/default/data/flight-data/json/2015-summary.json"
#inputPath = "/Volumes/workspace/default/data/flight-data/json/*-summary.json"
inputPath = "/Volumes/workspace/default/data/flight-data/json"
outputPath = "/Volumes/workspace/default/data/output/json"

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/flight-data/json

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/flight-data/json/2015-summary.json

# COMMAND ----------

json_df = spark.read.json(inputPath)
display(json_df)

# COMMAND ----------

partitions(json_df)

# COMMAND ----------

json_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply schema on the DF**

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

json_schema = StructType([
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("count", IntegerType(), True)
])

# COMMAND ----------

json_schema = "ORIGIN_COUNTRY_NAME STRING, DEST_COUNTRY_NAME STRING, count INT"

# COMMAND ----------

#json_df = spark.read.schema(json_schema).json(inputPath)
json_df = spark.read.json(inputPath, schema=json_schema)
display(json_df)

# COMMAND ----------

partitions(json_df)

# COMMAND ----------

json_df.write.json(outputPath, mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(outputPath))

# COMMAND ----------

# MAGIC %md
# MAGIC **Working with multi-line JSON file**

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/users_multiline.json

# COMMAND ----------

multi_line_json = "/Volumes/workspace/default/data/users_multiline.json"

# COMMAND ----------

multi_line_json_df = spark.read.json(multi_line_json, multiLine=True)
display(multi_line_json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading from nested JSON**

# COMMAND ----------

nested_json = "/Volumes/workspace/default/data/users-nested.json"

# COMMAND ----------

nested_json_df = spark.read.json(nested_json)
display(nested_json_df)

# COMMAND ----------

#nested_json_df_2 = nested_json_df.select("userid", "name", "address")
nested_json_df_2 = nested_json_df.select("userid", "name", "address.city", "address.state")
display(nested_json_df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet file format

# COMMAND ----------

parquet_file = "/Volumes/workspace/default/data/flight-data/parquet/2010-summary.parquet/"

# COMMAND ----------

parquet_df = spark.read.parquet(parquet_file)
display(parquet_df)

# COMMAND ----------

parquet_df_2 = parquet_df.where("count > 100")
display(parquet_df_2)

# COMMAND ----------

output_path_parquet = "/Volumes/workspace/default/data/output/parquet"

# COMMAND ----------

partitions(parquet_df_2)

# COMMAND ----------

#parquet_df_2.write.parquet(output_path_parquet)
parquet_df_2.write.parquet(output_path_parquet, compression="gzip", mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(output_path_parquet))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ORC file format

# COMMAND ----------

orc_file = "/Volumes/workspace/default/data/flight-data/orc/2010-summary.orc/"

# COMMAND ----------

orc_df = spark.read.orc(orc_file)
display(orc_df)

# COMMAND ----------

orc_df_2 = orc_df.where("count > 50")
display(orc_df_2)

# COMMAND ----------

partitions(orc_df_2)

# COMMAND ----------

output_path_orc = "/Volumes/workspace/default/data/output/orc"
orc_df_2.write.orc(output_path_orc)

# COMMAND ----------

display(dbutils.fs.ls(output_path_orc))

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV file format
# MAGIC - Represents a delimited text file

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/flight-data/csv/

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/flight-data/csv/2010-summary.csv

# COMMAND ----------

csv_path = "/Volumes/workspace/default/data/flight-data/csv/"

# COMMAND ----------

csv_schema = "destination STRING, origin STRING, count INT"

# COMMAND ----------

#csv_df = spark.read.csv(csv_path, header=True, inferSchema=True)
csv_df = spark.read.schema(csv_schema).csv(csv_path, header=True)
display(csv_df)

# COMMAND ----------

partitions(csv_df)

# COMMAND ----------

csv_df_2 = csv_df.where("count > 200")
display(csv_df_2)

# COMMAND ----------

partitions(csv_df_2)

# COMMAND ----------

output_path_csv = "/Volumes/workspace/default/data/output/csv"

csv_df_2.write.csv(output_path_csv, header=True, mode="overwrite", sep="\t")

# COMMAND ----------

display(dbutils.fs.ls(output_path_csv))

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/output/csv/part-00000-tid-7343662606439789457-99bc3623-88e3-4303-90cb-567a28c4d380-472-1-c000.csv

# COMMAND ----------

spark.read.csv(output_path_csv, header=True, sep="\t").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Text file format

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/wordcount.txt

# COMMAND ----------

text_path = "/Volumes/workspace/default/data/wordcount.txt"

# COMMAND ----------

text_df = spark.read.text(text_path)

wordcount_df = text_df \
                .select( explode(split("value", " ")).alias("word") ) \
                .groupBy("word").count()

wordcount_df.display()

# COMMAND ----------

output_path_text = "/Volumes/workspace/default/data/output/text"

wordcount_df = wordcount_df.select( concat(col("word"), lit(","), col("count")).alias("word_count") )
display(wordcount_df)

# COMMAND ----------

wordcount_df.write.text(output_path_text)

# COMMAND ----------

display(dbutils.fs.ls(output_path_text))

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/output/text/part-00000-tid-336422773716262199-5b783932-61e2-4ec1-a587-eb57a90c76c4-523-1-c000.txt

# COMMAND ----------


