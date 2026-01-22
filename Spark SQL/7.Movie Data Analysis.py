# Databricks notebook source
# MAGIC %md
# MAGIC **Dataset: /Volumes/workspace/default/data/movielens**
# MAGIC
# MAGIC
# MAGIC - From **movies.csv** and **ratings.csv** datasets, **fetch the top 10 movies with highest average user-rating**
# MAGIC 	- Consider only those movies that are rated by atleast 50 users
# MAGIC 	- Data: `movieId`, `title`, `totalRatings`, `averageRating`
# MAGIC 	- Arrange the data in the DESC order of averageRating
# MAGIC 	- Round the averageRating to 4 decimal places
# MAGIC 	- Save the output as a single pipe-separated CSV file with header
# MAGIC 	- Use only DF transformation methods (not SQL)

# COMMAND ----------

movies_file = "/Volumes/workspace/default/data/movielens/movies.csv"
ratings_file = "/Volumes/workspace/default/data/movielens/ratings.csv"

# COMMAND ----------

movies_schema = "movieId INT, title STRING"
ratings_schema = "userId INT, movieId INT, rating DOUBLE"

# COMMAND ----------

movies_df = spark.read.csv(movies_file, header=True, schema=movies_schema)
ratings_df = spark.read.csv(ratings_file, header=True, schema=ratings_schema).drop("userId")

# COMMAND ----------

display(movies_df)

# COMMAND ----------

display(ratings_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

average_rating_df = (
    ratings_df
    .groupBy("movieId")
    .agg(
        count("rating").alias("totalRatings"),
        avg("rating").alias("averageRating")
    )
    .where("totalRatings >= 50")
    .orderBy(desc("averageRating"))
    .limit(10)
    .join(movies_df, "movieId")
    .select("movieId", "title", "totalRatings", "averageRating")
    .orderBy(desc("averageRating"))
    .withColumn("averageRating", round(col("averageRating"), 4))
    .coalesce(1)
)

display(average_rating_df)

# COMMAND ----------

output_path = "/Volumes/workspace/default/data/output/movielens"
average_rating_df.write.csv(output_path, header=True, sep="|")

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/output/movielens

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/output/movielens/part-00000-tid-2552675311100567182-c93420f7-0285-4aae-8109-a939a146d3a8-587-1-c000.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q2: Fetch top 5 genres with highest number of movies.
# MAGIC - Data: `genre`, `numberOfMovies`

# COMMAND ----------

movies_file = "/Volumes/workspace/default/data/movielens/movies.csv"
movies_schema = "movieId INT, title STRING, genres STRING"
movies_df = spark.read.csv(movies_file, header=True, schema=movies_schema)
display(movies_df)

# COMMAND ----------

top_genres_df = (
    movies_df
    .select("movieId", explode(split("genres", "#")).alias("genre"))
    .groupBy("genre").count()
    .sort(desc("count"))
    .limit(5)
)

display(top_genres_df)

# COMMAND ----------


