-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create external tables from landing zone

-- COMMAND ----------

USE CATALOG movielens_dev;

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_landing.movies;

CREATE TABLE IF NOT EXISTS movielens_landing.movies
(
  movieId INT, 
  title STRING, 
  genres STRING
)
USING CSV
OPTIONS (
  header = "true",
  path = "s3://cts-databricks-demo-data/landing/movies.csv"
);

-- COMMAND ----------

SELECT * FROM movielens_landing.movies

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_landing.ratings;

CREATE TABLE IF NOT EXISTS movielens_landing.ratings
(
  userId INT, 
  movieId INT, 
  rating DOUBLE, 
  timestamp BIGINT
)
USING CSV
OPTIONS (
  header = "true",
  path = "s3://cts-databricks-demo-data/landing/ratings.csv"
);

-- COMMAND ----------

-- SELECT * FROM movielens_landing.ratings
