-- Databricks notebook source
USE CATALOG movielens_dev;

-- COMMAND ----------

-- SHOW SCHEMAS

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_silver.movie_ratings;

CREATE TABLE movielens_silver.movie_ratings
AS
  SELECT 
    r.movieId, 
    r.rating, 
    m.title
  FROM movielens_bronze.ratings r 
  JOIN movielens_bronze.movies m
  ON r.movieId = m.movieId;

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_silver.movie_ratings_genres;

CREATE TABLE movielens_silver.movie_ratings_genres
AS
  SELECT 
    r.movieId, 
    r.rating, 
    m.title, 
    explode(split(m.genres,"#")) as genre
  FROM movielens_bronze.ratings r 
  JOIN movielens_bronze.movies m
  ON r.movieId = m.movieId;

-- COMMAND ----------

-- SELECT * FROM movielens_silver.movie_ratings

-- COMMAND ----------

-- SELECT * FROM movielens_silver.movie_ratings_genres

-- COMMAND ----------


