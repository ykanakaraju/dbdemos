-- Databricks notebook source
USE CATALOG movielens_dev;

-- COMMAND ----------

-- SHOW SCHEMAS

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_gold.top20Movies;

CREATE TABLE IF NOT EXISTS movielens_gold.top20Movies
  SELECT 
    movieId, 
    title, 
    COUNT(rating) as totalRatings, 
    ROUND(AVG(rating),4) as averageRating
  FROM movielens_silver.movie_ratings
  GROUP BY movieId, title HAVING totalRatings > 50
  ORDER BY averageRating DESC
  LIMIT 20;

-- COMMAND ----------

-- SELECT * FROM movielens_gold.top20Movies

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_gold.moviesByGenre;

CREATE TABLE IF NOT EXISTS movielens_gold.moviesByGenre
AS
  SELECT 
    genre, 
    COUNT(rating) as totalRatings, 
    ROUND(AVG(rating), 4) as averageRatings
  FROM 
    movielens_silver.movie_ratings_genres
  GROUP BY genre
  ORDER BY totalRatings DESC;


-- COMMAND ----------

-- SELECT * FROM movielens_gold.moviesByGenre

-- COMMAND ----------


