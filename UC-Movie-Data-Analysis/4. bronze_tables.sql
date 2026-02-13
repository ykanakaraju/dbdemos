-- Databricks notebook source
USE CATALOG movielens_dev;

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_bronze.movies;

CREATE TABLE IF NOT EXISTS movielens_bronze.movies
AS
SELECT * FROM movielens_landing.movies;

-- COMMAND ----------

SELECT * FROM movielens_bronze.movies

-- COMMAND ----------

DROP TABLE IF EXISTS movielens_bronze.ratings;

CREATE TABLE IF NOT EXISTS movielens_bronze.ratings
AS
SELECT * FROM movielens_landing.ratings;

-- COMMAND ----------

SELECT * FROM movielens_bronze.ratings

-- COMMAND ----------


