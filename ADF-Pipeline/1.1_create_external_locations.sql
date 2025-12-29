-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS bookstore_landing
URL "abfss://bookstore-landing@adfdatasa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `adfdatasa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS bookstore_bronze
URL "abfss://bookstore-bronze@adfdatasa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `adfdatasa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS bookstore_silver
URL "abfss://bookstore-silver@adfdatasa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `adfdatasa-cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS bookstore_gold
URL "abfss://bookstore-gold@adfdatasa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `adfdatasa-cred`);
