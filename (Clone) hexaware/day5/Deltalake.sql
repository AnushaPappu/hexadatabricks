-- Databricks notebook source
-- MAGIC %python
-- MAGIC https://docs.delta.io/latest/delta-batch.html#create-a-table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##3 ways to create deltalake
-- MAGIC 1.sql
-- MAGIC 2.python
-- MAGIC 3.df

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create schema padelta;
-- MAGIC use padelta

-- COMMAND ----------

show schemas

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use padelta;
-- MAGIC CREATE TABLE IF NOT EXISTS pdelta.people10m (  id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT)

-- COMMAND ----------

select * from padelta.people10m

-- COMMAND ----------

describe extended padelta.people10m

-- COMMAND ----------

describe history padelta.people10m

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/paadlshexaware/

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS padelta.people20m (
-- MAGIC   id INT,
-- MAGIC   firstName STRING,
-- MAGIC   middleName STRING,
-- MAGIC   lastName STRING,
-- MAGIC   gender STRING,
-- MAGIC   birthDate TIMESTAMP,
-- MAGIC   ssn STRING,
-- MAGIC   salary INT
-- MAGIC ) LOCATION 'dbfs:/mnt/paadlshexaware/processeddata/delta/anusha'

-- COMMAND ----------

describe extended padelta.people20m

-- COMMAND ----------

describe history padelta.people20m

-- COMMAND ----------


