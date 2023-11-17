-- Databricks notebook source
show catalogs

-- COMMAND ----------

use iotdata;
show tables

-- COMMAND ----------

create view tempabove25 as(select * from sample where temperature>25)

-- COMMAND ----------

create temp view tempabove25 as(select * from sample where temperature>25)

-- COMMAND ----------

show views

-- COMMAND ----------

create Global temp view tempabove25 as(select * from sample where temperature>25)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/paadlshexaware/bronze/fact/circuits.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("namestemp")

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("namesglobal")

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.namesglobal

-- COMMAND ----------


