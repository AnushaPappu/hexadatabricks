# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/pit_stops.json

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/pit_stops.json")

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/FileStore/tables/formula1/pit_stops.json")

# COMMAND ----------

display(df)

# COMMAND ----------


