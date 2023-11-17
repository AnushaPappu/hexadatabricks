# Databricks notebook source
# MAGIC
# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").show()

# COMMAND ----------


