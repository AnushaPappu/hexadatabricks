# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/constructors.json

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("ingestiondate",current_date()).display()

# COMMAND ----------

df.withColumn("file_path",input_file_name()).display()

# COMMAND ----------


