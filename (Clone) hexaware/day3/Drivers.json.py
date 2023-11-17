# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/drivers.json

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df_final=df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name","url")

# COMMAND ----------

display(df_final)
