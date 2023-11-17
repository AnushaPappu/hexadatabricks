# Databricks notebook source
df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/formula1/circuits.csv")
df.select("circuitId","circuitRef","name").display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("circuitId").alias("circuit_Id")).display()

df.select(col("circuitId"),"circuitRef",df.name,df["location"]).display()

# COMMAND ----------

df.select(concat("location","country").alias("new column")).display()

# COMMAND ----------

df.select('*',concat("location",lit(" "),"country").alias("new column")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.columns

# COMMAND ----------

newcolumns=['circuit_id','circuit_ref','firstname','location','country','lat','lng','alt','url']

# COMMAND ----------

df.toDF(newcolumns)
df.toDF(*newcolumns).display()

# COMMAND ----------

df.withColumn("Date",current_timestamp()).display()

# COMMAND ----------

df.where("circuitId==1").display()

# COMMAND ----------

df.filter("circuitId>10 and country=='UK'").display()

# COMMAND ----------

dataframe functions
df.select()
df.columns
df.withColumnRenamed()
df.withColumn()
df.filter()/df.whereduce

functions
concat
lit
current_date
current_timestamp

# COMMAND ----------

df.orderBy(col("circuitId").desc()).display()

# COMMAND ----------

df.orderBy(desc("circuitId")).display()

# COMMAND ----------

df.sort("country").select("country","location").display()

# COMMAND ----------

df.drop("url","alt").display()

# COMMAND ----------


