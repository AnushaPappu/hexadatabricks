# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/inputstream/csv/
# MAGIC

# COMMAND ----------

inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"

# COMMAND ----------

from pyspark.sql.types import *
users_sch=StructType([StructField("Id",IntegerType()),
                      StructField("Name",StringType()),
                      StructField("Gender",StringType()),
                      StructField("Salary",IntegerType()),
                      StructField("Country",StringType()),
                      StructField("Date",StringType()),
])

# COMMAND ----------

df=spark.readStream.schema(users_sch).csv(f"{inputpath}")

# COMMAND ----------

df=spark.readStream.option("header",True).schema(users_sch).csv(f"{inputpath}")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema stream;

# COMMAND ----------

outputpath="dbfs:/mnt/asadlsad/processeddata/outputstream"

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/anusha/teststream/files").option("checkpointLocation",f"{outputpath}/anusha/teststream/checkpoint").toTable("stream.teststream")

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/anusha/teststream/files").option("checkpointLocation",f"{outputpath}/anusha/teststream/checkpoint").trigger(processingTime="1 minute").toTable("stream.teststream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.teststream

# COMMAND ----------

spark.streams.active

# COMMAND ----------

for i in spark.streams.active:
    i.stop()

# COMMAND ----------


