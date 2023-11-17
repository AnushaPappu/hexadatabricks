# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/anusha/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/anusha/checkpoint")
.option("path",f"{outputpath}/anusha/files")
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.firstauto

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table stream.firstauto

# COMMAND ----------

infering column types

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/anusha/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/anusha/checkpoint")
.option("path",f"{outputpath}/anusha/files")
.option("mergeSchema",True)
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.firstauto

# COMMAND ----------


