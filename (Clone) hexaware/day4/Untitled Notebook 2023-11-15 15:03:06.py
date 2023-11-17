# Databricks notebook source
dbutils.fs.mount(source="wasbs://processeddata@paadlshexaware.blob.core.windows.net",
mount_point="/mnt/paadlshexaware/processeddata",
extra_configs={"fs.azure.account.key.paadlshexaware.blob.core.windows.net":"9c9vqba5gFh4tx7w/2QH7awlsaK9w9oSZ34629nm8iNjhpTim9JNA8HwwcJY2Knp3AaJmW0ovWEO+ASt/DpDLQ=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/paadlshexaware/processeddata/delta/anusha/

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history padelta.people20m

# COMMAND ----------


