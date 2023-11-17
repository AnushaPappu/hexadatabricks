# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount
# MAGIC Notebook
# MAGIC Cluster

# COMMAND ----------

dbutils.fs.mount(source="wasbs://testblobcontainer@blobadhex.blob.core.windows.net",
mount_point="/mnt/blobadhex/testblobcontainer",
extra_configs={"fs.azure.account.key.blobadhex.blob.core.windows.net":"q5I8uK1/Z4PL9+10sIPWik5gHWWliO8M4RO7iVYcf2uE+sJCXKTNd5tdcPa/U99JDmpkT7Zbphl+AStgc+3LQ=="})

# COMMAND ----------

dbutils.fs.mount(source="wasbs://testblobcontainer@blobadhex.blob.core.windows.net",
mount_point="/mnt/blobadhex/testblobcontainer",
extra_configs={"fs.azure.account.key.blobadhex.blob.core.windows.net":"q5I8uK1/Z4PL9+1OsIPWik5gHWWliO8M4RO7iVIYcf2uE+sJCXKTNd5tdcPa/U99JDmpkT7Zbphl+AStgc+3LQ=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/

# COMMAND ----------

dbutils.fs.mount(source="wasbs://processeddata@asadlsad.blob.core.windows.net",
mount_point="/mnt/asadlsad/processeddata",
extra_configs={"fs.azure.account.key.asadlsad.blob.core.windows.net":"CgjjI6wIHTDiuYdRjWwkN0akzNZMWLd5H1dfezAgt9/1QxEsBftLQvagg5iz3uh5+LkRQctEgYQK+ASt+jU+rA=="})

# COMMAND ----------

outputpath="dbfs:/mnt/asadlssad/processeddata/iotdata/"

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")
