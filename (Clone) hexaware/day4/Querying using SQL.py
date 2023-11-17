# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/raw/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists iotdata;
# MAGIC use iotdata

# COMMAND ----------

# MAGIC %sql
# MAGIC create table iotdata.sample as
# MAGIC (select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/blobadhex/testblobcontainer/circuits.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/blobadhex/testblobcontainer/drivers.json`
