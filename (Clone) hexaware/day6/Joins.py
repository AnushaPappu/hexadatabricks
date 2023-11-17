# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/paadlshexaware/processeddata/

# COMMAND ----------

df_drivers=spark.read.json("dbfs:/mnt/paadlshexaware/processeddata/raw/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/paadlshexaware/processeddata/raw/pit_stops.json")

# COMMAND ----------

df_drivers=spark.read.json("dbfs:/mnt/paadlshexaware/bronze/raw/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/paadlshexaware/bronze/raw/pit_stops.json")

# COMMAND ----------

df_drivers.join(df_pitstop).display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId").display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId","left").select("driverId","code","lap").display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

df1.dropna("all",subset="id").display()

# COMMAND ----------

df1.fillna({"id":0,"name":"unknown","Marks":49,"placed":False}).display()

# COMMAND ----------


