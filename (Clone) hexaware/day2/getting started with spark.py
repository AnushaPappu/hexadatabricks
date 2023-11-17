# Databricks notebook source
##Introduction to databricks

# COMMAND ----------

##spark:spark is an empty point to start driver program

# COMMAND ----------

users=[(1,'a',20),(2,'b',30)]

# COMMAND ----------

sampledf=spark.createDataFrame(users)

# COMMAND ----------

sampledf.show()

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

users=[(1,'a',30),(2,'b',32)]
users_schema_str= " id int, name string, age int"
df1=spark.createDataFrame(users,users_schema_str)
df1.display()
