-- Databricks notebook source
-- MAGIC %python
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DeltaTable.createOrReplace(spark)\
-- MAGIC     .tableName("padelta.employee")\
-- MAGIC     .addColumn("emp_id","int")\
-- MAGIC     .addColumn("emp_name","String")\
-- MAGIC     .addColumn("gender","String")\
-- MAGIC     .location("dbfs:/mnt/paadlshexaware/processeddata/delta/emp")\
-- MAGIC     .execute()

-- COMMAND ----------

select * from padelta.employee

-- COMMAND ----------

insert into padelta.employee values(1,"Sachin","M")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[(2,'Rohit',"M")]
-- MAGIC schema=StructType([StructField("emp_id",IntegerType()),
-- MAGIC                    StructField("emp_name",StringType()),
-- MAGIC                    StructField("gender",StringType()),
-- MAGIC ])
-- MAGIC (spark.createDataFrame(data,schema).write.mode("append").saveAsTable("padelta.employee"))

-- COMMAND ----------

select * from padelta.employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/paadlshexaware/processeddata/delta/emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=[(3,'virat',"M","Batsman")]
-- MAGIC schema=StructType([StructField("emp_id",IntegerType()),
-- MAGIC                    StructField("emp_name",StringType()),
-- MAGIC                    StructField("gender",StringType()),
-- MAGIC                    StructField("dept",StringType())
-- MAGIC ])
-- MAGIC (spark.createDataFrame(data,schema).write.mode("append").option("mergeSchema","true").saveAsTable("padelta.employee"))

-- COMMAND ----------

select * from padelta.employee
