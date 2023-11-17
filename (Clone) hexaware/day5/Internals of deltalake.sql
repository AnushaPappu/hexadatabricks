-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS padelta.people20m (
-- MAGIC   id INT,
-- MAGIC   firstName STRING,
-- MAGIC   middleName STRING,
-- MAGIC   lastName STRING,
-- MAGIC   gender STRING,
-- MAGIC   birthDate TIMESTAMP,
-- MAGIC   ssn STRING,
-- MAGIC   salary INT
-- MAGIC ) LOCATION 'dbfs:/mnt/paadlshexaware/processeddata/delta/anusha'

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/paadlshexaware/processeddata/delta/anusha/

-- COMMAND ----------

describe extended padelta.people20m

-- COMMAND ----------

insert into padelta.people20m values(1,'Virat','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

insert into padelta.people20m values(2,'Rohit','R','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

describe history padelta.people20m

-- COMMAND ----------

select * from padelta.people20m

-- COMMAND ----------

select * from padelta.people20m

-- COMMAND ----------

create table padelta.people20v3 as (select * from padelta.people20m)

-- COMMAND ----------

select * from padelta.people20m timestamp as of '2023-11-14T05:58:40Z'

-- COMMAND ----------

select * from padelta.people20m@v2

-- COMMAND ----------

insert into padelta.people20m values(3,'K','L','Rahul','M',2023-11-14,"150",2000)

-- COMMAND ----------

insert into padelta.people20m values(4,'Shubman','Gill','K','M',2023-11-14,"123",1500)

-- COMMAND ----------

SELECT * from padelta.people20m

-- COMMAND ----------

delete from padelta.people20m where id=4

-- COMMAND ----------

select * from padelta.people20m

-- COMMAND ----------

describe history padelta.people20m

-- COMMAND ----------

select * from padelta.people20m

-- COMMAND ----------

delete from padelta.people20m where id=1

-- COMMAND ----------

select * from padelta.people20m
