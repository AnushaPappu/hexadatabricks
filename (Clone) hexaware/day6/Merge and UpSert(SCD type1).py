# Databricks notebook source
SCD Type1-If address column got changed,the column can be overwritten 
We will lost all the history

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee(
# MAGIC   employee_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   salary int,
# MAGIC   nationality string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source
# MAGIC ON target.employee_id = source.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC    target.first_name=source.first_name,
# MAGIC    target.last_name=source.last_name,
# MAGIC    target.salary=source.salary,
# MAGIC   target.nationality=source.nationality
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK")]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------



# COMMAND ----------

SCD Type2-We are getting new address, we willmake previous record inactive and we will add new address
Maintains the history and have duplicate  records

# COMMAND ----------

employees2 = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df1 = spark. \
    createDataFrame(employees2,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING,
                    start_date STRING, end_date STRING
                    """
                   )
display(df1)

# COMMAND ----------

df1.createOrReplaceTempView("source_view1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee2(
# MAGIC   employee_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   salary int,
# MAGIC   nationality string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source
# MAGIC ON target.employee_id = source.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC    target.first_name=source.first_name,
# MAGIC    target.last_name=source.last_name,
# MAGIC    target.salary=source.salary,
# MAGIC   target.nationality=source.nationality
# MAGIC   target.start_date=source.start_date
# MAGIC   target.end_date=source.end_date
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     start_date,
# MAGIC     end_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     start_date,
# MAGIC     end_date
# MAGIC   )
