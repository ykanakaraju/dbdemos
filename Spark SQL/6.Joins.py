# Databricks notebook source
# MAGIC %md
# MAGIC #### Joins
# MAGIC
# MAGIC **Supported Joins**
# MAGIC - inner join
# MAGIC - outer joins
# MAGIC   - left
# MAGIC   - right
# MAGIC   - full
# MAGIC - semi join (left-semi join)
# MAGIC - anti join (left-anti join)
# MAGIC - cross join

# COMMAND ----------

employee = spark.createDataFrame([
    (1, "Raju", 25, 101),
    (2, "Ramesh", 26, 101),
    (3, "Amrita", 30, 102),
    (4, "Madhu", 32, 102),
    (5, "Aditya", 28, 102),
    (6, "Vinay", 42, 103),
    (7, "Smita", 27, 103),
    (8, "Vinod", 28, 100)])\
  .toDF("id", "name", "age", "deptid")
  
department = spark.createDataFrame([
    (101, "IT", 1),
    (102, "ITES", 2),
    (103, "Opearation", 3),
    (104, "HRD", 4)])\
  .toDF("id", "deptname", "locationid")
  
location = spark.createDataFrame([
    (1, 'Hyderabad'),
    (2, 'Chennai'),
    (3, 'Bengalure'),
    (4, 'Pune')])\
  .toDF("locationid", "location")

# COMMAND ----------

display(employee)

# COMMAND ----------

display(department)

# COMMAND ----------

display(location)

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %md
# MAGIC **Using PySpark SQL method**

# COMMAND ----------

employee.createOrReplaceTempView("emp")
department.createOrReplaceTempView("dept")
location.createOrReplaceTempView("loc")

# COMMAND ----------

df1 = spark.sql("""select emp.*, dept.deptname, loc.*
         from emp 
         join dept on emp.deptid = dept.id
         join loc on dept.locationid = loc.locationid""")
         
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*, dept.deptname, loc.*
# MAGIC from emp 
# MAGIC join dept on emp.deptid = dept.id
# MAGIC join loc on dept.locationid = loc.locationid

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*, dept.*
# MAGIC from emp 
# MAGIC   left join dept on emp.deptid = dept.id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*, dept.*
# MAGIC from emp 
# MAGIC   right join dept on emp.deptid = dept.id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*, dept.*
# MAGIC from emp 
# MAGIC   full join dept on emp.deptid = dept.id

# COMMAND ----------

# MAGIC %md
# MAGIC **Semi Join (Left Semi Join)**
# MAGIC - Is like inner join, but you get data only from left table
# MAGIC - Equivalent to following subquery
# MAGIC   - select * from emp where deptid in (select id from dept)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*
# MAGIC from emp semi join dept on emp.deptid = dept.id

# COMMAND ----------

# MAGIC %md
# MAGIC **Anti Join (Left anti Join)**
# MAGIC - Equivalent to following subquery
# MAGIC   - select * from emp where deptid not in (select id from dept)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select emp.*
# MAGIC from emp anti join dept on emp.deptid = dept.id

# COMMAND ----------

# MAGIC %md
# MAGIC **cross join**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp join dept where emp.id < 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Using 'join' Transformation method**
# MAGIC

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.catalog.dropTempView("_sqldf")

# COMMAND ----------

c = employee.deptid == department.id
type(c)

# COMMAND ----------

joined_df = employee \
            .join(department, employee.deptid == department.id) \
            .join(location, department.locationid == location.locationid)

display(joined_df)

# COMMAND ----------

joined_df = employee.join(department, employee.deptid == department.id, "left")
display(joined_df)

# COMMAND ----------

joined_df = employee.join(department, employee.deptid == department.id, "right")
display(joined_df)

# COMMAND ----------

joined_df = employee.join(department, employee.deptid == department.id, "full")
display(joined_df)

# COMMAND ----------

joined_df = employee.join(department, employee.deptid == department.id, "semi")
display(joined_df)

# COMMAND ----------

joined_df = employee.join(department, employee.deptid == department.id, "anti")
display(joined_df)

# COMMAND ----------

joined_df = employee.join(department)
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Natural** join

# COMMAND ----------

joined_df = department \
            .join(location, department.locationid == location.locationid) \
            .drop(location.locationid)

display(joined_df)

# COMMAND ----------

joined_df = department.join(location, "locationid")
#joined_df = department.join(location, ["locationid"])

display(joined_df)

# COMMAND ----------

joined_df =  employee.join(department.withColumnRenamed("id", "deptid"), "deptid")
display(joined_df)

# COMMAND ----------


