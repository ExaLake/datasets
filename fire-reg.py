# Databricks notebook source
# MAGIC %fs 
# MAGIC ls /FileStore/landing_zone

# COMMAND ----------

# MAGIC  %scala
# MAGIC  val df = spark.read.json("dbfs:/FileStore/landing_zone/firearm_regulations_in_the_u_s.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %scala 
# MAGIC val df = spark.read.format("json")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .load("dbfs:/FileStore/landing_zone/firearm_regulations_in_the_u_s.json")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/landing_zone/test.csv")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.saveAs()
