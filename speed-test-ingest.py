# Databricks notebook source
st_df = spark.read.json("dbfs:/FileStore/landing_zone/speed_test.json")

# COMMAND ----------

print(st_df)

# COMMAND ----------


st_df.write.format("Delta").saveAsTable("timeseries.speed_test")

# COMMAND ----------

st_df.write.format("delta").mode("overwrite").save("/FileStore/tables/TimeSeries/speed_test")

# COMMAND ----------

# MAGIC %sql select count(*) from timeseries.speed_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from timeseries.speed_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select client.country, client.ip, client.isp, client.lat, client.lon from timeseries.speed_test

# COMMAND ----------


