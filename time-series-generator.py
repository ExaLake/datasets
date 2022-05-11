# Databricks notebook source
# MAGIC %pip install time-series-generator

# COMMAND ----------

# Load and preview dataset
import datetime
import pandas as pd
import numpy as np

day = 24 * 60 * 60
year = 365.2425 * day


def load_dataframe() -> pd.DataFrame:
    """ Create a time series x sin wave dataframe. """
    df = pd.DataFrame(columns=['date', 'sin'])
    df.date = pd.date_range(start='1900-01-01', end='2022-05-01', freq='0h05min')
    df.sin = 1 + np.sin(df.date.astype('int64') // 1e9 * (2 * np.pi / year))
    df.sin = (df.sin * 100).round(2)
    df.date = df.date.apply(lambda d: d.strftime('%Y-%m-%d'))
    return df

train_df = load_dataframe()

# COMMAND ----------

print(train_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists  TimeSeries;

# COMMAND ----------

sparkDF=spark.createDataFrame(train_df) 
sparkDF.printSchema()
sparkDF.show()


# COMMAND ----------

sparkDF.write.format("delta").saveAsTable("TimeSeries.SinWave")

# COMMAND ----------

sparkDF.write.format("delta").mode("overwrite").save("/tmp/delta/people30m")

# COMMAND ----------

# MAGIC %sql
# MAGIC use TimeSeries;
# MAGIC select count(*) from SinWave;

# COMMAND ----------

# MAGIC %sql
# MAGIC select EXTRACT(YEAR FROM date) as year, avg(sin-100) from sinwave
# MAGIC group by year
# MAGIC order by year asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select EXTRACT(YEAR FROM date) as year, count(EXTRACT(YEAR FROM date)) from sinwave
# MAGIC group by year
# MAGIC order by year asc

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.util.Random
# MAGIC val data = 1 to 10000 map(x =>  (Random.,
# MAGIC                                  Random.nextInt(10000000), 
# MAGIC                                       Random.nextInt(110)+Random.nextFloat(), 
# MAGIC                                       Random.nextInt(110)+Random.nextFloat(),
# MAGIC                                       Randowm.nextBoolean()))
# MAGIC 
# MAGIC sqlContext.createDataFrame(data).toDF("base_station_id", "volt_in", "volt_out", "port_failure").show(false) 

# COMMAND ----------


