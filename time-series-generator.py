# Databricks notebook source
# MAGIC %pip install time-series-generator

# COMMAND ----------



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
    df.date = pd.date_range(start='1900-01-01', end='2022-05-01', freq='H')
    df.sin = 1 + np.sin(df.date.astype('int64') // 1e9 * (2 * np.pi / year))
    df.sin = (df.sin * 100).round(2)
    df.date = df.date.apply(lambda d: d.strftime('%Y-%m-%d'))
    return df

train_df = load_dataframe()

# COMMAND ----------

print(train_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema TimeSeries;

# COMMAND ----------

sparkDF=spark.createDataFrame(train_df) 
sparkDF.printSchema()
sparkDF.show()


# COMMAND ----------

sparkDF.write.format("delta").saveAsTable("TimeSeries.SinWave")

# COMMAND ----------


