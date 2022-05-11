# Databricks notebook source
df = spark.sql("select * from timeseries.sinwave")

# COMMAND ----------

import mlflow
logged_model = 'runs:/181485b3a92e486aa52e777d8c96a7cd/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# Predict on a Spark DataFrame.
columns = list(df.columns)
df.withColumn('predictions', loaded_model(*columns)).collect()

# COMMAND ----------


