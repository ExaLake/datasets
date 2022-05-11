# Databricks notebook source
import dbldatagen as dg
import pyspark.sql.functions as func

from pyspark.sql.types import IntegerType, FloatType, StringType
column_count = 8
data_rows = 1000 * 1000 * 100
df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                                  partitions=4)
                            .withIdOutput()
                            .withColumn("reading", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                        numColumns=column_count)
                            .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                            .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
                            .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                            .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                            .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
                            .withColumn("event_ts", "timestamp", begin="2010-01-01 01:00:00", end="2022-05-04 23:59:00", interval="1 minute", random=True)

                            )
                            
df = df_spec.build()
num_rows=df.count()                          

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("delta").saveAsTable("TimeSeries.dbl_dgen_2")

# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy("code4").save("/FileStore/tables/TimeSeries/dbl_dgen_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from timeseries.dbl_dgen_2
# MAGIC where code4 = 'a'
# MAGIC order by event_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from timeseries.dbl_dgen_2;

# COMMAND ----------

# MAGIC %sql optimize timeseries.dbl_dgen_2

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType, StringType

import dbldatagen as dg

shuffle_partitions_requested = 8
device_population = 100000
data_rows = 20 * 1000000
partitions_requested = 20

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = ['CN', 'US', 'FR', 'CA', 'IN', 'JM', 'IE', 'PK', 'GB', 'IL', 'AU', 'SG',
                 'ES', 'GE', 'MX', 'ET', 'SA', 'LB', 'NL']
country_weights = [1300, 1465, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83, 126, 109, 58, 8,
                   17]

manufacturers = ['Delta corp', 'Xyzzy Inc.', 'Lakehouse Ltd', 'Acme Corp', 'Embanks Devices']

lines = ['delta', 'xyzzy', 'lakehouse', 'gadget', 'droid']

testDataSpec = (dg.DataGenerator(spark, name="device_data_set", rows=data_rows,
                                 partitions=partitions_requested,
                                 randomSeedMethod='hash_fieldname')
                .withIdOutput()
                # we'll use hash of the base field to generate the ids to
                # avoid a simple incrementing sequence
                .withColumn("internal_device_id", LongType(), minValue=0x1000000000000,
                            uniqueValues=device_population, omit=True, baseColumnType="hash")

                # note for format strings, we must use "%lx" not "%x" as the
                # underlying value is a long
                .withColumn("device_id", StringType(), format="0x%013x",
                            baseColumn="internal_device_id")

                # the device / user attributes will be the same for the same device id
                # so lets use the internal device id as the base column for these attribute
                .withColumn("country", StringType(), values=country_codes,
                            weights=country_weights,
                            baseColumn="internal_device_id")
                .withColumn("manufacturer", StringType(), values=manufacturers,
                            baseColumn="internal_device_id")

                # use omit = True if you don't want a column to appear in the final output
                # but just want to use it as part of generation of another column
                .withColumn("line", StringType(), values=lines, baseColumn="manufacturer",
                            baseColumnType="hash", omit=True)
                .withColumn("model_ser", IntegerType(), minValue=1, maxValue=11,
                            baseColumn="device_id",
                            baseColumnType="hash", omit=True)

                .withColumn("model_line", StringType(), expr="concat(line, '#', model_ser)",
                            baseColumn=["line", "model_ser"])
                .withColumn("event_type", StringType(),
                            values=["activation", "deactivation", "plan change",
                                    "telecoms activity", "internet activity", "device error"],
                            random=True)
                .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00", interval="1 minute", random=True)

                )

dfTestData = testDataSpec.build()

display(dfTestData)

# COMMAND ----------

dfTestData.write.format("delta").saveAsTable("TimeSeries.dbl_test_data1")

# COMMAND ----------

dfTestData.write.format("delta").mode("overwrite").partitionBy("country").save("/FileStore/tables/TimeSeries/dbl_test_data1")

# COMMAND ----------

# MAGIC %sql select * from timeseries.dbl_test_data1;

# COMMAND ----------


