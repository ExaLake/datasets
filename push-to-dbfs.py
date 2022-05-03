# Databricks notebook source
dbutils.widgets.text("kaggle_username", "alexandergauthier", "Kaggle user name")
dbutils.widgets.text("kaggle_key", "f1daff4cc05c9c7094ba0da07b961e4f", "Kaggle key")


# COMMAND ----------

# MAGIC %md ##Download list of files from Kaggle

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install kaggle

# COMMAND ----------

import os
os.environ["KAGGLE_USERNAME"] = dbutils.widgets.get("kaggle_username")
os.environ["KAGGLE_KEY"]  = dbutils.widgets.get("kaggle_key")

kaggle_ds_user = "muratkokludataset"
kaggle_ds = "date-fruit-datasets"

# COMMAND ----------

cmd="kaggle datasets download --force  %s/%s -p /tmp/%s" % (kaggle_ds_user,kaggle_ds,kaggle_ds)
stream = os.popen(cmd)
output = stream.read()
output

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp

# COMMAND ----------

cmd = "unzip /tmp/%s.zip -d /dbfs/FileStore/tmp/%s" % (kaggle_ds,kaggle_ds)
os.popen(cmd)
output = stream.read()
output

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/date-fruit-datasets
# MAGIC whoami

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tmp/Date_Fruit_Datasets

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/tmp/Date_Fruit_Datasets/Date_Fruit_Datasets.xlsx

# COMMAND ----------

dbutils.fs.fsutils.head("/FileStore/tmp/Date_Fruit_Datasets/Date_Fruit_Datasets_Citation_Request.txt")

# COMMAND ----------

dbutils.fs.
