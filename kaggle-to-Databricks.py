# Databricks notebook source
# MAGIC %md
# MAGIC haber0322/twitter-dataset-of-facebook  

# COMMAND ----------

dbutils.widgets.text("kaggle_username", "alexandergauthier", "Kaggle user name")
dbutils.widgets.text("kaggle_key", "f1daff4cc05c9c7094ba0da07b961e4f", "Kaggle key")
dbutils.widgets.text("dataset_name", "", "Dataset Name")
dbutils.widgets.text("kaggle_user_ds", "", "User's Dataset Name")



# COMMAND ----------

# MAGIC %md ##Download list of files from Kaggle

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install kaggle

# COMMAND ----------

# MAGIC %md __> one way to do it, command line approach

# COMMAND ----------

import os
os.environ["KAGGLE_USERNAME"] = dbutils.widgets.get("kaggle_username")
os.environ["KAGGLE_KEY"]  = dbutils.widgets.get("kaggle_key")

kaggler_ds_name = "%s/%s" % (dbutils.widgets.get("kaggle_user_ds"), dbutils.widgets.get("dataset_name"))

print(kaggler_ds_name)

# COMMAND ----------

cmd="kaggle datasets download --force %s -p /dbfs/FileStore/tmp/%s" % (kaggler_ds_name, dbutils.widgets.get("dataset_name"))
stream = os.popen(cmd)
output = stream.read()
output

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/tmp

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

def init_on_kaggle(username, api_key):
    KAGGLE_CONFIG_DIR = os.path.join(os.path.expandvars('$HOME'), '.kaggle')
    os.makedirs(KAGGLE_CONFIG_DIR, exist_ok = True)
    api_dict = {"username":username, "key":api_key}
    with open(f"{KAGGLE_CONFIG_DIR}/kaggle.json", "w", encoding='utf-8') as f:
        json.dump(api_dict, f)
    cmd = f"chmod 600 {KAGGLE_CONFIG_DIR}/kaggle.json"
    output = subprocess.check_output(cmd.split(" "))
    output = output.decode(encoding='UTF-8')
    print(output)

# COMMAND ----------


