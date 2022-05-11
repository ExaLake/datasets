# Databricks notebook source
# MAGIC %pip install speedtest

# COMMAND ----------

# MAGIC %pip install ping3  # install ping

# COMMAND ----------

verbose_ping('8.8.8.8')  # Returns delay in seconds.




# COMMAND ----------

from ping3 import ping, verbose_ping
ping3.DEBUG = True  # Default is False.

# COMMAND ----------


