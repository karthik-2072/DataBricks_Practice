# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Key Vault
# MAGIC - Set the spark configuration
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv

# COMMAND ----------

access_key=dbutils.secrets.get(scope='formula1-scope',key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.dbprojectdl2072.dfs.core.windows.net",access_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbprojectdl2072.dfs.core.windows.net/circuits.csv"))
