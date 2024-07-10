# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Cluster Scoped Authentication
# MAGIC - Set the spark configuration fs.azure.account.key.dbprojectdl2072.dfs.core.windows.net at the cluster
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbprojectdl2072.dfs.core.windows.net/circuits.csv"))
