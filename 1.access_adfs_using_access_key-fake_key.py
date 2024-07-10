# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Access Keys
# MAGIC - Set the spark configuration
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv

# COMMAND ----------

spark.conf.set("fs.azure.account.key.dbprojectdl2072.dfs.core.windows.net",
               "AccessKey")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbprojectdl2072.dfs.core.windows.net/circuits.csv"))
