# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Key Vault(SAS token)
# MAGIC - Set the spark configuration for sas token
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv

# COMMAND ----------

sas_token=dbutils.secrets.get(scope='formula1-scope',key='formula1-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbprojectdl2072.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbprojectdl2072.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dbprojectdl2072.dfs.core.windows.net",sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbprojectdl2072.dfs.core.windows.net/circuits.csv"))
