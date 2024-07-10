# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service principal
# MAGIC - Register Azure AD Application/Service Principal
# MAGIC - Generate Secret/Password for the application
# MAGIC - Set spark config with App/Client id,Directory/Tenant id & secret
# MAGIC - Assign Role 'Storage Blob Data Contributor' to the Data Lake
# MAGIC - Read data from circuits.csv

# COMMAND ----------

client_id="<client_id>"
tenant_id="<tenant_id"
client_secret="<client_secret>"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbprojectdl2072.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dbprojectdl2072.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbprojectdl2072.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dbprojectdl2072.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dbprojectdl2072.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbprojectdl2072.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbprojectdl2072.dfs.core.windows.net/circuits.csv"))
