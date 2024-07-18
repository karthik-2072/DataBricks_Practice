# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-id-1')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenant-id-1')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@dbprojectstorage1.dfs.core.windows.net/",
  mount_point = "/mnt/dbprojectstorage1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dbprojectstorage1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/dbprojectstorage1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())
