# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess azure data lake using service principal
# MAGIC 1. Register Azure AD Application / service principal
# MAGIC 2. Generate a secret / password for the application
# MAGIC 3. Set Spark confog with app / client id, directory/ tenant id & secret
# MAGIC 4. Assign role 'storage blob data contributor' to the data lake
# MAGIC
# MAGIC Accede al Azure Data Lake utilizando un principal de servicio.
# MAGIC Registra la Aplicación/Aplicación principal de Azure AD.
# MAGIC Genera una contraseña/secreto para la aplicación.
# MAGIC Configura Spark con la ID de la aplicación/cliente, la ID del directorio/inquilino y el secreto.
# MAGIC Asigna el rol 'contribuyente de datos de blob de almacenamiento' al Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope ='vinkostest1-scope', key = 'vinkostest1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'vinkostest1-scope', key = 'vinkostest1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'vinkostest1-scope', key = 'vinkostest1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vinkostest1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.vinkostest1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.vinkostest1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.vinkostest1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.vinkostest1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@vinkostest1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.text("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt"))

# COMMAND ----------


