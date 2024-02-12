# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess azure data lake using service principal def mount_adls_container(storage_account_name, container_name):
# MAGIC 1. Register Azure AD Application / service principal
# MAGIC 2. Generate a secret / password for the application
# MAGIC 3. Set Spark confog with app / client id, directory/ tenant id & secret
# MAGIC 4. Assign role 'storage blob data contributor' to the data lake
# MAGIC
# MAGIC Accede al Azure Data Lake utilizando un principal de servicio.
# MAGIC Registra la Aplicación/Aplicación principal de Azure AD.
# MAGIC Genera un secreto/contraseña para la aplicación.
# MAGIC Configura Spark con la ID de la aplicación/cliente, la ID del directorio/inquilino y el secreto.
# MAGIC Asigna el rol 'contribuyente de datos de blob de almacenamiento' al Data Lake.
# MAGIC
# MAGIC Se crea una funcion para automatizar
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls_container(storage_account_name, container_name):
    # Obtener las credenciales del cliente de la aplicación desde los secretos
    client_id = dbutils.secrets.get(scope='vinkostest1-scope', key='vinkostest1-app-client-id')
    tenant_id = dbutils.secrets.get(scope='vinkostest1-scope', key='vinkostest1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope='vinkostest1-scope', key='vinkostest1-app-client-secret') 

    # Establecer las configuraciones de Spark
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    # Definir el punto de montaje
    mount_point = f"/mnt/vinkostest1/{container_name}_mount"

    # Verificar si el punto de montaje ya está en uso
    if mount_point in [mount.mountPoint for mount in dbutils.fs.mounts()]:
        print(f"El punto de montaje {mount_point} ya está en uso. Por favor, elige un nombre diferente.")
        return

    # Montar el contenedor de la cuenta de almacenamiento
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )

    print(f"Contenedor montado en {mount_point}")

# Llamar a la función para montar el contenedor
mount_adls_container("vinkostest1", "demo")

# Mostrar los puntos de montaje
display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls_container('vinkostest1', 'raw')
display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls_container('vinkostest1', 'presentation')
display(dbutils.fs.mounts())

# COMMAND ----------

client_id = dbutils.secrets.get(scope ='vinkostest1-scope', key = 'vinkostest1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'vinkostest1-scope', key = 'vinkostest1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'vinkostest1-scope', key = 'vinkostest1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source="abfss://demo@vinkostest1.dfs.core.windows.net/",
  mount_point="/mnt/vinkostest1/demo",
  extra_configs=configs)



# COMMAND ----------

display(dbutils.fs.ls("/mnt/vinkostest1/demo"))

# COMMAND ----------


