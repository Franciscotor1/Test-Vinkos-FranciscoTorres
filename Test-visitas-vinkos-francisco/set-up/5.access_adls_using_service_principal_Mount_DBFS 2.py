# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess adls using service y montaje del DBFS para llamar a los contenedores del metodo medallion bronce, silver, gold.
# MAGIC 1. crear la funcion con mount_adls_container y los secrets
# MAGIC 2. establecer la configuracion de spark
# MAGIC 3. puentear el punto de montaje
# MAGIC 4. verificar si existe el montaje y si existe crear otro
# MAGIC 5. montar el contenedor de la cuenta de almacenamiento en este caso se sustituye demo y vinkostest1
# MAGIC 6. llamar a la funcion para ejecutar el contenedor
# MAGIC 7. listar el contenedor

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

mount_adls_container('vinkostest1', 'process')
display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls_container('vinkostest1', 'presentation')
display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls_container('vinkostest1', 'demo')
display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls_container('vinkostest1', 'dbdemo')
display(dbutils.fs.mounts())

# COMMAND ----------


