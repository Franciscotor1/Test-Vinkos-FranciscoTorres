# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess azure data lake using SAS Token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List file for demo container
# MAGIC 3. Read data from reports_7.txt, etc
# MAGIC
# MAGIC Acceder al Azure Data Lake utilizando un token SAS.
# MAGIC Configurar Spark para el token SAS.
# MAGIC Listar archivos para el contenedor de demostración.
# MAGIC Leer datos de reports_7.txt, etc.

# COMMAND ----------

vinkostest1_demo_sas_token = dbutils.secrets.get(scope= 'vinkostest1-scope', key= 'vinkostest1-sas-token-demo')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vinkostest1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.vinkostest1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.vinkostest1.dfs.core.windows.net", vinkostest1_demo_sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vinkostest1.dfs.core.windows.net"))



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vinkostest1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.text("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Definir el esquema del DataFrame
schema = StructType([
    StructField("email", StringType(), nullable=True),
    StructField("jk", StringType(), nullable=True),
    StructField("Badmail", StringType(), nullable=True),
    StructField("Baja", StringType(), nullable=True),
    StructField("Fecha_envio", TimestampType(), nullable=True),
    StructField("Fecha_open", TimestampType(), nullable=True),
    StructField("Opens", IntegerType(), nullable=True),
    StructField("Opens_virales", IntegerType(), nullable=True),
    StructField("Fecha_click", TimestampType(), nullable=True),
    StructField("Clicks", IntegerType(), nullable=True),
    StructField("Clicks_virales", IntegerType(), nullable=True)
])

# Leer el archivo desde Azure Data Lake Gen2 con el esquema definido
df = spark.read.csv("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt", schema=schema, header=False, sep=",")

# Mostrar el esquema del DataFrame
df.printSchema()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Definir el esquema del DataFrame
schema = StructType([
    StructField("email", StringType(), nullable=True),
    StructField("jk", StringType(), nullable=True),
    StructField("Badmail", StringType(), nullable=True),
    StructField("Baja", StringType(), nullable=True),
    StructField("Fecha_envio", TimestampType(), nullable=True),
    StructField("Fecha_open", TimestampType(), nullable=True),
    StructField("Opens", IntegerType(), nullable=True),
    StructField("Opens_virales", IntegerType(), nullable=True),
    StructField("Fecha_click", TimestampType(), nullable=True),
    StructField("Clicks", IntegerType(), nullable=True),
    StructField("Clicks_virales", IntegerType(), nullable=True)
])

# Leer el archivo desde Azure Data Lake Gen2 con el esquema definido
df = spark.read.csv("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt", schema=schema, header=False, sep=",")

# Mostrar el DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Definir el esquema del DataFrame
schema = StructType([
    StructField("email", StringType(), nullable=True),
    StructField("jk", StringType(), nullable=True),
    StructField("Badmail", StringType(), nullable=True),
    StructField("Baja", StringType(), nullable=True),
    StructField("Fecha_envio", TimestampType(), nullable=True),
    StructField("Fecha_open", TimestampType(), nullable=True),
    StructField("Opens", IntegerType(), nullable=True),
    StructField("Opens_virales", IntegerType(), nullable=True),
    StructField("Fecha_click", TimestampType(), nullable=True),
    StructField("Clicks", IntegerType(), nullable=True),
    StructField("Clicks_virales", IntegerType(), nullable=True)
])

# Leer el archivo desde Azure Data Lake Gen2 con el esquema definido
df = spark.read.csv("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt", schema=schema, header=False, sep=",")

# Mostrar el DataFrame usando display()
display(df)


# COMMAND ----------


