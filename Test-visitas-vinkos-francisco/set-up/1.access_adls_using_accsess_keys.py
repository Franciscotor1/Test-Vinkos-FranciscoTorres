# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess azure data lake using access keys
# MAGIC 1. Set para la spark config fs.azure.account.key
# MAGIC 2. Lista de archivos demo container
# MAGIC 3. Read data from reports_7.txt, etc

# COMMAND ----------

vinkostest1_account_key = dbutils.secrets.get(scope = 'vinkostest1-scope', key = 'vinkostest1-account-key')

# COMMAND ----------

#Conexion al data lake
spark.conf.set(
    "fs.azure.account.key.vinkostest1.dfs.core.windows.net",
    vinkostest1_account_key)


# COMMAND ----------

dbutils.fs.ls("abfss://demo@vinkostest1.dfs.core.windows.net")

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

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Definir el esquema del DataFrame
schema = StructType([
    StructField("email", StringType(), nullable=True),
    StructField("jk", StringType(), nullable=True),
    StructField("Badmail", StringType(), nullable=True),
    StructField("Baja", StringType(), nullable=True),
    StructField("Fecha_envio", StringType(), nullable=True),
    StructField("Fecha_open", StringType(), nullable=True),
    StructField("Opens", IntegerType(), nullable=True),
    StructField("Opens_virales", IntegerType(), nullable=True),
    StructField("Fecha_click", StringType(), nullable=True),
    StructField("Clicks", IntegerType(), nullable=True),
    StructField("Clicks_virales", IntegerType(), nullable=True),
    StructField("Links", StringType(), nullable=True),
    StructField("IPs", StringType(), nullable=True),
    StructField("Navegadores", StringType(), nullable=True),
    StructField("Plataformas", StringType(), nullable=True)
])

# Leer los datos desde el archivo TXT con el nuevo esquema
df = spark.read.csv("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.txt", schema=schema, header=False, sep=",")

# Escribir datos en formato Parquet
df.write.parquet("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.parquet")


# COMMAND ----------

# Leer los datos en formato Parquet
df_parquet = spark.read.parquet("abfss://demo@vinkostest1.dfs.core.windows.net/report_7.parquet")

# Mostrar los datos en forma de tabla utilizando display
display(df_parquet)


# COMMAND ----------


