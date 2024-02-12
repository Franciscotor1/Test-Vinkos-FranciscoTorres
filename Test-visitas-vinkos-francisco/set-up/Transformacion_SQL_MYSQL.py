# Databricks notebook source
# MAGIC %md
# MAGIC #### Limpieza y conversion en dataframes para guardarlos en un BASE DE DATOS MYSQL o en .SQL
# MAGIC 1. Cargar el archivo desde dbfs
# MAGIC 2. Ajustar tipos de datos y ajustar las columnas
# MAGIC 3. crear DATA FRAME limpio
# MAGIC 4. Pasar ese dataframe a .SQL en la ruta dbfs donde tenemos nuestro contenedor montado dbdemo_mount y que en Blobstore se llama dbmount
# MAGIC 5. hacer consultas SQL desde ese archivo guardado

# COMMAND ----------

# Ruta del archivo de texto
file_path = "/mnt/vinkostest1/demo/report_7.txt"

# Leer el archivo de texto en un DataFrame
df_read = spark.read.text(file_path)

# Imprimir el esquema del DataFrame
df_read.printSchema()

# Mostrar el contenido del DataFrame con display
display(df_read)


# COMMAND ----------

from pyspark.sql.functions import expr

# Definir la expresión para dividir la cadena en columnas utilizando ","
split_expr = "split(value, ',') as split_values"

# Aplicar la expresión para dividir la cadena en columnas
df_split = df_read.selectExpr(split_expr)

# Eliminar la primera fila que contiene los nombres de las columnas
df_data = df_split.filter("value NOT LIKE '%email%'")

# Asignar nombres a las columnas
column_names = ["email", "jk", "Badmail", "Baja", "Fecha envio", "Fecha open", "Opens", 
                "Opens virales", "Fecha click", "Clicks", "Clicks virales", "Links", 
                "IPs", "Navegadores", "Plataformas"]

# Crear columnas utilizando expr
for i, name in enumerate(column_names):
    df_data = df_data.withColumn(name, expr(f"split_values[{i}]"))

# Eliminar la columna "split_values" ya que ya no la necesitamos
df_data = df_data.drop("split_values")

# Convertir las columnas al tipo de dato adecuado
df_formatted = df_data.withColumn("Fecha envio", expr("to_timestamp(`Fecha envio`, 'dd/MM/yyyy HH:mm')")) \
                      .withColumn("Fecha open", expr("to_timestamp(`Fecha open`, 'dd/MM/yyyy HH:mm')")) \
                      .withColumn("Fecha click", expr("to_timestamp(`Fecha click`, 'dd/MM/yyyy HH:mm')")) \
                      .withColumn("Opens", expr("int(`Opens`)")) \
                      .withColumn("Opens virales", expr("int(`Opens virales`)")) \
                      .withColumn("Clicks", expr("int(`Clicks`)")) \
                      .withColumn("Clicks virales", expr("int(`Clicks virales`)"))

# Mostrar el DataFrame con el esquema actualizado
display(df_formatted)

# Imprimir el esquema del DataFrame
df_formatted.printSchema()


# COMMAND ----------

# Listar archivos en el directorio dbfs:/mnt/vinkostest1/dbdemo_mount
file_list = dbutils.fs.ls("dbfs:/mnt/vinkostest1/dbdemo_mount")

# Convertir la lista de archivos en un DataFrame
file_df = spark.createDataFrame(file_list)

# Mostrar el DataFrame utilizando display()
display(file_df)


# COMMAND ----------

# Definir la ruta para guardar el archivo Parquet
archivo_parquet_path = "/mnt/vinkostest1/dbdemo_mount/archivos_sql/visitasframe.parquet"

# Escribir el DataFrame como archivo Parquet
df_formatted.write.mode("overwrite").parquet(archivo_parquet_path)


# COMMAND ----------

# Convert the timestamp column to string
from pyspark.sql.functions import col

df_formatted = df_formatted.withColumn("Fecha envio", col("Fecha envio").cast("string"))

# Define the path to save the Parquet file
archivo_parquet_path = "/mnt/vinkostest1/dbdemo_mount/archivos_sql/visitasframe.parquet"

# Write the DataFrame as a Parquet file
df_formatted.write.mode("overwrite").parquet(archivo_parquet_path)

# Define the path to save the CSV file
archivo_csv_path = "/mnt/vinkostest1/dbdemo_mount/archivos_sql/visitasframe.csv"

# Write the DataFrame as a CSV file
df_formatted.write.mode("overwrite").csv(archivo_csv_path)

# COMMAND ----------

# Ruta del archivo Parquet
archivo_parquet_path = "/mnt/vinkostest1/dbdemo_mount/archivos_sql/visitasframe.parquet"

# Cargar el archivo Parquet en un DataFrame
df_parquet = spark.read.parquet(archivo_parquet_path)

# Mostrar el contenido del DataFrame
df_parquet.show()

# Mostrar el contenido del DataFrame utilizando display()
display(df_parquet)


# COMMAND ----------

# MAGIC %md
# MAGIC #### CREAMOS UNA TEMP VIEW para consultas en SQL

# COMMAND ----------

# Crear un tempView para consultar en SQL
df_parquet.createOrReplaceTempView("vinkosvisitas_temp_view")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vinkosvisitas_temp_view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vinkosvisitas_temp_view LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM vinkosvisitas_temp_view;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(Opens) AS promedio_opens, AVG(Clicks) AS promedio_clicks FROM vinkosvisitas_temp_view;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT email) AS num_correos_unicos FROM vinkosvisitas_temp_view;

# COMMAND ----------


