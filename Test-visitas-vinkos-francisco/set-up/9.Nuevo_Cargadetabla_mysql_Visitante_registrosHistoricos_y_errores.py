# Databricks notebook source
# MAGIC %md
# MAGIC ##### El siguiente notebook guarda registro en el contenedor raw del blob storage de Azure data lake gen2
# MAGIC #### Carga y limpieza de Tabla visitante y Tabla Errores, para convertir en SQL. Como extra se agrega Manejo de registros historicos por json y manejo de rrores datados en csv
# MAGIC 1. Cargar la ruta del montaje demo y extraer el report_7
# MAGIC 2. hacer limpieza de datos acomodando email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual.
# MAGIC con los Email correcto  y los timestamp en Fechas en formato dd/mm/yyyy HH:mm
# MAGIC 3. Filtrar y calcular los siguientes datos y cargarlos en nuevo Data Frame: email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual
# MAGIC 4. Crear conversiones en temp view SQL y guardar archivo en tabla SQL
# MAGIC 5. Registros historicos por json y manejo de errores registrados en csv
# MAGIC 6. Tabla errores en temp view MySQL y registro sql guardado
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Cargar la ruta del montaje demo y extraer el report_7

# COMMAND ----------

# MAGIC %md
# MAGIC #### 

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

# MAGIC %md
# MAGIC #### 2. hacer limpieza de datos acomodando email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual.

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

# Imprimir el esquema del DataFrame
df_formatted.printSchema()
df_formatted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Filtrar y calcular los siguientes datos y cargarlos en un nuevo Data Frame: email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual

# COMMAND ----------

from pyspark.sql.functions import col, year, month, date_format, count, when

# Filtrar registros con campos relevantes no nulos
df_cleaned = df_formatted.filter(col("email").isNotNull() & col("Fecha open").isNotNull())

# Calcular la fecha de la primera visita y la fecha de la última visita por email
df_dates = df_cleaned.groupBy("email") \
    .agg(
        {"Fecha open": "min", "Opens": "sum"}
    ) \
    .withColumnRenamed("min(Fecha open)", "fechaPrimeraVisita") \
    .withColumnRenamed("sum(Opens)", "visitasTotales") \
    .withColumn("fechaUltimaVisita", date_format(col("fechaPrimeraVisita"), "yyyy-MM-dd HH:mm:ss"))  # Placeholder, se actualiza más adelante

# Calcular el año y el mes de la fecha de la última visita
df_dates = df_dates.withColumn("anioUltimaVisita", year(col("fechaUltimaVisita"))) \
                   .withColumn("mesUltimaVisita", month(col("fechaUltimaVisita")))

# Filtrar para obtener solo visitas del año actual
current_year = df_dates.selectExpr("YEAR(CURRENT_DATE()) as current_year").collect()[0][0]
df_current_year = df_cleaned.filter(year(col("Fecha open")) == current_year)

# Calcular visitas totales en el año actual
df_current_year_count = df_current_year.groupBy("email").agg(count("*").alias("visitasAnioActual"))

# Unir los DataFrames
df_result = df_dates.join(df_current_year_count, "email", "left_outer") \
                   .select("email", "fechaPrimeraVisita", "fechaUltimaVisita", "visitasTotales", "visitasAnioActual")

# Calcular visitas en el mes actual
current_month = df_result.selectExpr("MONTH(CURRENT_DATE()) as current_month").collect()[0][0]
df_current_month = df_cleaned.filter(month(col("Fecha open")) == current_month) \
                              .groupBy("email") \
                              .agg(count("*").alias("visitasMesActual"))

# Unir los DataFrames y llenar con ceros si no hay visitas en el mes actual
df_result = df_result.join(df_current_month, "email", "left_outer") \
                     .fillna(0, subset=["visitasMesActual"])

# Mostrar el resultado
df_result.show()


# COMMAND ----------

# MAGIC %md #### 4. hacer conversiones en temp view SQL y cargar archivo en tabla SQL

# COMMAND ----------

# Listar archivos en el directorio dbfs:/mnt/vinkostest1/raw_mount/
file_list = dbutils.fs.ls("dbfs:/mnt/vinkostest1/raw_mount/")

# Convertir la lista de archivos en un DataFrame
file_df = spark.createDataFrame(file_list)

# Mostrar el DataFrame utilizando display()
display(file_df)


# COMMAND ----------

# Guardar el DataFrame como archivo Delta en la carpeta seleccionada
delta_path = "/mnt/vinkostest1/raw_mount/11-02-2024/visitante_delta"
df_result.write.format("delta").mode("overwrite").save(delta_path)

# Leer los datos Delta en un DataFrame
df_delta = spark.read.format("delta").load(delta_path)

# Crear una vista temporal para el DataFrame
df_delta.createOrReplaceTempView("visitante")

# Guardar el DataFrame como un archivo Parquet
parquet_path = "/mnt/vinkostest1/raw_mount/11-02-2024/visitante.parquet"
df_delta.write.mode("overwrite").parquet(parquet_path)

print("El archivo Delta se ha guardado correctamente en la carpeta seleccionada.")
print("El archivo Parquet se ha guardado correctamente en la carpeta seleccionada.")


# COMMAND ----------

# Cargar la tabla Delta en un DataFrame
df_delta = spark.read.format("delta").table("visitante")

# Crear una vista temporal para el DataFrame
df_delta.createOrReplaceTempView("visitante_tempview")

# Ejecutar consultas SQL en la vista temporal
spark.sql("SELECT * FROM visitante_tempview").show()


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Convertir DataFrame a SQL") \
    .getOrCreate()


# Crear una vista temporal del DataFrame
df_result.createOrReplaceTempView("temp_view")

# Ejecutar una consulta SQL para obtener los datos de la vista
sql_query = "SELECT * FROM temp_view"
sql_result = spark.sql(sql_query)

# Convertir el resultado a un formato SQL y guardarlo en un archivo
sql_output = sql_result.collect()
sql_lines = [str(row) for row in sql_output]

# Definir la ruta del archivo SQL
sql_file_path = "/mnt/vinkostest1/raw_mount/13-02-2024/custom_output.sql"

# Guardar los datos en el archivo SQL
with open(sql_file_path, "w") as f:
    for line in sql_lines:
        f.write(line + "\n")

print(f"Los datos se han guardado en el archivo SQL en: {sql_file_path}")


# COMMAND ----------

# Crear una vista temporal del DataFrame
df_result.createOrReplaceTempView("temp_view")


# COMMAND ----------

# Ejecutar una consulta SQL para seleccionar algunas filas de la vista temporal
sample_sql_query = "SELECT * FROM temp_view LIMIT 5"
sample_sql_result = spark.sql(sample_sql_query)

# Mostrar los resultados de la consulta
sample_sql_result.show()


# COMMAND ----------

# Ejecutar una consulta SQL para seleccionar algunas filas de la vista temporal
sample_sql_query = "SELECT * FROM temp_view LIMIT 5"

# Definir la ruta del archivo SQL
sql_file_path = "/mnt/vinkostest1/raw_mount/13-02-2024/sample_output.sql"

# Guardar la consulta SQL en un archivo
with open(sql_file_path, "w") as f:
    f.write(sample_sql_query)

print(f"La consulta SQL se ha guardado en el archivo SQL en: {sql_file_path}")


# COMMAND ----------

# Listar archivos en el directorio dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/
file_list = dbutils.fs.ls("dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/")

# Convertir la lista de archivos en un DataFrame
file_df = spark.createDataFrame(file_list)

# Mostrar el DataFrame utilizando display() 11-02-2024/visitante_output.sql/
display(file_df)

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Lectura de archivo Delta") \
    .getOrCreate()

# Definir la ruta del archivo Delta
delta_file_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql"

# Leer el contenido del archivo Delta como un DataFrame
df_delta = spark.read.format("delta").load(delta_file_path)

# Mostrar los datos del DataFrame
df_delta.show()



# COMMAND ----------

# MAGIC %md
# MAGIC #### GUardado correctamente en SQL MySQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM visitante_tempview

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Registros historicos por json y manejo de errores

# COMMAND ----------

# Ruta al directorio _delta_log
delta_log_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/_delta_log/"

# Listar los archivos en el directorio _delta_log
delta_log_files = dbutils.fs.ls(delta_log_path)

# Mostrar la lista de archivos
for file in delta_log_files:
    print(file.path)


# COMMAND ----------

import json
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Path of the JSON file
json_file_path = "/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/_delta_log/00000000000000000000.json"

# Read the content of the JSON file using Spark DataFrame
df = spark.read.json(json_file_path)

# Convert the DataFrame to JSON string and parse it
json_data = df.toJSON().first()

# Print the content of the JSON
print(json_data)


# COMMAND ----------

import json
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Path of the JSON file
json_file_path = "/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/_delta_log/00000000000000000000.json"

# Read the content of the JSON file using Spark DataFrame
df = spark.read.json(json_file_path)

# Display the DataFrame
df.show(truncate=False)
# Lee el contenido del archivo JSON en un DataFrame de Spark
df = spark.read.json(json_file_path)

# Muestra el contenido del DataFrame utilizando la función display
display(df)


# COMMAND ----------

import json
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Ruta del archivo JSON
json_file_path = "/mnt/vinkostest1/raw_mount/11-02-2024/visitante_output.sql/_delta_log/00000000000000000000.json"

# Leer el contenido del archivo JSON en un DataFrame de Spark
df = spark.read.json(json_file_path)

# Mostrar el contenido del DataFrame utilizando la función display
display(df)


# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Registro de Errores") \
    .getOrCreate()

# Crear un DataFrame con la información del error
error_df = spark.createDataFrame([
    (datetime.now(), "Error de ejemplo 1"),
    (datetime.now(), "Error de ejemplo 2")
], ["timestamp", "mensaje_error"])

# Definir la ruta del archivo CSV donde se guardarán los errores
error_csv_path = "/mnt/vinkostest1/raw_mount/11-02-2024/error_log.csv"

# Escribir los datos en el archivo CSV
error_df.write.csv(error_csv_path, header=True, mode="append")

# Confirmar que se han escrito los errores en el archivo CSV
error_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Registro de Errores") \
    .getOrCreate()

# Definir la ruta del archivo CSV donde se guardarán los errores
error_csv_path = "/mnt/vinkostest1/raw_mount/11-02-2024/error_log.csv"

def procesar_datos(datos):
    try:
        # Aquí va tu lógica para procesar los datos
        # Simulación de una operación que podría lanzar una excepción
        resultado = 1 / 0
        return resultado
    except Exception as e:
        # Capturar la excepción y registrar el error
        registrar_error(str(e))
        return None

def registrar_error(mensaje):
    # Crear un DataFrame con la información del error
    error_df = spark.createDataFrame([
        (datetime.now(), mensaje)
    ], ["timestamp", "mensaje_error"])

    # Escribir los datos en el archivo CSV
    error_df.write.csv(error_csv_path, header=True, mode="append")

# Llamar a la función para procesar datos
datos = [1, 2, 3, 4, 5]
resultado = procesar_datos(datos)


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Registro de Errores") \
    .getOrCreate()

# Definir la ruta del archivo CSV donde se guardarán los errores
error_csv_path = "/mnt/vinkostest1/raw_mount/11-02-2024/error_log.csv"

# Leer los datos del archivo CSV
error_df = spark.read.csv(error_csv_path, header=True)

# Mostrar los registros de errores
error_df.show(truncate=False)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Tabla errores

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Lectura de archivo CSV en DBFS") \
    .getOrCreate()

# Ruta completa del archivo CSV en DBFS
dbfs_csv_file_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/11-02-2024/visitas_7.csv"

# Leer el archivo CSV en un DataFrame de Spark
df = spark.read.csv(dbfs_csv_file_path, header=True, inferSchema=True)

# Mostrar el esquema y los primeros registros del DataFrame
df.printSchema()
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Filtrado de registros con errores e incompletos") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame de Spark
csv_file_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/11-02-2024/visitas_7.csv"
df = spark.read.csv(csv_file_path, header=True)

# Filtrar registros con valores nulos o vacíos en las columnas de interés
filtered_df = df.filter((col("jk").isNull()) |
                        (col("Badmail").isNull()) |
                        (col("Baja").isNull()) |
                        (col("Fecha envio").isNull()) |
                        (col("Fecha open").isNull()) |
                        (col("Opens").isNull()) |
                        (col("Opens virales").isNull()) |
                        (col("Fecha click").isNull()) |
                        (col("Clicks").isNull()) |
                        (col("Clicks virales").isNull()) |
                        (col("Links").isNull()) |
                        (col("IPs").isNull()) |
                        (col("Navegadores").isNull()) |
                        (col("Plataformas").isNull()) |
                        (col("jk") == "") |
                        (col("Badmail") == "") |
                        (col("Baja") == "") |
                        (col("Fecha envio") == "") |
                        (col("Fecha open") == "") |
                        (col("Opens") == "") |
                        (col("Opens virales") == "") |
                        (col("Fecha click") == "") |
                        (col("Clicks") == "") |
                        (col("Clicks virales") == "") |
                        (col("Links") == "") |
                        (col("IPs") == "") |
                        (col("Navegadores") == "") |
                        (col("Plataformas") == ""))

# Mostrar los registros filtrados
filtered_df.show(truncate=False)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Filtrado de registros con errores") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame de Spark
csv_file_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/11-02-2024/visitas_7.csv"
df = spark.read.csv(csv_file_path, header=True)

# Filtrar registros con errores (sin incluir los registros incompletos)
error_df = df.filter((col("jk").isNull()) |
                     (col("Badmail").isNull()) |
                     (col("Baja").isNull()) |
                     (col("jk") == "") |
                     (col("Badmail") == "") |
                     (col("Baja") == "") |
                     (col("jk") == "HARD"))  # También filtramos por registros con "HARD" en la columna "jk"

# Mostrar los registros con errores
error_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Filtrado de registros con errores") \
    .getOrCreate()

# Leer el archivo CSV en un DataFrame de Spark
csv_file_path = "dbfs:/mnt/vinkostest1/raw_mount/11-02-2024/11-02-2024/visitas_7.csv"
df = spark.read.csv(csv_file_path, header=True)

# Definir las columnas originales y los nuevos nombres de las columnas
columnas = df.columns
nuevos_nombres = ['email', 'jk_error', 'badmail_error', 'baja_error', 'fecha_envio_error', 'fecha_open_error', 
                  'opens_error', 'opens_virales_error', 'fecha_click_error', 'clicks_error', 'clicks_virales_error', 
                  'links_error', 'ips_error', 'navegadores_error', 'plataformas_error']

# Filtrar registros con valores nulos, 0 o vacíos
registros_con_errores = df.filter(
    (col("jk") == "") |
    (col("Badmail") == "") |
    (col("Baja") == "") |
    (col("Fecha envio") == "") |
    (col("Fecha open") == "") |
    (col("Opens") == "0") |
    (col("Opens virales") == "0") |
    (col("Fecha click") == "") |
    (col("Clicks") == "0") |
    (col("Clicks virales") == "0") |
    (col("Links") == "-") |
    (col("IPs") == "-") |
    (col("Navegadores") == "-") |
    (col("Plataformas") == "-")
)

# Renombrar las columnas
for col_original, col_nuevo in zip(columnas, nuevos_nombres):
    registros_con_errores = registros_con_errores.withColumnRenamed(col_original, col_nuevo)

# Mostrar los registros con errores y sus columnas renombradas
registros_con_errores.show(truncate=False)

display(registros_con_errores)


# COMMAND ----------

# Registrar el DataFrame como una vista temporal
registros_con_errores.createOrReplaceTempView("registros_con_errores_view")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM registros_con_errores_view

# COMMAND ----------

# Crear una vista temporal
registros_con_errores.createOrReplaceTempView("temp_errores")

# Ejecutar una consulta SQL para seleccionar todos los registros de la vista temporal
consulta_sql = "SELECT * FROM temp_errores"

# Obtener los resultados de la consulta SQL en un DataFrame
resultados_sql = spark.sql(consulta_sql)

# Escribir los resultados en un archivo .sql
ruta_sql = "/mnt/vinkostest1/raw_mount/registros_con_errores_sql.sql"
resultados_sql.write.mode("overwrite").csv(ruta_sql, header=True, sep="|")


# COMMAND ----------


