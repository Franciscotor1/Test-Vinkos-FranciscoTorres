# Databricks notebook source
# MAGIC %md
# MAGIC #### Crear la tabla estadistica en SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS estadistica1
# MAGIC WITH DBPROPERTIES (MANAGEDLOCATION='abfss://REDACTED_LOCAL_PART@vinkostest1.dfs.core.windows.net/mnt/vinkostest1/raw_mount');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED testing_ws_vinkos.estadistica1;
# MAGIC

# COMMAND ----------

# Cargar el archivo CSV en un DataFrame de Spark con encabezados de columna
visitas_df1 = spark.read \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/vinkostest1/dbdemo_mount/csvdemos/visitas_7.csv")  # Ruta del archivo CSV

# Mostrar los primeros registros del DataFrame
visitas_df1.show()



# COMMAND ----------

from pyspark.sql.functions import col

# Replace any invalid characters in column names
new_columns = [col(column).alias(column.replace(' ', '_').replace(',', '_')) for column in visitas_df1.columns]

# Apply the new column names to the DataFrame
visitas_df2 = visitas_df1.select(new_columns)

# Save the DataFrame as a Delta table
visitas_df2.write.format("delta").mode("overwrite").saveAsTable("estadistica1.estadisticas_transformacion1")


# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE estadistica1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED testing_ws_vinkos.estadistica1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM estadistica1.estadisticas_transformacion1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

visitas_df2.write.format("delta").mode("overwrite").save("/mnt/vinkostest1/raw_mount/estadisticasql/estadistica_bronce")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_registros
# MAGIC FROM estadistica1.estadisticas_transformacion1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calcular la cantidad de registros donde el campo "Badmail" no es nulo:

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_badmail
# MAGIC FROM estadistica1.estadisticas_transformacion1
# MAGIC WHERE Badmail IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Encontrar el correo electrónico (email) más frecuente en la tabla:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, COUNT(*) AS frecuencia
# MAGIC FROM estadistica1.estadisticas_transformacion1
# MAGIC GROUP BY email
# MAGIC ORDER BY COUNT(*) DESC
# MAGIC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calcular el porcentaje de registros con "Opens" igual a 0:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     (COUNT(*) FILTER (WHERE Opens = 0) * 100.0 / COUNT(*)) AS porcentaje_opens_0
# MAGIC FROM estadistica1.estadisticas_transformacion1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED estadistica1.estadisticas_transformacion1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN Clicks >= 10 THEN 'Alto interés'
# MAGIC         WHEN Clicks >= 5 THEN 'Interés medio'
# MAGIC         ELSE 'Bajo interés'
# MAGIC     END AS segmento,
# MAGIC     COUNT(*) AS cantidad_clientes
# MAGIC FROM estadistica1.estadisticas_transformacion1
# MAGIC GROUP BY segmento;
# MAGIC

# COMMAND ----------


