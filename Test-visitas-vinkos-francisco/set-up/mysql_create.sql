-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS dbdemo

-- COMMAND ----------

DESC DATABASE EXTENDED dbdemo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ruta del archivo de texto
-- MAGIC file_path = "/mnt/vinkostest1/demo/report_7.txt"
-- MAGIC
-- MAGIC # Leer el archivo de texto en un DataFrame
-- MAGIC df_read = spark.read.text(file_path)
-- MAGIC
-- MAGIC # Mostrar el contenido del DataFrame
-- MAGIC df_read.show()
-- MAGIC

-- COMMAND ----------


