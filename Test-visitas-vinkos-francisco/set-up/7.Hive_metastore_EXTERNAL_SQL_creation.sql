-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Se creean bases de datos de ingesta csv y se canalizan externamente a SQL o Mysql

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SELECT * FROM hive_metastore.default.visitas_7;

-- COMMAND ----------

SHOW TABLES IN hive_metastore.default;

-- COMMAND ----------

SELECT COUNT(*) FROM hive_metastore.default.visitas_7;

-- COMMAND ----------

DESCRIBE DATABASE hive_metastore.default;

-- COMMAND ----------

USE hive_metastore.default;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE default;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC DATABASE default;

-- COMMAND ----------

SELECT * FROM visitas_7;

-- COMMAND ----------

DESC visitas_7;

-- COMMAND ----------


