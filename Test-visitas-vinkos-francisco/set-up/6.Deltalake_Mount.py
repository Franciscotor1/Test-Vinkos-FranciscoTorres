# Databricks notebook source
# MAGIC %md
# MAGIC #### Montaje Del deltalake para crear bases de datos
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS v1_demo
# MAGIC LOCATION '/mnt/vinkostest1/dbdemo_mount'

# COMMAND ----------

process_df = spark.read \
    .option("inferschema", True) \
    .csv("/mnt/vinkostest1/raw_mount/11-02-2024/11-02-2024/visitas_7.csv")

# COMMAND ----------

process_df.write.format("delta").mode("append").saveAsTable("v1_demo.proceso1_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     _c0 AS email,
# MAGIC     _c1 AS jk,
# MAGIC     _c2 AS Badmail,
# MAGIC     _c3 AS Baja,
# MAGIC     _c4 AS Fecha_envio,
# MAGIC     _c5 AS Fecha_open,
# MAGIC     _c6 AS Opens,
# MAGIC     _c7 AS Opens_virales,
# MAGIC     _c8 AS Fecha_click,
# MAGIC     _c9 AS Clicks,
# MAGIC     _c10 AS Clicks_virales,
# MAGIC     _c11 AS Links,
# MAGIC     _c12 AS IPs,
# MAGIC     _c13 AS Navegadores,
# MAGIC     _c14 AS Plataformas
# MAGIC FROM v1_demo.proceso1_managed
# MAGIC OFFSET 1;
# MAGIC

# COMMAND ----------

resultados_df = spark.sql("""
    SELECT 
        _c0 AS email,
        _c1 AS jk,
        _c2 AS Badmail,
        _c3 AS Baja,
        _c4 AS Fecha_envio,
        _c5 AS Fecha_open,
        _c6 AS Opens,
        _c7 AS Opens_virales,
        _c8 AS Fecha_click,
        _c9 AS Clicks,
        _c10 AS Clicks_virales,
        _c11 AS Links,
        _c12 AS IPs,
        _c13 AS Navegadores,
        _c14 AS Plataformas
    FROM v1_demo.proceso1_managed
    OFFSET 1
""")

resultados_df.write.format("delta").mode("append").saveAsTable("v1_demo.proceso1_manage_resultados")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v1_demo.proceso1_manage_resultados;
# MAGIC

# COMMAND ----------

resultados_df.write.format("delta").mode("append").save("/mnt/vinkostest1/raw_mount/external_managed")


# COMMAND ----------


