# Test-Vinkos-FranciscoTorres
El proyecto sigue una arquitectura basada en la nube, aprovechando las capacidades de Azure para procesar y almacenar datos de manera escalable y eficiente que puede procesar cualquier tipo de archivo: json, txt, csv, sql, parquet, etc. Se utilizaron las siguientes tecnologías:

Apache Spark (Pyspark)
Azure Data lake gen2 (Blob Storage)
Azure Databricks
Azure Delta Lake
Data Bricks DBFS(Databricks File System)
Azure Key Vault.
MySQL y SQL en consultas y almacenamiento en delta

Metodología de almacenamiento:

Se utiliza el enfoque de almacenamiento en capas Medallion (Bronze, silver, and Gold, que incluye las carpetas "demo", "raw", "process" y "presentation", cada una con su propio propósito en el proceso de ETL.
Las carpetas "demo" y "raw" pueden contener datos en su forma original o mínimamente procesada.
La carpeta "process" almacena datos procesados y transformados listos para su análisis y presentación.
La carpeta "presentation“ o Gold contiene datos refinados y agregados, listos para ser consumidos por aplicaciones o usuarios finales.

Integración con MySQL:
Se integra una base de datos MySQL desde Databricks para almacenar tablas interactivas que pueden ser consultadas y utilizadas para análisis ad-hoc o para alimentar aplicaciones frontales.
Estas tablas pueden ser actualizadas de uso PERMANENTE o periódicamente utilizando los datos procesados y transformados almacenados en el Data Lake, o sacadas de Hive metastore.

El pipeline cumple con las pautas de ETL pedidas por el test:Ir todos los días al directorio para buscar los archivos
Validar el layout de los archivos
Validar la información a cargar.
Borrar los archivos cargados en el origen
Llevar una bitácora de control de carga de los archivos para poder reportar 
En el caso del path del backup lo sustituí por el Data lake en ejemplo mnt/vinkostest/demo (y otros contenedores como dbd) esto nos lleva al Data lake Storage Gen2 donde tenemos nuestra montura del Delta Lake y el control de versiones de nuestras consultas y transformaciones, así podemos descargar cualquier archivo o programarlo para que sea automático en un zip y también puedan dejar registro.Lo demás ya son cálculos y transformaciones



