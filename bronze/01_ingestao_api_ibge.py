# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import json

spark = SparkSession.builder.appName("IBGE_IPCA_Bronze").getOrCreate()

# URL da API
url = "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/all/variaveis/63?localidades=BR"
response = requests.get(url)
data = response.json()

# Cria DataFrame com JSON bruto
df_bronze = spark.createDataFrame([(json.dumps(data),)], ["raw_json"]) \
                 .withColumn("data_ingestao", current_timestamp()) \
                 .withColumn("fonte", lit("API_IBGE"))


display(df_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

df_bronze.write.mode("overwrite").parquet(
    "dbfs:/Volumes/workspace/default/arquivos-projetos/01_ingestao_api_ibge.parquet"
)
