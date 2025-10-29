# Databricks notebook source
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Lê dados da camada Bronze
df_bronze = spark.read.parquet("dbfs:/Volumes/workspace/default/arquivos-projetos/01_ingestao_api_ibge.parquet")

# Extrai o campo JSON
data = json.loads(df_bronze.collect()[0]["raw_json"])

serie = data[0]['resultados'][0]['series'][0]['serie']
df_silver = spark.createDataFrame(
    [(k, float(v)) for k, v in serie.items()],
    ["periodo", "ipca"]
)

# Limpa formato do período
df_silver = df_silver.withColumn("periodo", f.regexp_replace(f.col("periodo"), "M", "/"))

df_silver = df_silver.withColumn(
    "periodo",
    f.concat(
        f.substring("periodo", 5, 2),
        f.lit("/"),
        f.substring("periodo", 1, 4)
    )
)

df_silver = df_silver.withColumn(
    "periodo",
    f.to_date(f.col("periodo"), "MM/yyyy")
)

display(df_silver)

# # Salva na camada Silver
# df_silver.write.mode("overwrite").parquet(
#     "dbfs:/Volumes/workspace/default/arquivos-projetos/02_transformacao_silver"
# )

