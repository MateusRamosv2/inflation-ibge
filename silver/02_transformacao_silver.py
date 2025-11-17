# Databricks notebook source
import json
import pyspark.sql.functions as f

# Lê Bronze
df_bronze = spark.read.parquet(
    "dbfs:/Volumes/workspace/default/arquivos-projetos/01_ingestao_api_ibge.parquet"
)

# Extrai JSON com segurança
raw_json = df_bronze.select("raw_json").head()[0]
data = json.loads(raw_json)

# Extrai série histórica
serie = data[0]['resultados'][0]['series'][0]['serie']

# Cria Silver estruturada
df_silver = spark.createDataFrame(
    [(k, float(v)) for k, v in serie.items()],
    ["periodo_raw", "valor_ipca"]
)

# Limpando período
df_silver = df_silver.withColumn(
    "periodo_raw", 
    f.regexp_replace("periodo_raw", "M", "/")
)

df_silver = df_silver.withColumn(
    "periodo",
    f.concat(
        f.substring("periodo_raw", 5, 2),
        f.lit("/"),
        f.substring("periodo_raw", 1, 4)
    )
)

df_silver = df_silver.withColumn(
    "data_referencia",
    f.to_date(f.col("periodo"), "MM/yyyy")
)

# Metadados
df_silver = df_silver.withColumn("dt_processamento", f.current_timestamp())
df_silver = df_silver.withColumn("fonte", f.lit("API_IBGE"))

# Ordenação
df_silver = df_silver.orderBy("data_referencia")

display(df_silver)

# Salva Silver em Delta
df_silver.write.format("parquet").mode("overwrite").save(
    "dbfs:/Volumes/workspace/default/arquivos-projetos/02_transformacao_silver"
)

