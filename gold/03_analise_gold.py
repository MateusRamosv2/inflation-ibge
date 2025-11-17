# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# Carrega Silver
df_silver = spark.read.parquet(
    "dbfs:/Volumes/workspace/default/arquivos-projetos/02_transformacao_silver"
)

df = (
    df_silver
    .withColumn("ano", f.year("data_referencia"))
    .withColumn("mes", f.month("data_referencia"))
    .withColumn(
        "yyyymm",
        f.date_format("data_referencia", "yyyyMM").cast("int")
    )
)

window_order = (
    Window.partitionBy().orderBy("data_referencia")
)

df = df.withColumn(
    "valor_mes_anterior",
    f.lag("valor_ipca").over(window_order)
)

df = df.withColumn(
    "variacao_mensal",
    f.round((f.col("valor_ipca") - f.col("valor_mes_anterior")), 4)
)

from pyspark.sql.window import Window

w12 = Window.orderBy("data_referencia").rowsBetween(-11, 0)

df = df.withColumn(
    "ipca_12m",
    f.round(f.sum("valor_ipca").over(w12), 4)
)

w_ytd = (
    Window.partitionBy("ano").orderBy("data_referencia")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df = df.withColumn(
    "ipca_acumulado_ano",
    f.round(f.sum("valor_ipca").over(w_ytd), 4)
)

df = df.withColumn(
    "flag_maior_alta",
    f.when(f.col("variacao_mensal") == f.max("variacao_mensal").over(Window.partitionBy()), 1).otherwise(0)
)

df = df.withColumn(
    "flag_maior_queda",
    f.when(f.col("variacao_mensal") == f.min("variacao_mensal").over(Window.partitionBy()), 1).otherwise(0)
)

df_gold = (
    df.withColumn("dt_processamento", f.current_timestamp())
      .withColumn("camada", f.lit("GOLD"))
)

display(df_gold)

df_gold.write.format("parquet").mode("overwrite").save(
    "dbfs:/Volumes/workspace/default/arquivos-projetos/03_gold_ipca"
)
