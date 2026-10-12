# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("workspace.vendas.bronze_vendas")

print("Total de linhas:", df.count())
print("Pedidos distintos:", df.select("pedido_id").distinct().count())
print("Categorias vazias:", df.filter((F.col("categoria").isNull()) | (F.trim("categoria") == "")).count())
print("Quantidades nulas:", df.filter((F.col("quantidade").isNull()) | (F.trim("quantidade") == "")).count())

# COMMAND ----------

df_silver = (
    df
    .dropDuplicates(["pedido_id"])
    .withColumn("pedido_id", F.col("pedido_id").cast("int"))
    .withColumn("data_pedido", F.to_date("data_pedido", "dd/MM/yyyy"))
    .withColumn("quantidade", F.col("quantidade").cast("int"))
    .withColumn("preco_unitario", F.col("preco_unitario").cast("double"))
    .withColumn(
        "categoria",
        F.when(F.col("categoria").isNull() | (F.trim("categoria") == ""), F.lit("Não informada"))
         .otherwise(F.col("categoria"))
    )
    .withColumn("quantidade", F.coalesce(F.col("quantidade"), F.lit(1)))
    .withColumn("valor_total", F.round(F.col("quantidade") * F.col("preco_unitario"), 2))
    .drop("_ingestao_ts", "_arquivo_origem")
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.vendas.silver_vendas")
)

s = spark.table("workspace.vendas.silver_vendas")
print("Total de linhas:", s.count())
print("Categorias 'Não informada':", s.filter(F.col("categoria") == "Não informada").count())
print("Categorias nulas:", s.filter(F.col("categoria").isNull()).count())
print("Quantidades nulas:", s.filter(F.col("quantidade").isNull()).count())

# COMMAND ----------

