# Databricks notebook source
from pyspark.sql import functions as F

PASTA_LANDING = "/Volumes/workspace/vendas/landing/vendas"
CHECKPOINT = "/Volumes/workspace/vendas/checkpoints/bronze_vendas_incremental"
TABELA = "workspace.vendas.bronze_vendas_incremental"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", False)
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(PASTA_LANDING)
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
)

query = (
    df.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABELA)
)
query.awaitTermination()

total = spark.table(TABELA).count()
print("Bronze incremental OK:", total, "linhas")

# COMMAND ----------

