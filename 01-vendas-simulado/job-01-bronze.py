# Databricks notebook source
from pyspark.sql import functions as F

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("/Volumes/workspace/vendas/raw/vendas.csv")
)

df_bronze = (
    df_raw
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_arquivo_origem", F.lit("/Volumes/workspace/vendas/raw/vendas.csv"))
)

df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.bronze_vendas")
print("Bronze OK:", df_bronze.count(), "linhas")