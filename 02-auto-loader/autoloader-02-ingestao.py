# Databricks notebook source
from pyspark.sql import functions as F

PASTA_LANDING = "/Volumes/workspace/vendas/landing/vendas"
CHECKPOINT = "/Volumes/workspace/vendas/checkpoints/bronze_vendas_incremental"
TABELA = "workspace.vendas.bronze_vendas_incremental"

def ingerir():
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
    print("Ingestão concluída.")

# COMMAND ----------

ingerir()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _arquivo_origem, COUNT(*) AS linhas
# MAGIC FROM workspace.vendas.bronze_vendas_incremental
# MAGIC GROUP BY _arquivo_origem
# MAGIC ORDER BY _arquivo_origem

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT canal_venda, COUNT(*) AS pedidos
# MAGIC FROM workspace.vendas.bronze_vendas_incremental
# MAGIC GROUP BY canal_venda
# MAGIC ORDER BY pedidos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_linhas, COUNT(DISTINCT pedido_id) AS pedidos_distintos
# MAGIC FROM workspace.vendas.bronze_vendas_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.vendas.bronze_vendas_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_com_dado_resgatado
# MAGIC FROM workspace.vendas.bronze_vendas_incremental
# MAGIC WHERE _rescued_data IS NOT NULL

# COMMAND ----------

