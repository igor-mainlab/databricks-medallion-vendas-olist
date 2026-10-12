# Databricks notebook source
from pyspark.sql import functions as F

silver = spark.table("workspace.vendas.silver_vendas")

gold_mensal = (
    silver
    .withColumn("mes", F.date_format("data_pedido", "yyyy-MM"))
    .groupBy("mes")
    .agg(
        F.countDistinct("pedido_id").alias("total_pedidos"),
        F.sum("quantidade").alias("itens_vendidos"),
        F.round(F.sum("valor_total"), 2).alias("receita_total"),
        F.round(F.avg("valor_total"), 2).alias("ticket_medio")
    )
)
gold_mensal.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_mensal")

gold_categoria = (
    silver
    .groupBy("categoria")
    .agg(
        F.countDistinct("pedido_id").alias("total_pedidos"),
        F.sum("quantidade").alias("itens_vendidos"),
        F.round(F.sum("valor_total"), 2).alias("receita_total")
    )
)
gold_categoria.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_categoria")

gold_cidade = (
    silver
    .groupBy("cidade")
    .agg(
        F.countDistinct("pedido_id").alias("total_pedidos"),
        F.round(F.sum("valor_total"), 2).alias("receita_total"),
        F.round(F.avg("valor_total"), 2).alias("ticket_medio")
    )
)
gold_cidade.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_cidade")

print("Gold OK")

# COMMAND ----------

