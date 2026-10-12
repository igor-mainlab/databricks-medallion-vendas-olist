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
    .orderBy("mes")
)

gold_mensal.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_mensal")
display(gold_mensal)

# COMMAND ----------

gold_categoria = (
    silver
    .groupBy("categoria")
    .agg(
        F.countDistinct("pedido_id").alias("total_pedidos"),
        F.sum("quantidade").alias("itens_vendidos"),
        F.round(F.sum("valor_total"), 2).alias("receita_total")
    )
    .orderBy(F.desc("receita_total"))
)

gold_categoria.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_categoria")
display(gold_categoria)

# COMMAND ----------

gold_cidade = (
    silver
    .groupBy("cidade")
    .agg(
        F.countDistinct("pedido_id").alias("total_pedidos"),
        F.round(F.sum("valor_total"), 2).alias("receita_total"),
        F.round(F.avg("valor_total"), 2).alias("ticket_medio")
    )
    .orderBy(F.desc("receita_total"))
)

gold_cidade.write.format("delta").mode("overwrite").saveAsTable("workspace.vendas.gold_vendas_cidade")
display(gold_cidade)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'mensal' AS tabela, ROUND(SUM(receita_total), 2) AS receita FROM workspace.vendas.gold_vendas_mensal
# MAGIC UNION ALL
# MAGIC SELECT 'categoria', ROUND(SUM(receita_total), 2) FROM workspace.vendas.gold_vendas_categoria
# MAGIC UNION ALL
# MAGIC SELECT 'cidade', ROUND(SUM(receita_total), 2) FROM workspace.vendas.gold_vendas_cidade
# MAGIC UNION ALL
# MAGIC SELECT 'silver', ROUND(SUM(valor_total), 2) FROM workspace.vendas.silver_vendas

# COMMAND ----------

