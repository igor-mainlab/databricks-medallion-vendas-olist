# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.vendas.silver_merge_teste AS
# MAGIC SELECT * FROM workspace.vendas.silver_vendas_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas, ROUND(SUM(valor_total), 2) AS receita
# MAGIC FROM workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC -- guarda o estado de 3 pedidos que vamos corrigir depois
# MAGIC SELECT pedido_id, quantidade, preco_unitario, valor_total
# MAGIC FROM workspace.vendas.silver_merge_teste
# MAGIC WHERE pedido_id IN (10, 20, 30)
# MAGIC ORDER BY pedido_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW atualizacoes AS
# MAGIC SELECT * FROM VALUES
# MAGIC   -- correções: mesmos IDs, preço ajustado
# MAGIC   (10,   DATE'2026-02-10', 'cliente_7',  'Livros',  'São Paulo', 1, 800.00,  'não informado', 800.00),
# MAGIC   (20,   DATE'2026-03-15', 'cliente_8',  'Roupas',  'Belém',     4, 500.00,  'não informado', 2000.00),
# MAGIC   (30,   DATE'2026-04-20', 'cliente_9',  'Casa',    'Curitiba',  4, 1000.00, 'não informado', 4000.00),
# MAGIC   -- pedidos novos
# MAGIC   (2001, DATE'2026-09-01', 'cliente_50', 'Esportes','Fortaleza', 2, 300.00,  'site',          600.00),
# MAGIC   (2002, DATE'2026-09-02', 'cliente_51', 'Livros',  'Belém',     1, 120.00,  'app',           120.00)
# MAGIC AS t(pedido_id, data_pedido, cliente, categoria, cidade, quantidade, preco_unitario, canal_venda, valor_total)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM atualizacoes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.vendas.silver_merge_teste AS destino
# MAGIC USING atualizacoes AS origem
# MAGIC ON destino.pedido_id = origem.pedido_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas, COUNT(DISTINCT pedido_id) AS pedidos, ROUND(SUM(valor_total), 2) AS receita
# MAGIC FROM workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pedido_id, cliente, categoria, cidade, quantidade, preco_unitario, valor_total
# MAGIC FROM workspace.vendas.silver_merge_teste
# MAGIC WHERE pedido_id IN (10, 20, 30, 2001, 2002)
# MAGIC ORDER BY pedido_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW empilhado AS
# MAGIC SELECT * FROM workspace.vendas.silver_merge_teste VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT * FROM atualizacoes

# COMMAND ----------

from pyspark.sql import functions as F

dedup = spark.table("empilhado").dropDuplicates(["pedido_id"])
display(
    dedup.filter(F.col("pedido_id").isin(10, 20, 30))
    .select("pedido_id", "cliente", "cidade", "preco_unitario", "valor_total")
    .orderBy("pedido_id")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pedido_id, preco_unitario, valor_total
# MAGIC FROM workspace.vendas.silver_merge_teste
# MAGIC WHERE pedido_id IN (10, 20, 30)
# MAGIC ORDER BY pedido_id

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE workspace.vendas.silver_merge_teste TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas, ROUND(SUM(valor_total), 2) AS receita
# MAGIC FROM workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.vendas.silver_merge_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.vendas.silver_merge_teste

# COMMAND ----------

