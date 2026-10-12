# Databricks notebook source
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/olist/raw"
arquivos = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
    "product_category_name_translation.csv",
]

for nome in arquivos:
    df = spark.read.option("header", True).option("inferSchema", False).csv(f"{BASE}/{nome}")
    print(f"{nome}: {df.count()} linhas, {len(df.columns)} colunas")
    print(df.columns)
    print()

# COMMAND ----------

orders = spark.read.option("header", True).csv(f"{BASE}/olist_orders_dataset.csv")

orders.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in orders.columns]).show(vertical=True)
orders.groupBy("order_status").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_orders' AS tabela, COUNT(*) AS linhas FROM workspace.olist.bronze_orders
# MAGIC UNION ALL SELECT 'bronze_order_items', COUNT(*) FROM workspace.olist.bronze_order_items
# MAGIC UNION ALL SELECT 'bronze_customers', COUNT(*) FROM workspace.olist.bronze_customers
# MAGIC UNION ALL SELECT 'bronze_products', COUNT(*) FROM workspace.olist.bronze_products
# MAGIC UNION ALL SELECT 'bronze_payments', COUNT(*) FROM workspace.olist.bronze_payments
# MAGIC UNION ALL SELECT 'bronze_category_translation', COUNT(*) FROM workspace.olist.bronze_category_translation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- nulos de entrega por status
# MAGIC SELECT
# MAGIC   order_status,
# MAGIC   COUNT(*) AS pedidos,
# MAGIC   SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS sem_data_entrega
# MAGIC FROM workspace.olist.bronze_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY pedidos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- itens e pagamentos sem pedido correspondente, e pedidos sem item
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.olist.bronze_order_items i
# MAGIC      LEFT JOIN workspace.olist.bronze_orders o ON i.order_id = o.order_id
# MAGIC      WHERE o.order_id IS NULL) AS itens_sem_pedido,
# MAGIC   (SELECT COUNT(*) FROM workspace.olist.bronze_payments p
# MAGIC      LEFT JOIN workspace.olist.bronze_orders o ON p.order_id = o.order_id
# MAGIC      WHERE o.order_id IS NULL) AS pagamentos_sem_pedido,
# MAGIC   (SELECT COUNT(*) FROM workspace.olist.bronze_orders o
# MAGIC      LEFT JOIN (SELECT DISTINCT order_id FROM workspace.olist.bronze_order_items) i ON o.order_id = i.order_id
# MAGIC      WHERE i.order_id IS NULL) AS pedidos_sem_item

# COMMAND ----------

# MAGIC %sql
# MAGIC -- produtos sem categoria e categorias sem tradução
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN p.product_category_name IS NULL THEN 1 ELSE 0 END) AS produtos_sem_categoria,
# MAGIC   COUNT(DISTINCT p.product_category_name) - COUNT(DISTINCT t.product_category_name) AS categorias_sem_traducao
# MAGIC FROM workspace.olist.bronze_products p
# MAGIC LEFT JOIN workspace.olist.bronze_category_translation t
# MAGIC   ON p.product_category_name = t.product_category_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.order_status, COUNT(*) AS pedidos_sem_item
# MAGIC FROM workspace.olist.bronze_orders o
# MAGIC LEFT JOIN (SELECT DISTINCT order_id FROM workspace.olist.bronze_order_items) i
# MAGIC   ON o.order_id = i.order_id
# MAGIC WHERE i.order_id IS NULL
# MAGIC GROUP BY o.order_status
# MAGIC ORDER BY pedidos_sem_item DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver_orders' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT order_id) AS ids_distintos FROM workspace.olist.silver_orders
# MAGIC UNION ALL SELECT 'silver_order_items', COUNT(*), COUNT(DISTINCT order_id) FROM workspace.olist.silver_order_items
# MAGIC UNION ALL SELECT 'silver_payments', COUNT(*), COUNT(DISTINCT order_id) FROM workspace.olist.silver_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- controle direto, sem passar pelas Gold
# MAGIC SELECT COUNT(DISTINCT o.order_id) AS pedidos, COUNT(*) AS itens, ROUND(SUM(i.price), 2) AS receita
# MAGIC FROM workspace.olist.silver_order_items i
# MAGIC JOIN workspace.olist.silver_orders o ON i.order_id = o.order_id
# MAGIC WHERE o.order_status = 'delivered'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'mensal' AS tabela, SUM(pedidos) AS pedidos, SUM(itens) AS itens, ROUND(SUM(receita), 2) AS receita FROM workspace.olist.gold_receita_mensal
# MAGIC UNION ALL SELECT 'categoria', SUM(pedidos), SUM(itens), ROUND(SUM(receita), 2) FROM workspace.olist.gold_receita_categoria
# MAGIC UNION ALL SELECT 'estado', SUM(pedidos), NULL, ROUND(SUM(receita), 2) FROM workspace.olist.gold_receita_estado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(pedidos_entregues) AS entregues, SUM(pedidos_atrasados) AS atrasados,
# MAGIC        ROUND(100 * SUM(pedidos_atrasados) / SUM(pedidos_entregues), 2) AS pct_atrasados_geral
# MAGIC FROM workspace.olist.gold_entrega_estado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS estados_com_100_ou_mais
# MAGIC FROM workspace.olist.gold_entrega_estado
# MAGIC WHERE pedidos_entregues >= 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_state, pedidos_entregues, pct_atrasados
# MAGIC FROM workspace.olist.gold_entrega_estado
# MAGIC WHERE pedidos_entregues < 100
# MAGIC ORDER BY pedidos_entregues

# COMMAND ----------

from pyspark import pipelines as dp

print([n for n in dir(dp) if "cdc" in n.lower() or "change" in n.lower() or "target" in n.lower() or "streaming" in n.lower()])

# COMMAND ----------

