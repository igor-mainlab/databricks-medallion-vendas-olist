# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.vendas;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.vendas.raw;

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

categorias = ["Eletrônicos", "Roupas", "Livros", "Casa", "Esportes"]
cidades = ["Rio de Janeiro", "São Paulo", "Belém", "Fortaleza", "Curitiba"]

linhas = ["pedido_id,data_pedido,cliente,categoria,cidade,quantidade,preco_unitario"]

for i in range(1, 1001):
    data = date(2026, 1, 1) + timedelta(days=random.randint(0, 270))
    cliente = f"cliente_{random.randint(1, 200)}"
    categoria = random.choice(categorias)
    cidade = random.choice(cidades)
    qtd = random.randint(1, 5)
    preco = round(random.uniform(20, 1500), 2)

    # sujeira proposital
    if i % 50 == 0:
        categoria = ""        # categoria vazia
    if i % 70 == 0:
        qtd = ""              # quantidade nula

    linhas.append(f"{i},{data.strftime('%d/%m/%Y')},{cliente},{categoria},{cidade},{qtd},{preco}")

# duplicatas propositais
linhas += linhas[10:20]

caminho = "/Volumes/workspace/vendas/raw/vendas.csv"
dbutils.fs.put(caminho, "\n".join(linhas), overwrite=True)
print(f"Arquivo gravado em {caminho} com {len(linhas)-1} linhas de dados")

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("/Volumes/workspace/vendas/raw/vendas.csv")
)

df_raw.printSchema()
print("Total de linhas:", df_raw.count())
display(df_raw.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_raw
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_arquivo_origem", F.lit("/Volumes/workspace/vendas/raw/vendas.csv"))
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.vendas.bronze_vendas")
)

print("Tabela bronze_vendas criada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_linhas FROM workspace.vendas.bronze_vendas

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.vendas.bronze_vendas

# COMMAND ----------

