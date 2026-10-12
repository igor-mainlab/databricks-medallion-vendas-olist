# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.vendas.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.vendas.checkpoints;

# COMMAND ----------

import random
from datetime import date, timedelta

CATEGORIAS = ["Eletrônicos", "Roupas", "Livros", "Casa", "Esportes"]
CIDADES = ["Rio de Janeiro", "São Paulo", "Belém", "Fortaleza", "Curitiba"]
PASTA = "/Volumes/workspace/vendas/landing/vendas"

def gerar_lote(numero, inicio_id, qtd):
    random.seed(numero)
    linhas = ["pedido_id,data_pedido,cliente,categoria,cidade,quantidade,preco_unitario"]

    for i in range(inicio_id, inicio_id + qtd):
        data = date(2026, 1, 1) + timedelta(days=random.randint(0, 270))
        cliente = f"cliente_{random.randint(1, 200)}"
        categoria = random.choice(CATEGORIAS)
        cidade = random.choice(CIDADES)
        quantidade = random.randint(1, 5)
        preco = round(random.uniform(20, 1500), 2)

        # sujeira proposital, como antes
        if i % 50 == 0:
            categoria = ""
        if i % 70 == 0:
            quantidade = ""

        linhas.append(f"{i},{data.strftime('%d/%m/%Y')},{cliente},{categoria},{cidade},{quantidade},{preco}")

    dbutils.fs.mkdirs(PASTA)
    caminho = f"{PASTA}/vendas_lote_{numero:03d}.csv"
    dbutils.fs.put(caminho, "\n".join(linhas), overwrite=True)
    print(f"Lote {numero} gravado: {qtd} pedidos (ids {inicio_id} a {inicio_id + qtd - 1}) em {caminho}")

# COMMAND ----------

gerar_lote(numero=1, inicio_id=1, qtd=500)
display(dbutils.fs.ls(PASTA))

# COMMAND ----------

gerar_lote(numero=2, inicio_id=501, qtd=300)
gerar_lote(numero=3, inicio_id=801, qtd=200)
display(dbutils.fs.ls(PASTA))

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(4)
canais = ["site", "app", "loja_fisica"]

linhas = ["pedido_id,data_pedido,cliente,categoria,cidade,quantidade,preco_unitario,canal_venda"]

for i in range(1001, 1101):
    data = date(2026, 1, 1) + timedelta(days=random.randint(0, 270))
    cliente = f"cliente_{random.randint(1, 200)}"
    categoria = random.choice(CATEGORIAS)
    cidade = random.choice(CIDADES)
    quantidade = random.randint(1, 5)
    preco = round(random.uniform(20, 1500), 2)
    canal = random.choice(canais)
    linhas.append(f"{i},{data.strftime('%d/%m/%Y')},{cliente},{categoria},{cidade},{quantidade},{preco},{canal}")

dbutils.fs.put(f"{PASTA}/vendas_lote_004.csv", "\n".join(linhas), overwrite=True)
print("Lote 4 gravado: 100 pedidos (ids 1001 a 1100) com a coluna canal_venda")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS linhas,
# MAGIC   COUNT(DISTINCT pedido_id) AS pedidos,
# MAGIC   SUM(CASE WHEN categoria = 'Não informada' THEN 1 ELSE 0 END) AS cat_nao_informada,
# MAGIC   SUM(CASE WHEN canal_venda = 'não informado' THEN 1 ELSE 0 END) AS canal_nao_informado,
# MAGIC   ROUND(SUM(valor_total), 2) AS receita
# MAGIC FROM workspace.vendas.silver_vendas_v2

# COMMAND ----------

gerar_lote(numero=5, inicio_id=1101, qtd=150)

# COMMAND ----------

gerar_lote(numero=6, inicio_id=1251, qtd=200)