# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.vendas.landing_cdc

# COMMAND ----------

import random

PASTA_CDC = "/Volumes/workspace/vendas/landing_cdc/pedidos"

def gerar_mudancas(numero, ids, ts_base):
    random.seed(numero)
    linhas = ["pedido_id,cliente,categoria,cidade,quantidade,preco_unitario,atualizado_em"]
    for i in ids:
        cliente = f"cliente_{random.randint(1, 200)}"
        categoria = random.choice(["Eletrônicos", "Roupas", "Livros", "Casa", "Esportes"])
        cidade = random.choice(["Rio de Janeiro", "São Paulo", "Belém", "Fortaleza", "Curitiba"])
        qtd = random.randint(1, 5)
        preco = round(random.uniform(20, 1500), 2)
        linhas.append(f"{i},{cliente},{categoria},{cidade},{qtd},{preco},{ts_base}")
    dbutils.fs.mkdirs(PASTA_CDC)
    caminho = f"{PASTA_CDC}/mudancas_{numero:03d}.csv"
    dbutils.fs.put(caminho, "\n".join(linhas), overwrite=True)
    print(f"Arquivo {numero}: {len(ids)} registros em {caminho}")

gerar_mudancas(1, range(1, 301), "2026-10-01 10:00:00")

# COMMAND ----------

conteudo = """pedido_id,cliente,categoria,cidade,quantidade,preco_unitario,atualizado_em
10,cliente_7,Livros,Belém,2,100.00,2026-10-02 09:00:00
20,cliente_8,Casa,Fortaleza,1,200.00,2026-10-02 09:00:00
30,cliente_9,Esportes,Curitiba,4,300.00,2026-10-02 09:00:00
301,cliente_50,Roupas,Belém,2,150.00,2026-10-02 09:00:00
302,cliente_51,Livros,São Paulo,1,80.00,2026-10-02 09:00:00
"""
dbutils.fs.put(f"{PASTA_CDC}/mudancas_002.csv", conteudo, overwrite=True)
print("Arquivo 2 gravado: 3 correções (10, 20, 30) e 2 pedidos novos (301, 302)")

# COMMAND ----------

conteudo = """pedido_id,cliente,categoria,cidade,quantidade,preco_unitario,atualizado_em
10,cliente_999,Casa,Curitiba,9,999.00,2026-10-01 12:00:00
"""
dbutils.fs.put(f"{PASTA_CDC}/mudancas_003.csv", conteudo, overwrite=True)
print("Arquivo 3 gravado: versão ANTIGA do pedido 10 (2026-10-01 12:00), chegando depois da correção de 2026-10-02")

# COMMAND ----------

