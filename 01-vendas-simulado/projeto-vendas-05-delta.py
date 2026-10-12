# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.vendas.silver_vendas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.vendas.silver_teste AS
# MAGIC SELECT * FROM workspace.vendas.silver_vendas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- erro proposital: apaga todas as vendas de Belém
# MAGIC DELETE FROM workspace.vendas.silver_teste WHERE cidade = 'Belém'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_agora FROM workspace.vendas.silver_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_antes FROM workspace.vendas.silver_teste VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE workspace.vendas.silver_teste TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas_restauradas FROM workspace.vendas.silver_teste

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.vendas.silver_teste

# COMMAND ----------

