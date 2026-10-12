# Databricks notebook source
print("Olá, Databricks!")
print(spark.version)

# COMMAND ----------

df = spark.range(1, 6)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS usuario, current_date() AS hoje