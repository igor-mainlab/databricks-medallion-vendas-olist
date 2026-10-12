from pyspark import pipelines as dp
from pyspark.sql import functions as F

PASTA_LANDING = "/Volumes/workspace/vendas/landing/vendas"

@dp.table(
    name="bronze_vendas",
    comment="Vendas brutas ingeridas da landing via Auto Loader"
)
def bronze_vendas():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.inferColumnTypes", False)
        .load(PASTA_LANDING)
        .withColumn("_ingestao_ts", F.current_timestamp())
        .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
    )

@dp.table(
    name="silver_vendas",
    comment="Vendas limpas, tipadas e sem duplicatas"
)
@dp.expect("categoria_informada", "categoria IS NOT NULL AND trim(categoria) != ''")
@dp.expect("quantidade_informada", "quantidade IS NOT NULL")
@dp.expect_or_drop("pedido_id_valido", "pedido_id IS NOT NULL")
@dp.expect_or_drop("data_valida", "data_pedido IS NOT NULL")
@dp.expect_or_drop("preco_positivo", "preco_unitario > 0")
@dp.expect_or_fail("quantidade_nao_negativa", "quantidade IS NULL OR quantidade >= 0")
def silver_vendas():
    df = spark.read.table("bronze_vendas")  # ver nota abaixo
    return (
        df
        .dropDuplicates(["pedido_id"])
        .withColumn("pedido_id", F.col("pedido_id").cast("int"))
        .withColumn("data_pedido", F.to_date("data_pedido", "dd/MM/yyyy"))
        .withColumn("quantidade", F.col("quantidade").cast("int"))
        .withColumn("preco_unitario", F.col("preco_unitario").cast("double"))
        .withColumn("canal_venda", F.coalesce(F.col("canal_venda"), F.lit("não informado")))
        .drop("_ingestao_ts", "_arquivo_origem", "_rescued_data")
    )

@dp.materialized_view(
    name="gold_vendas_mensal",
    comment="Receita e pedidos por mês"
)
def gold_vendas_mensal():
    s = spark.read.table("silver_vendas")
    return (
        s
        .withColumn("valor_total", F.round(F.coalesce("quantidade", F.lit(1)) * F.col("preco_unitario"), 2))
        .withColumn("mes", F.date_format("data_pedido", "yyyy-MM"))
        .groupBy("mes")
        .agg(
            F.countDistinct("pedido_id").alias("total_pedidos"),
            F.sum(F.coalesce("quantidade", F.lit(1))).alias("itens_vendidos"),
            F.round(F.sum("valor_total"), 2).alias("receita_total"),
            F.round(F.avg("valor_total"), 2).alias("ticket_medio")
        )
    )


@dp.materialized_view(
    name="gold_vendas_categoria",
    comment="Receita e pedidos por categoria"
)
def gold_vendas_categoria():
    s = spark.read.table("silver_vendas")
    return (
        s
        .withColumn("valor_total", F.round(F.coalesce("quantidade", F.lit(1)) * F.col("preco_unitario"), 2))
        .withColumn(
            "categoria",
            F.when(F.col("categoria").isNull() | (F.trim("categoria") == ""), F.lit("Não informada"))
             .otherwise(F.col("categoria"))
        )
        .groupBy("categoria")
        .agg(
            F.countDistinct("pedido_id").alias("total_pedidos"),
            F.sum(F.coalesce("quantidade", F.lit(1))).alias("itens_vendidos"),
            F.round(F.sum("valor_total"), 2).alias("receita_total")
        )
    )


@dp.materialized_view(
    name="gold_vendas_cidade",
    comment="Receita e ticket médio por cidade"
)
def gold_vendas_cidade():
    s = spark.read.table("silver_vendas")
    return (
        s
        .withColumn("valor_total", F.round(F.coalesce("quantidade", F.lit(1)) * F.col("preco_unitario"), 2))
        .groupBy("cidade")
        .agg(
            F.countDistinct("pedido_id").alias("total_pedidos"),
            F.round(F.sum("valor_total"), 2).alias("receita_total"),
            F.round(F.avg("valor_total"), 2).alias("ticket_medio")
        )
    )

PASTA_CDC = "/Volumes/workspace/vendas/landing_cdc/pedidos"


@dp.table(name="bronze_pedidos_cdc", comment="Mudanças de pedidos recebidas")
def bronze_pedidos_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("cloudFiles.inferColumnTypes", True)
        .load(PASTA_CDC)
    )


dp.create_streaming_table(
    name="silver_pedidos_cdc",
    comment="Estado atual de cada pedido, mantido por AUTO CDC",
)

dp.create_auto_cdc_flow(
    target="silver_pedidos_cdc",
    source="bronze_pedidos_cdc",
    keys=["pedido_id"],
    sequence_by=F.col("atualizado_em"),
    stored_as_scd_type=1,
)