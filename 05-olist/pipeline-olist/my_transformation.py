from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/olist/raw"


def ler_csv(nome_arquivo):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{BASE}/{nome_arquivo}")
        .withColumn("_ingestao_ts", F.current_timestamp())
        .withColumn("_arquivo_origem", F.lit(nome_arquivo))
    )


@dp.materialized_view(name="bronze_orders", comment="Pedidos brutos")
def bronze_orders():
    return ler_csv("olist_orders_dataset.csv")


@dp.materialized_view(name="bronze_order_items", comment="Itens dos pedidos brutos")
def bronze_order_items():
    return ler_csv("olist_order_items_dataset.csv")


@dp.materialized_view(name="bronze_customers", comment="Clientes brutos")
def bronze_customers():
    return ler_csv("olist_customers_dataset.csv")


@dp.materialized_view(name="bronze_products", comment="Produtos brutos")
def bronze_products():
    return ler_csv("olist_products_dataset.csv")


@dp.materialized_view(name="bronze_payments", comment="Pagamentos brutos")
def bronze_payments():
    return ler_csv("olist_order_payments_dataset.csv")


@dp.materialized_view(name="bronze_category_translation", comment="Tradução das categorias para inglês")
def bronze_category_translation():
    return ler_csv("product_category_name_translation.csv")

@dp.materialized_view(name="silver_orders", comment="Pedidos tipados")
@dp.expect("data_aprovacao_informada", "order_approved_at IS NOT NULL")
@dp.expect("entregue_com_data", "order_status != 'delivered' OR order_delivered_customer_date IS NOT NULL")
@dp.expect_or_drop("pedido_id_valido", "order_id IS NOT NULL")
@dp.expect_or_drop("compra_valida", "order_purchase_timestamp IS NOT NULL")
def silver_orders():
    return (
        spark.read.table("bronze_orders")
        .dropDuplicates(["order_id"])
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        .drop("_ingestao_ts", "_arquivo_origem")
    )


@dp.materialized_view(name="silver_order_items", comment="Itens tipados")
@dp.expect_or_drop("item_com_pedido", "order_id IS NOT NULL")
@dp.expect_or_drop("preco_positivo", "price > 0")
@dp.expect("frete_nao_negativo", "freight_value >= 0")
def silver_order_items():
    return (
        spark.read.table("bronze_order_items")
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .drop("_ingestao_ts", "_arquivo_origem")
    )

@dp.materialized_view(name="silver_payments", comment="Pagamentos tipados")
@dp.expect_or_drop("pagamento_com_pedido", "order_id IS NOT NULL")
@dp.expect("valor_nao_negativo", "payment_value >= 0")
def silver_payments():
    return (
        spark.read.table("bronze_payments")
        .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .drop("_ingestao_ts", "_arquivo_origem")
    )

@dp.materialized_view(name="silver_customers", comment="Clientes por pedido (customer_id)")
@dp.expect_or_drop("cliente_id_valido", "customer_id IS NOT NULL")
@dp.expect("estado_informado", "customer_state IS NOT NULL")
def silver_customers():
    return (
        spark.read.table("bronze_customers")
        .dropDuplicates(["customer_id"])
        .drop("_ingestao_ts", "_arquivo_origem")
    )


@dp.materialized_view(name="silver_products", comment="Produtos tipados")
@dp.expect_or_drop("produto_id_valido", "product_id IS NOT NULL")
@dp.expect("categoria_informada", "product_category_name IS NOT NULL")
def silver_products():
    return (
        spark.read.table("bronze_products")
        .dropDuplicates(["product_id"])
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .withColumn("product_name_length", F.col("product_name_length").cast("int"))
        .withColumn("product_description_length", F.col("product_description_length").cast("int"))
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast("int"))
        .withColumn("product_weight_g", F.col("product_weight_g").cast("double"))
        .withColumn("product_length_cm", F.col("product_length_cm").cast("double"))
        .withColumn("product_height_cm", F.col("product_height_cm").cast("double"))
        .withColumn("product_width_cm", F.col("product_width_cm").cast("double"))
        .drop("_ingestao_ts", "_arquivo_origem")
    )

def base_itens_entregues():
    pedidos = (
        spark.read.table("silver_orders")
        .filter(F.col("order_status") == "delivered")
        .select("order_id", "customer_id", "order_purchase_timestamp")
    )
    itens = spark.read.table("silver_order_items").select("order_id", "product_id", "price", "freight_value")
    return itens.join(pedidos, "order_id", "inner")


@dp.materialized_view(name="gold_receita_mensal", comment="Receita de pedidos entregues por mês de compra")
def gold_receita_mensal():
    return (
        base_itens_entregues()
        .withColumn("mes", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("mes")
        .agg(
            F.countDistinct("order_id").alias("pedidos"),
            F.count("*").alias("itens"),
            F.round(F.sum("price"), 2).alias("receita"),
            F.round(F.sum("freight_value"), 2).alias("frete"),
        )
        .withColumn("ticket_medio", F.round(F.col("receita") / F.col("pedidos"), 2))
    )


@dp.materialized_view(name="gold_receita_categoria", comment="Receita de pedidos entregues por categoria")
def gold_receita_categoria():
    produtos = spark.read.table("silver_products").select("product_id", "product_category_name")
    traducao = (
        spark.read.table("bronze_category_translation")
        .select("product_category_name", "product_category_name_english")
        .dropDuplicates(["product_category_name"])
    )
    return (
        base_itens_entregues()
        .join(produtos, "product_id", "left")
        .join(traducao, "product_category_name", "left")
        .withColumn(
            "categoria",
            F.coalesce(F.col("product_category_name_english"), F.col("product_category_name"), F.lit("sem_categoria")),
        )
        .groupBy("categoria")
        .agg(
            F.countDistinct("order_id").alias("pedidos"),
            F.count("*").alias("itens"),
            F.round(F.sum("price"), 2).alias("receita"),
        )
    )


@dp.materialized_view(name="gold_receita_estado", comment="Receita de pedidos entregues por estado do cliente")
def gold_receita_estado():
    clientes = spark.read.table("silver_customers").select("customer_id", "customer_state")
    return (
        base_itens_entregues()
        .join(clientes, "customer_id", "left")
        .groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("pedidos"),
            F.round(F.sum("price"), 2).alias("receita"),
        )
        .withColumn("ticket_medio", F.round(F.col("receita") / F.col("pedidos"), 2))
    )


@dp.materialized_view(name="gold_entrega_estado", comment="Prazo e atraso de entrega por estado")
def gold_entrega_estado():
    clientes = spark.read.table("silver_customers").select("customer_id", "customer_state")
    return (
        spark.read.table("silver_orders")
        .filter((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull())
        .join(clientes, "customer_id", "left")
        .withColumn(
            "dias_atraso",
            F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_estimated_delivery_date")),
        )
        .withColumn(
            "dias_entrega",
            F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_purchase_timestamp")),
        )
        .groupBy("customer_state")
        .agg(
            F.count("*").alias("pedidos_entregues"),
            F.sum(F.when(F.col("dias_atraso") > 0, 1).otherwise(0)).alias("pedidos_atrasados"),
            F.round(F.avg("dias_entrega"), 1).alias("media_dias_entrega"),
        )
        .withColumn("pct_atrasados", F.round(100 * F.col("pedidos_atrasados") / F.col("pedidos_entregues"), 2))
    )