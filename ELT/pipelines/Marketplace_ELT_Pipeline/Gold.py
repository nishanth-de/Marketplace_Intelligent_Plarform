from pyspark import pipelines as dp
from pyspark.sql.functions import col, sha2

#==================================================================
# Dimensions
#==================================================================

@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_customer")
def dim_customer():

    customers = spark.read.table(
        "dev_catalog.silver_clean.silver_customers"
    )

    return (
        customers
        .withColumn(
            "customer_key",
            sha2(
                col("customer_id"),
                256
            )
        )
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_seller")
def dim_seller():

    sellers = spark.read.table(
        "dev_catalog.silver_clean.silver_sellers"
    )

    return (
        sellers
        .withColumn(
            "seller_key",
            sha2(
                col("seller_id"),
                256
            )
        )
        .select(
            "seller_key",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "_ingested_at",
            "_source_file"
        )
    )


@dp.materialized_view(name="dev_catalog.gold_warehouse.dim_product")
def dim_product():

    products = spark.read.table(
        "dev_catalog.silver_clean.silver_products"
    )

    return (
        products
        .withColumn(
            "product_key",
            sha2(
                col("product_id"),
                256
            )
        )
        .select(
            "product_key",
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "_ingested_at",
            "_source_file"
        )
    )

#===================================================================
# Facts
#===================================================================
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date


@dp.materialized_view(name="dev_catalog.gold_warehouse.fact_orders")
def fact_orders():

    orders = spark.read.table(
        "dev_catalog.silver_clean.silver_orders"
    ).alias("o")

    customers = spark.read.table(
        "dev_catalog.gold_warehouse.dim_customer"
    ).alias("c")

    dates = spark.read.table(
        "dev_catalog.gold_warehouse.dim_date"
    ).alias("d")

    return (
        orders
        .join(
            customers,
            col("o.customer_id") == col("c.customer_id"),
            "left"
        )
        .join(
            dates,
            to_date(col("o.order_purchase_timestamp"))
            == col("d.date"),
            "left"
        )
        .select(
            col("o.order_id"),
            col("c.customer_key"), # references dim_customer
            col("d.date_key").alias("order_date_key"), # references dim_date

            col("o.order_status"),

            col("o.order_purchase_timestamp"),
            col("o.order_approved_at"),
            col("o.order_delivered_carrier_date"),
            col("o.order_delivered_customer_date"),
            col("o.order_estimated_delivery_date"),

            col("o._ingested_at"),
            col("o._source_file")
        )
    )

@dp.materialized_view(name="dev_catalog.gold_warehouse.fact_order_items")
def fact_order_items():

    items = spark.read.table(
        "dev_catalog.silver_clean.silver_order_items"
    ).alias("i")

    orders = spark.read.table(
        "dev_catalog.silver_clean.silver_orders"
    ).alias("o")

    products = spark.read.table(
        "dev_catalog.gold_warehouse.dim_product"
    ).alias("p")

    sellers = spark.read.table(
        "dev_catalog.gold_warehouse.dim_seller"
    ).alias("s")

    customers = spark.read.table(
        "dev_catalog.gold_warehouse.dim_customer"
    ).alias("c")

    dates = spark.read.table(
        "dev_catalog.gold_warehouse.dim_date"
    ).alias("d")

    return (
        items

        # Order customer/date
        .join(
            orders,
            col("i.order_id") == col("o.order_id"),
            "left"
        )

        # Product lookup
        .join(
            products,
            col("i.product_id") == col("p.product_id"),
            "left"
        )

        # Seller lookup
        .join(
            sellers,
            col("i.seller_id") == col("s.seller_id"),
            "left"
        )

        # Customer lookup
        .join(
            customers,
            col("o.customer_id") == col("c.customer_id"),
            "left"
        )

        # Date lookup
        .join(
            dates,
            to_date(col("o.order_purchase_timestamp"))
            == col("d.date"),
            "left"
        )

        .select(
            col("i.order_id"),
            col("i.order_item_id"),

            col("c.customer_key"), # references dim_customer
            col("p.product_key"),  # references dim_product
            col("s.seller_key"),   # references dim_seller
            col("d.date_key").alias("order_date_key"), # references dim_date

            col("i.shipping_limit_date"),
            col("i.price"),
            col("i.freight_value"),

            col("i._ingested_at"),
            col("i._source_file")
        )
    )

@dp.materialized_view(name="dev_catalog.gold_warehouse.fact_payments")
def fact_payments():

    payments = spark.read.table(
        "dev_catalog.silver_clean.silver_order_payments"
    ).alias("p")

    orders = spark.read.table(
        "dev_catalog.silver_clean.silver_orders"
    ).alias("o")

    customers = spark.read.table(
        "dev_catalog.gold_warehouse.dim_customer"
    ).alias("c")

    dates = spark.read.table(
        "dev_catalog.gold_warehouse.dim_date"
    ).alias("d")

    return (
        payments

        .join(
            orders,
            col("p.order_id") == col("o.order_id"),
            "left"
        )

        .join(
            customers,
            col("o.customer_id") == col("c.customer_id"),
            "left"
        )

        .join(
            dates,
            to_date(col("o.order_purchase_timestamp"))
            == col("d.date"),
            "left"
        )

        .select(
            col("p.order_id"),
            col("p.payment_sequential"),

            col("c.customer_key"),
            col("d.date_key").alias("order_date_key"),

            col("p.payment_type"),
            col("p.payment_installments"),
            col("p.payment_value"),

            col("p._ingested_at"),
            col("p._source_file")
        )
    )
