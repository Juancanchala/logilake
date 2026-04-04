"""
utils/schemas.py — LogiLake
Schemas PySpark para cada capa del pipeline.
PySpark 3.5.0 + Delta Lake 3.1.0 local (WSL2).
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    TimestampType, BooleanType
)

# Bronze schemas — 9 CSVs Olist tal como llegan en data/raw/
BRONZE_ORDERS = StructType([
    StructField("order_id",                       StringType(),    False),
    StructField("customer_id",                    StringType(),    True),
    StructField("order_status",                   StringType(),    True),
    StructField("order_purchase_timestamp",       TimestampType(), True),
    StructField("order_approved_at",              TimestampType(), True),
    StructField("order_delivered_carrier_date",   TimestampType(), True),
    StructField("order_delivered_customer_date",  TimestampType(), True),
    StructField("order_estimated_delivery_date",  TimestampType(), True),
])
BRONZE_ORDER_ITEMS = StructType([
    StructField("order_id",            StringType(),    False),
    StructField("order_item_id",       IntegerType(),   True),
    StructField("product_id",          StringType(),    True),
    StructField("seller_id",           StringType(),    True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price",               DoubleType(),    True),
    StructField("freight_value",       DoubleType(),    True),
])
BRONZE_ORDER_PAYMENTS = StructType([
    StructField("order_id",             StringType(),  False),
    StructField("payment_sequential",   IntegerType(), True),
    StructField("payment_type",         StringType(),  True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value",        DoubleType(),  True),
])
BRONZE_ORDER_REVIEWS = StructType([
    StructField("review_id",               StringType(),    True),
    StructField("order_id",                StringType(),    False),
    StructField("review_score",            IntegerType(),   True),
    StructField("review_comment_title",    StringType(),    True),
    StructField("review_comment_message",  StringType(),    True),
    StructField("review_creation_date",    TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])
BRONZE_CUSTOMERS = StructType([
    StructField("customer_id",               StringType(), False),
    StructField("customer_unique_id",        StringType(), True),
    StructField("customer_zip_code_prefix",  StringType(), True),
    StructField("customer_city",             StringType(), True),
    StructField("customer_state",            StringType(), True),
])
BRONZE_SELLERS = StructType([
    StructField("seller_id",              StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city",            StringType(), True),
    StructField("seller_state",           StringType(), True),
])
BRONZE_PRODUCTS = StructType([
    StructField("product_id",                 StringType(), False),
    StructField("product_category_name",      StringType(), True),
    StructField("product_name_lenght",        DoubleType(), True),
    StructField("product_description_lenght", DoubleType(), True),
    StructField("product_photos_qty",         DoubleType(), True),
    StructField("product_weight_g",           DoubleType(), True),
    StructField("product_length_cm",          DoubleType(), True),
    StructField("product_height_cm",          DoubleType(), True),
    StructField("product_width_cm",           DoubleType(), True),
])
BRONZE_GEOLOCATION = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat",             DoubleType(), True),
    StructField("geolocation_lng",             DoubleType(), True),
    StructField("geolocation_city",            StringType(), True),
    StructField("geolocation_state",           StringType(), True),
])
BRONZE_CATEGORY_TRANSLATION = StructType([
    StructField("product_category_name",         StringType(), False),
    StructField("product_category_name_english",  StringType(), True),
])
BRONZE_SCHEMAS = {
    "orders":               BRONZE_ORDERS,
    "order_items":          BRONZE_ORDER_ITEMS,
    "order_payments":       BRONZE_ORDER_PAYMENTS,
    "order_reviews":        BRONZE_ORDER_REVIEWS,
    "customers":            BRONZE_CUSTOMERS,
    "sellers":              BRONZE_SELLERS,
    "products":             BRONZE_PRODUCTS,
    "geolocation":          BRONZE_GEOLOCATION,
    "category_translation": BRONZE_CATEGORY_TRANSLATION,
}

# Silver — columnas de la tabla enriquecida (JOIN de las 9 tablas Bronze)
SILVER_COLUMNS = {
    "order_id":                StringType(),
    "customer_id":             StringType(),
    "order_status":            StringType(),
    "order_purchase_ts":       TimestampType(),
    "order_approved_ts":       TimestampType(),
    "order_delivered_ts":      TimestampType(),
    "order_estimated_ts":      TimestampType(),
    "item_count":              LongType(),
    "total_items_value":       DoubleType(),
    "total_freight":           DoubleType(),
    "payment_type":            StringType(),
    "payment_installments":    DoubleType(),
    "payment_value":           DoubleType(),
    "review_score":            DoubleType(),
    "delivery_days_actual":    DoubleType(),
    "delivery_days_estimated": DoubleType(),
    "delay_days":              DoubleType(),
    "is_delivered":            BooleanType(),
    "is_canceled":             BooleanType(),
    "is_late_delivery":        BooleanType(),
    "dq_passed":               BooleanType(),
    "dq_flags":                StringType(),
    "bronze_ingested_at":      TimestampType(),
    "silver_processed_at":     TimestampType(),
}

# Gold — tablas producidas por 03_gold_kpis.ipynb
GOLD_TABLES = {
    "kpi_global":       "Totales globales del dataset - 1 fila",
    "kpi_monthly":      "KPIs por mes (order_month YYYY-MM)",
    "kpi_category":     "KPIs por categoria de producto (ingles)",
    "kpi_nps":          "Distribucion de review scores 1-5",
    "kpi_seller_state": "KPIs por estado de Brasil del vendedor",
}

# Serving — contrato de datos con la capa BI (no cambiar sin versionar)
SERVING_CONTRACT = {
    "kpi_global.csv": [
        "total_orders","total_delivered","total_canceled",
        "otif_rate_pct","avg_delivery_days_actual","avg_delivery_days_estimated",
        "avg_delay_days","total_revenue_brl","avg_order_value_brl",
        "avg_review_score","avg_freight_ratio_pct","gold_computed_at",
    ],
    "kpi_monthly.csv": [
        "order_month","orders","delivered","canceled",
        "otif_rate_pct","avg_delivery_days","avg_delay_days",
        "revenue_brl","avg_review_score","cancellation_rate_pct",
    ],
    "kpi_nps.csv": [
        "review_score_int","orders","avg_payment_value",
        "avg_delivery_days","pct_of_total",
    ],
    "kpi_category.csv": [
        "category","orders","revenue_brl","avg_order_value",
        "avg_review_score","otif_rate_pct",
    ],
    "kpi_seller_state.csv": [
        "seller_state","orders","avg_delivery_days","avg_delay_days",
        "otif_rate_pct","revenue_brl","avg_review_score",
    ],
}
