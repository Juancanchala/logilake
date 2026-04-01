"""
utils/schemas.py
─────────────────────────────────────────────────────────────────────────────
Definición de schemas para cada capa de LogiLake.
Se usa tanto en el producer Kafka como en los notebooks Databricks.
─────────────────────────────────────────────────────────────────────────────
"""
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, ArrayType,
    TimestampType, BooleanType
)

# ── Bronze Schema ─────────────────────────────────────────────────────────────
# Refleja exactamente el payload del producer Kafka (sin casteos)
BRONZE_SCHEMA = StructType([
    StructField("event_id",             StringType(),  nullable=False),
    StructField("order_id",             StringType(),  nullable=False),
    StructField("customer_id",          StringType(),  nullable=True),
    StructField("order_status",         StringType(),  nullable=True),
    StructField("order_purchase_ts",    StringType(),  nullable=True),
    StructField("order_approved_ts",    StringType(),  nullable=True),
    StructField("order_delivered_ts",   StringType(),  nullable=True),
    StructField("order_estimated_ts",   StringType(),  nullable=True),
    StructField("item_count",           IntegerType(), nullable=True),
    StructField("categories",           ArrayType(StringType()), nullable=True),
    StructField("seller_states",        ArrayType(StringType()), nullable=True),
    StructField("total_items_value",    FloatType(),   nullable=True),
    StructField("total_freight",        FloatType(),   nullable=True),
    StructField("payment_type",         StringType(),  nullable=True),
    StructField("payment_installments", IntegerType(), nullable=True),
    StructField("payment_value",        FloatType(),   nullable=True),
    StructField("review_score",         FloatType(),   nullable=True),
    StructField("ingested_at",          StringType(),  nullable=True),
    StructField("source",               StringType(),  nullable=True),
])

# ── Silver Schema ─────────────────────────────────────────────────────────────
# Añade columnas derivadas con tipos correctos y flags de calidad
SILVER_COLUMNS = [
    # IDs
    "order_id", "customer_id",
    # Estado
    "order_status",
    # Timestamps (TimestampType)
    "order_purchase_ts", "order_approved_ts",
    "order_delivered_ts", "order_estimated_ts",
    # Logística
    "item_count", "categories", "seller_states",
    "total_items_value", "total_freight",
    "payment_type", "payment_installments", "payment_value",
    "review_score",
    # Columnas derivadas Silver
    "delivery_days_actual",      # días reales de entrega
    "delivery_days_estimated",   # días estimados
    "is_late_delivery",          # bool: entregó después del estimado
    "is_delivered",              # bool: status == 'delivered'
    "is_canceled",               # bool: status == 'canceled'
    "order_value_total",         # items + freight
    # Calidad
    "dq_passed",                 # bool: pasó todas las validaciones
    "dq_flags",                  # string: lista de flags de calidad
    # Metadatos
    "silver_processed_at",
]

# ── Gold Schema ───────────────────────────────────────────────────────────────
# KPIs agregados para el dashboard
GOLD_METRICS = {
    # OTIF: On Time In Full
    "otif_rate":               "Porcentaje de órdenes entregadas a tiempo y completas",
    "avg_delivery_days":       "Promedio de días de entrega reales",
    "avg_estimated_days":      "Promedio de días estimados",
    "delivery_delay_days":     "Retraso promedio vs estimado (puede ser negativo)",
    # Revenue
    "total_revenue":           "Suma de payment_value",
    "avg_order_value":         "Ticket promedio por orden",
    "revenue_by_category":     "Revenue por categoría de producto",
    "revenue_by_state":        "Revenue por estado del vendedor",
    # Calidad de servicio
    "cancellation_rate":       "Tasa de cancelación de órdenes",
    "avg_review_score":        "NPS proxy — promedio de review_score (1-5)",
    "pct_5star":               "Porcentaje de órdenes con review 5 estrellas",
    # Volumen
    "total_orders":            "Total de órdenes procesadas",
    "orders_by_status":        "Distribución de órdenes por estado",
    "orders_by_month":         "Evolución mensual de órdenes",
    # Logística
    "freight_ratio":           "Relación flete / valor de items",
    "installments_avg":        "Cuotas promedio por pago",
}
