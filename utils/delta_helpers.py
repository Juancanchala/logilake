"""
utils/delta_helpers.py
─────────────────────────────────────────────────────────────────────────────
Funciones de utilidad para operaciones Delta Lake en Databricks.
Importar en notebooks: %run ../utils/delta_helpers
─────────────────────────────────────────────────────────────────────────────
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

log = logging.getLogger("delta_helpers")


# ── Escritura ─────────────────────────────────────────────────────────────────
def write_bronze(df: DataFrame, path: str, checkpoint_path: str, trigger_seconds: int = 30):
    """
    Escribe un streaming DataFrame a Delta Lake (Bronze).
    Usa Append mode con schema evolution habilitado.
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start(path)
    )


def write_batch_delta(df: DataFrame, path: str, mode: str = "overwrite",
                      partition_by: list = None):
    """
    Escribe un batch DataFrame a Delta Lake (Silver / Gold).
    """
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    log.info("Escritura Delta completada: %s (%d filas)", path, df.count())


# ── MERGE / UPSERT ────────────────────────────────────────────────────────────
def upsert_delta(spark: SparkSession, source_df: DataFrame,
                 target_path: str, merge_key: str):
    """
    Realiza un MERGE (upsert) en una tabla Delta Lake.
    Útil para Silver → evitar duplicados de order_id.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        write_batch_delta(source_df, target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("MERGE completado sobre %s", target_path)


# ── Lectura ───────────────────────────────────────────────────────────────────
def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Lee una tabla Delta Lake como batch DataFrame."""
    return spark.read.format("delta").load(path)


def read_delta_stream(spark: SparkSession, path: str,
                      max_files_per_trigger: int = 100) -> DataFrame:
    """Lee una tabla Delta Lake como streaming DataFrame (para Silver)."""
    return (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(path)
    )


# ── Optimización ──────────────────────────────────────────────────────────────
def optimize_delta(spark: SparkSession, path: str, z_order_cols: list = None):
    """
    Compacta archivos pequeños y opcionalmente aplica Z-Order.
    Ejecutar después de cargas grandes en Silver/Gold.
    """
    spark.sql(f"OPTIMIZE delta.`{path}`")
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    log.info("OPTIMIZE aplicado sobre %s", path)


def vacuum_delta(spark: SparkSession, path: str, retain_hours: int = 168):
    """
    Elimina archivos históricos fuera de la ventana de retención.
    Default: 7 días (168h). Requiere spark.databricks.delta.retentionDurationCheck.enabled=false
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    log.info("VACUUM aplicado sobre %s (retención: %dh)", path, retain_hours)


# ── Calidad de datos ──────────────────────────────────────────────────────────
def add_dq_flags(df: DataFrame) -> DataFrame:
    """
    Agrega columnas de calidad de datos al DataFrame Silver.
    dq_passed: bool — True si la fila pasó todas las validaciones.
    dq_flags:  string — lista de issues encontrados.
    """
    flags = F.array(
        F.when(F.col("order_id").isNull(),           F.lit("NULL_ORDER_ID")).otherwise(F.lit(None)),
        F.when(F.col("payment_value") <= 0,          F.lit("ZERO_PAYMENT")).otherwise(F.lit(None)),
        F.when(F.col("item_count").isNull() | (F.col("item_count") < 1),
                                                     F.lit("INVALID_ITEM_COUNT")).otherwise(F.lit(None)),
        F.when(
            F.col("order_delivered_ts").isNotNull() &
            F.col("order_estimated_ts").isNotNull() &
            (F.col("order_delivered_ts") < F.col("order_purchase_ts")),
                                                     F.lit("DELIVERY_BEFORE_PURCHASE")).otherwise(F.lit(None)),
        F.when(F.col("review_score") > 5,            F.lit("INVALID_REVIEW_SCORE")).otherwise(F.lit(None)),
    )

    # Filtra nulls y convierte a string
    flags_clean = F.array_join(
        F.filter(flags, lambda x: x.isNotNull()), "|"
    )

    return df.withColumn("dq_flags", flags_clean)\
             .withColumn("dq_passed", F.size(F.filter(flags, lambda x: x.isNotNull())) == 0)


# ── Metadatos ─────────────────────────────────────────────────────────────────
def get_table_stats(spark: SparkSession, path: str) -> dict:
    """Retorna estadísticas básicas de una tabla Delta Lake."""
    history = DeltaTable.forPath(spark, path).history(1).collect()[0]
    df = read_delta(spark, path)
    return {
        "path":          path,
        "row_count":     df.count(),
        "version":       history["version"],
        "timestamp":     str(history["timestamp"]),
        "operation":     history["operation"],
    }
