"""
utils/delta_helpers.py — LogiLake
Funciones de utilidad para operaciones Delta Lake.
PySpark 3.5.0 + Delta Lake 3.1.0 local (WSL2).
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import logging

log = logging.getLogger("delta_helpers")


def write_batch_delta(df: DataFrame, path: str, mode: str = "overwrite",
                      partition_by: list = None) -> None:
    """Escribe un batch DataFrame a Delta Lake (Bronze / Silver / Gold)."""
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    log.info("Delta escrito: %s", path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Lee una tabla Delta Lake como batch DataFrame."""
    return spark.read.format("delta").load(path)


def upsert_delta(spark: SparkSession, source_df: DataFrame,
                 target_path: str, merge_key: str) -> None:
    """
    MERGE (upsert) sobre una tabla Delta Lake.
    Si la tabla no existe la crea. Util para Silver — evita duplicados.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        write_batch_delta(source_df, target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("target")
        .merge(source_df.alias("source"),
               f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("MERGE completado: %s", target_path)


def table_exists(spark: SparkSession, path: str) -> bool:
    """Retorna True si existe una tabla Delta en el path indicado."""
    return DeltaTable.isDeltaTable(spark, path)


def get_row_count(spark: SparkSession, path: str) -> int:
    """Cuenta las filas de una tabla Delta sin cargarla completa en memoria."""
    return spark.read.format("delta").load(path).count()


def add_dq_flags(df: DataFrame) -> DataFrame:
    """
    Agrega columnas de calidad de datos al DataFrame Silver.
    dq_passed: bool — True si paso todas las validaciones.
    dq_flags:  str  — issues encontrados separados por pipe.
    """
    flags = F.array(
        F.when(F.col("order_id").isNull(),
               F.lit("NULL_ORDER_ID")).otherwise(F.lit(None)),
        F.when(F.col("payment_value") <= 0,
               F.lit("ZERO_PAYMENT")).otherwise(F.lit(None)),
        F.when(F.col("item_count").isNull() | (F.col("item_count") < 1),
               F.lit("INVALID_ITEM_COUNT")).otherwise(F.lit(None)),
        F.when(
            F.col("order_delivered_ts").isNotNull() &
            F.col("order_purchase_ts").isNotNull() &
            (F.col("order_delivered_ts") < F.col("order_purchase_ts")),
            F.lit("DELIVERY_BEFORE_PURCHASE")).otherwise(F.lit(None)),
        F.when(
            F.col("review_score").isNotNull() & (F.col("review_score") > 5),
            F.lit("INVALID_REVIEW_SCORE")).otherwise(F.lit(None)),
    )

    flags_clean = F.array_join(F.filter(flags, lambda x: x.isNotNull()), "|")

    return (
        df.withColumn("dq_flags", flags_clean)
          .withColumn("dq_passed",
                      F.size(F.filter(flags, lambda x: x.isNotNull())) == 0)
    )
