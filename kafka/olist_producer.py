"""
olist_producer.py
─────────────────────────────────────────────────────────────────────────────
Producer Kafka para el dataset Brazilian E-commerce Olist.
Lee los CSVs locales y envía mensajes JSON al topic `olist_orders`
simulando un stream de eventos de negocio en tiempo real.

Instalación de dependencias:
    pip install kafka-python pandas

Uso:
    python olist_producer.py --data_path ../data/raw --delay 0.05 --batch 50
─────────────────────────────────────────────────────────────────────────────
"""
import argparse
import json
import time
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("olist_producer")


# ── Configuración del Producer ────────────────────────────────────────────────
def build_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                 # máxima durabilidad
        retries=3,
        max_block_ms=10_000,
        compression_type="gzip",   # reduce tráfico ~60%
    )


# ── Carga y enriquecimiento de datos ─────────────────────────────────────────
def load_olist_orders(data_path: Path) -> pd.DataFrame:
    """
    Une las tablas Olist en un único DataFrame de eventos de orden.
    Genera un payload completo por orden: datos del pedido, items,
    pagos, entrega y review (cuando existe).
    """
    log.info("Cargando CSVs Olist desde: %s", data_path)

    orders   = pd.read_csv(data_path / "olist_orders_dataset.csv")
    items    = pd.read_csv(data_path / "olist_order_items_dataset.csv")
    payments = pd.read_csv(data_path / "olist_order_payments_dataset.csv")
    reviews  = pd.read_csv(data_path / "olist_order_reviews_dataset.csv")
    products = pd.read_csv(data_path / "olist_products_dataset.csv")
    category = pd.read_csv(data_path / "product_category_name_translation.csv")
    sellers  = pd.read_csv(data_path / "olist_sellers_dataset.csv")

    # Enriquecer items con categoría en inglés y estado del vendedor
    products = products.merge(category, on="product_category_name", how="left")
    items = (
        items
        .merge(products[["product_id", "product_category_name_english"]], on="product_id", how="left")
        .merge(sellers[["seller_id", "seller_state", "seller_city"]], on="seller_id", how="left")
    )

    # Agregar items por orden
    items_agg = items.groupby("order_id").agg(
        item_count=("order_item_id", "count"),
        total_items_value=("price", "sum"),
        total_freight=("freight_value", "sum"),
        categories=("product_category_name_english", lambda x: list(x.dropna().unique())),
        seller_states=("seller_state", lambda x: list(x.dropna().unique())),
    ).reset_index()

    # Agregar pagos por orden
    payments_agg = payments.groupby("order_id").agg(
        payment_type=("payment_type", lambda x: x.mode()[0] if not x.empty else None),
        payment_installments=("payment_installments", "max"),
        payment_value=("payment_value", "sum"),
    ).reset_index()

    # Review más reciente por orden
    reviews_latest = (
        reviews.sort_values("review_creation_date")
        .drop_duplicates("order_id", keep="last")
        [["order_id", "review_score", "review_comment_message"]]
    )

    # Join final
    df = (
        orders
        .merge(items_agg, on="order_id", how="left")
        .merge(payments_agg, on="order_id", how="left")
        .merge(reviews_latest, on="order_id", how="left")
    )

    log.info("Dataset cargado: %d órdenes", len(df))
    return df


# ── Transformación a payload de evento ───────────────────────────────────────
def row_to_event(row: dict) -> dict:
    """Construye un payload de evento limpio a partir de una fila del DataFrame."""
    return {
        # Identificadores
        "event_id":            f"{row['order_id']}_{int(time.time()*1000)}",
        "order_id":            row.get("order_id"),
        "customer_id":         row.get("customer_id"),
        # Estado del pedido
        "order_status":        row.get("order_status"),
        "order_purchase_ts":   str(row.get("order_purchase_timestamp", "")),
        "order_approved_ts":   str(row.get("order_approved_at", "")),
        "order_delivered_ts":  str(row.get("order_delivered_customer_date", "")),
        "order_estimated_ts":  str(row.get("order_estimated_delivery_date", "")),
        # Items y logística
        "item_count":          int(row.get("item_count") or 0),
        "categories":          row.get("categories") or [],
        "seller_states":       row.get("seller_states") or [],
        "total_items_value":   float(row.get("total_items_value") or 0.0),
        "total_freight":       float(row.get("total_freight") or 0.0),
        # Pago
        "payment_type":        row.get("payment_type"),
        "payment_installments":int(row.get("payment_installments") or 1),
        "payment_value":       float(row.get("payment_value") or 0.0),
        # Review
        "review_score":        row.get("review_score"),
        # Metadatos de ingesta
        "ingested_at":         datetime.utcnow().isoformat(),
        "source":              "olist_producer_v1",
    }


# ── Callbacks ─────────────────────────────────────────────────────────────────
def on_send_success(metadata):
    log.debug(
        "OK  topic=%s partition=%d offset=%d",
        metadata.topic, metadata.partition, metadata.offset
    )

def on_send_error(exc):
    log.error("ERROR al enviar mensaje: %s", exc)


# ── Loop principal ────────────────────────────────────────────────────────────
def produce(args):
    data_path = Path(args.data_path)
    df = load_olist_orders(data_path)

    producer = build_producer(args.bootstrap_servers)
    log.info("Producer conectado a %s", args.bootstrap_servers)
    log.info("Enviando %d órdenes al topic '%s' (delay=%.3fs, batch=%d)",
             len(df), args.topic, args.delay, args.batch)

    sent = 0
    for _, row in df.iterrows():
        event = row_to_event(row.to_dict())
        key = event["order_id"]

        producer.send(args.topic, key=key, value=event)\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)

        sent += 1
        if sent % args.batch == 0:
            producer.flush()
            log.info("Enviadas %d / %d órdenes", sent, len(df))

        time.sleep(args.delay)

    producer.flush()
    producer.close()
    log.info("✓ Producción completada: %d mensajes enviados", sent)


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist Kafka Producer — LogiLake")
    parser.add_argument("--data_path",         default="../data/raw",    help="Ruta a los CSVs Olist")
    parser.add_argument("--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic",             default="olist_orders",   help="Nombre del topic Kafka")
    parser.add_argument("--delay",             type=float, default=0.05, help="Delay entre mensajes (segundos)")
    parser.add_argument("--batch",             type=int,   default=50,   help="Flush cada N mensajes")
    args = parser.parse_args()
    produce(args)
