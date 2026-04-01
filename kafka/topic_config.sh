#!/usr/bin/env bash
# topic_config.sh — Crea el topic olist_orders en Kafka local
# Uso: bash topic_config.sh [create|describe|list|delete]

BROKER="localhost:9092"
TOPIC="olist_orders"
PARTITIONS=3
REPLICATION=1

case "$1" in
  create)
    echo ">>> Creando topic: $TOPIC"
    docker exec logilake-kafka kafka-topics \
      --bootstrap-server $BROKER \
      --create \
      --topic $TOPIC \
      --partitions $PARTITIONS \
      --replication-factor $REPLICATION \
      --if-not-exists
    echo ">>> Topic creado. Verificando..."
    docker exec logilake-kafka kafka-topics \
      --bootstrap-server $BROKER \
      --describe --topic $TOPIC
    ;;
  list)
    docker exec logilake-kafka kafka-topics \
      --bootstrap-server $BROKER --list
    ;;
  describe)
    docker exec logilake-kafka kafka-topics \
      --bootstrap-server $BROKER \
      --describe --topic $TOPIC
    ;;
  delete)
    echo ">>> Eliminando topic: $TOPIC"
    docker exec logilake-kafka kafka-topics \
      --bootstrap-server $BROKER \
      --delete --topic $TOPIC
    ;;
  *)
    echo "Uso: bash topic_config.sh [create|describe|list|delete]"
    ;;
esac
