# LogiLake
**Data Engineering Portfolio Project — D'LOGIA**

Pipeline completo de ingenieria de datos sobre el dataset **Brazilian E-commerce Olist** (100k ordenes reales),
implementando una **arquitectura Medallion** con Kafka, PySpark y Delta Lake.
El producto final es un **dashboard interactivo con SQL Agent** (GPT-4o + DuckDB WASM)
desplegado en [dlogia.tech/logilake](https://dlogia.tech/logilake).

---

## Arquitectura

```
Olist CSVs (data/raw/)
       |
       v
[Kafka Producer]  --> Topic: olist_orders
 olist_producer.py     (Docker local, puerto 9092)
                              |
                              v
                   +---------------------+
                   |   BRONZE LAYER      |  Raw ingest
                   |   Spark Streaming   |  Delta Lake
                   |   + batch CSV mode  |  data/bronze/
                   +--------+------------+
                            |
                            v
                   +---------------------+
                   |   SILVER LAYER      |  Limpieza + DQ
                   |   Timestamps cast   |  Delta Lake
                   |   Columnas OTIF     |  data/silver/
                   |   DQ flags + MERGE  |
                   +--------+------------+
                            |
                            v
                   +---------------------+
                   |   GOLD LAYER        |  KPIs negocio
                   |   OTIF Rate         |  Delta Lake
                   |   Revenue analytics |  data/gold/
                   |   NPS proxy         |
                   +--------+------------+
                            |
                            v
                   +---------------------+
                   |   SERVING           |  CSVs estaticos
                   |   pandas to_csv()   |  data/serving/
                   |   UTF-8, header     |  -> dashboard/data/
                   +--------+------------+
                            |
                            v
                   +---------------------+
                   |   DASHBOARD         |  Netlify
                   |   HTML + JS         |  dlogia.tech/logilake
                   |   Charts (KPIs)     |
                   |   SQL Agent         |
                   |   GPT-4o + DuckDB   |
                   +---------------------+
```

---

## Stack Tecnico

| Capa | Tecnologia |
|---|---|
| **Ingesta** | Apache Kafka 7.5 (Docker) + Spark Structured Streaming |
| **Procesamiento** | PySpark 3.5.0 + Delta Lake 3.1.0 (Bronze -> Silver -> Gold) |
| **Export** | pandas `to_csv()` — CSVs estaticos en `./data/serving/` |
| **Producto** | Dashboard HTML + SQL Agent (GPT-4o + DuckDB WASM) |
| **Deploy** | Netlify — [dlogia.tech/logilake](https://dlogia.tech/logilake) |

---

## Dataset

**[Brazilian E-commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** — Kaggle

| Archivo | Descripcion | Registros |
|---|---|---|
| olist_orders_dataset.csv | Pedidos principales | ~100k |
| olist_order_items_dataset.csv | Items por pedido | ~113k |
| olist_order_payments_dataset.csv | Pagos | ~104k |
| olist_order_reviews_dataset.csv | Reviews de clientes | ~100k |
| olist_products_dataset.csv | Catalogo de productos | ~33k |
| olist_sellers_dataset.csv | Vendedores | ~3k |
| product_category_name_translation.csv | Traduccion categorias | 71 |

---

## Setup Local

### 1. Clonar e instalar dependencias

```bash
git clone https://github.com/Juancanchala/logilake.git
cd logilake
pip install -r requirements.txt
```

> **Windows**: requiere Java 11 o 17 instalado y `JAVA_HOME` configurado.
> Descargar desde [adoptium.net](https://adoptium.net).

### 2. Descargar dataset Olist

```bash
pip install kaggle
# Configurar API key en ~/.kaggle/kaggle.json
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/
```

### 3. Levantar Kafka en Docker (opcional)

```bash
cd kafka
docker-compose up -d
bash topic_config.sh create
# Verificar en Kafka UI: http://localhost:8080
cd ..
```

> Si no tienes Docker, el notebook Bronze incluye modo batch directo desde CSV.

### 4. Ejecutar notebooks en orden

```bash
jupyter notebook
```

```
01_bronze_ingest.ipynb    -> data/bronze/   (Delta Lake)
02_silver_transform.ipynb -> data/silver/   (Delta Lake)
03_gold_kpis.ipynb        -> data/gold/     (Delta Lake)
04_export_serving.ipynb   -> data/serving/  (CSVs)
```

### 5. Copiar CSVs al dashboard y desplegar

```bash
cp data/serving/*.csv dashboard/data/
# Despliegue automatico via Netlify en cada push a main
```

---

## KPIs

| KPI | Descripcion | Tabla |
|---|---|---|
| **OTIF Rate** | % ordenes entregadas a tiempo y completas | kpi_global |
| **Avg Delivery Days** | Dias promedio de entrega real | kpi_monthly |
| **Delivery Delay** | Retraso promedio vs estimado | kpi_monthly |
| **Cancellation Rate** | % ordenes canceladas | kpi_monthly |
| **Total Revenue** | Revenue total en BRL | kpi_global |
| **Avg Order Value** | Ticket promedio | kpi_global |
| **Avg Review Score** | NPS proxy (1-5 estrellas) | kpi_global |
| **Revenue por Categoria** | Top categorias por revenue | kpi_category |
| **OTIF por Estado** | Performance de entrega por estado BR | kpi_seller_state |

---

## Estructura del Repositorio

```
logilake/
|-- kafka/
|   |-- docker-compose.yml       # Kafka + Zookeeper + Kafka UI
|   |-- olist_producer.py        # Producer CSV -> Kafka
|   +-- topic_config.sh          # Gestion del topic
|-- notebooks/
|   |-- bronze/01_bronze_ingest.ipynb
|   |-- silver/02_silver_transform.ipynb
|   |-- gold/03_gold_kpis.ipynb
|   +-- serving/04_export_serving.ipynb
|-- utils/
|   |-- schemas.py               # Schemas PySpark por capa
|   +-- delta_helpers.py         # Helpers de Delta Lake
|-- dashboard/
|   |-- index.html               # Dashboard principal
|   |-- js/
|   |   |-- charts.js            # Visualizaciones KPIs
|   |   +-- agent.js             # SQL Agent (GPT-4o + DuckDB WASM)
|   +-- data/                    # CSVs Gold (copiados desde data/serving/)
|-- data/
|   |-- raw/                     # CSVs Olist (gitignored)
|   |-- bronze/                  # Delta Lake Bronze (gitignored)
|   |-- silver/                  # Delta Lake Silver (gitignored)
|   |-- gold/                    # Delta Lake Gold (gitignored)
|   +-- serving/                 # CSVs exportados (gitignored)
|-- requirements.txt
|-- .gitignore
+-- README.md
```

---

## Notas de Arquitectura

### SparkSession local con Delta Lake

Cada notebook configura su propia `SparkSession` usando `configure_spark_with_delta_pip`
de `delta-spark`, que descarga automaticamente los JARs necesarios la primera vez.

### SQL Agent con DuckDB WASM

El dashboard incluye un agente conversacional que permite hacer preguntas en lenguaje
natural sobre los datos. GPT-4o genera SQL, DuckDB WASM lo ejecuta en el navegador
sobre los CSVs cargados, y el resultado se renderiza como tabla o grafico.

### Por que Kafka si solo corre local

Kafka simula el escenario real de streaming de eventos de un OMS en tiempo real.
El producer Python reemplaza la fuente real y demuestra la integracion completa
Bronze <- Spark Structured Streaming <- Kafka. El notebook Bronze incluye
**modo batch** como alternativa cuando Kafka no esta disponible.

---

## Autor

**Juan Camilo** — Data & AI Engineer
**D'LOGIA** | [dlogia.tech](https://dlogia.tech) | [github.com/Juancanchala](https://github.com/Juancanchala)
Medellin, Colombia
