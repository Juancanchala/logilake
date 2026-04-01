# LogiLake 🏭
**Data Engineering Portfolio Project — D'LOGIA**

Pipeline completo de ingeniería de datos sobre el dataset **Brazilian E-commerce Olist** (100k órdenes reales), implementando una **arquitectura Medallion** con Kafka, PySpark, Delta Lake y Databricks.

---

## Arquitectura

```
Olist CSVs (local)
       │
       ▼
[Kafka Producer]  ──► Topic: olist_orders
 olist_producer.py      (Docker local)
                               │
                               ▼
                    ┌─────────────────────┐
                    │   BRONZE LAYER      │  Raw ingest
                    │   Spark Streaming   │  Delta Lake
                    │   + metadata Kafka  │
                    └────────┬────────────┘
                             │
                             ▼
                    ┌─────────────────────┐
                    │   SILVER LAYER      │  Limpieza + DQ
                    │   Timestamps cast   │  Delta Lake
                    │   Columnas OTIF     │  (MERGE)
                    │   DQ flags          │
                    └────────┬────────────┘
                             │
                             ▼
                    ┌─────────────────────┐
                    │   GOLD LAYER        │  KPIs negocio
                    │   OTIF Rate         │  Delta Lake
                    │   Revenue analytics │
                    │   NPS proxy         │
                    └────────┬────────────┘
                             │
                             ▼
                    ┌─────────────────────┐
                    │   SERVING           │  Exportación
                    │   JSON estático     │  → GitHub
                    │   → dashboard/data/ │  → Netlify
                    └─────────────────────┘
                             │
                             ▼
                    [Dashboard HTML]
                    apsb-logilake.netlify.app
```

---

## Stack Técnico

| Componente | Tecnología |
|---|---|
| Mensajería | Apache Kafka 7.5 (Docker local) |
| Procesamiento | PySpark + Spark Structured Streaming |
| Storage | Delta Lake (Databricks Community Edition) |
| Notebooks | Databricks (Python 3.10) |
| Frontend | HTML + JS (sin framework) |
| Deploy | Netlify (CDN estático) |
| Versionamiento | GitHub |

---

## Dataset

**[Brazilian E-commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** — Kaggle

| Archivo | Descripción | Registros |
|---|---|---|
| olist_orders_dataset.csv | Pedidos principales | ~100k |
| olist_order_items_dataset.csv | Items por pedido | ~113k |
| olist_order_payments_dataset.csv | Pagos | ~104k |
| olist_order_reviews_dataset.csv | Reviews de clientes | ~100k |
| olist_products_dataset.csv | Catálogo de productos | ~33k |
| olist_sellers_dataset.csv | Vendedores | ~3k |
| product_category_name_translation.csv | Traducción categorías | 71 |

---

## Setup Rápido

### 1. Clonar repositorio

```bash
git clone https://github.com/Juancanchala/logilake.git
cd logilake
```

### 2. Descargar dataset Olist

```bash
# Instalar Kaggle CLI
pip install kaggle

# Configurar API key en ~/.kaggle/kaggle.json
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/
```

### 3. Levantar Kafka en Docker

```bash
cd kafka
docker-compose up -d

# Esperar ~30 segundos y crear el topic
bash topic_config.sh create
bash topic_config.sh describe

# Verificar en Kafka UI: http://localhost:8080
```

### 4. Ejecutar el Producer

```bash
pip install kafka-python pandas
python kafka/olist_producer.py \
  --data_path data/raw \
  --delay 0.02 \
  --batch 100
```

### 5. Configurar Databricks Community Edition

1. Crear cuenta en [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Crear cluster: **Databricks Runtime 13.3 LTS ML** (incluye Delta Lake + Kafka)
3. Subir los notebooks desde `notebooks/` via **Workspace > Import**
4. Subir los CSVs a DBFS: **Data > Add Data > DBFS > Upload File**
   - Ruta: `/FileStore/logilake/data/raw/`

### 6. Conectar Kafka ↔ Databricks (via ngrok)

```bash
# Instalar ngrok: https://ngrok.com/download
ngrok tcp 29092

# El tunnel genera una URL como: tcp://X.tcp.ngrok.io:XXXXX
# Usar esa URL en KAFKA_BOOTSTRAP de los notebooks
```

### 7. Ejecutar notebooks en orden

```
01_bronze_ingest.ipynb   → Ingesta Kafka → Delta Lake Bronze
02_silver_transform.ipynb → Transformación y DQ → Silver
03_gold_kpis.ipynb        → KPIs de negocio → Gold
04_export_serving.ipynb   → Export JSON → dashboard/data/
```

### 8. Desplegar dashboard

```bash
# Descargar JSONs desde DBFS y copiar a dashboard/data/
# Luego subir a GitHub y conectar Netlify
git add dashboard/
git commit -m "feat: actualizar data Gold"
git push
# Netlify deploy automático en cada push
```

---

## KPIs del Dashboard

| KPI | Descripción | Fuente |
|---|---|---|
| **OTIF Rate** | % de órdenes entregadas a tiempo y completas | kpi_global |
| **Avg Delivery Days** | Días promedio de entrega real | kpi_monthly |
| **Delivery Delay** | Retraso promedio vs estimado | kpi_monthly |
| **Cancellation Rate** | % de órdenes canceladas | kpi_monthly |
| **Total Revenue** | Revenue total en BRL | kpi_global |
| **Avg Order Value** | Ticket promedio | kpi_global |
| **Avg Review Score** | NPS proxy (1-5 estrellas) | kpi_global |
| **Revenue por Categoría** | Top categorías por revenue | kpi_category |
| **OTIF por Estado** | Performance de entrega por estado BR | kpi_seller_state |

---

## Estructura del Repositorio

```
logilake/
├── kafka/
│   ├── docker-compose.yml     # Kafka + Zookeeper + Kafka UI
│   ├── olist_producer.py      # Producer que lee CSVs → Kafka
│   └── topic_config.sh        # Gestión del topic
├── notebooks/
│   ├── bronze/01_bronze_ingest.ipynb
│   ├── silver/02_silver_transform.ipynb
│   ├── gold/03_gold_kpis.ipynb
│   └── serving/04_export_serving.ipynb
├── utils/
│   ├── schemas.py             # Schemas PySpark por capa
│   └── delta_helpers.py       # Helpers de Delta Lake
├── dashboard/
│   ├── index.html             # Dashboard HTML
│   └── data/                  # JSONs Gold (auto-generados)
│       ├── kpi_global.json
│       ├── kpi_monthly.json
│       ├── kpi_category.json
│       ├── kpi_nps.json
│       └── kpi_seller_state.json
├── data/
│   └── raw/                   # CSVs Olist (gitignored)
└── README.md
```

---

## Notas de Arquitectura

### Por qué Kafka si solo corre local

Kafka simula el escenario real de streaming de eventos de e-commerce.
En producción equivaldría a eventos de un sistema de pedidos (OMS) publicando
en tiempo real. El producer Python reemplaza la fuente real y demuestra
la integración completa Bronze ← Spark Structured Streaming ← Kafka.

### Alternativa batch

El notebook Bronze incluye un bloque de ingesta batch directa desde CSV,
útil cuando Kafka no está disponible (demos rápidas, CI/CD).

### Databricks Community Edition — Limitaciones

- No soporta clusters persistentes (timeout tras inactividad)
- Storage en DBFS (no S3/ADLS), suficiente para portafolio
- Sin Delta Live Tables (DLT) en Community — se usan notebooks manuales

---

## Autor

**Juan Camilo** — Data & AI Engineer  
**D'LOGIA** | [dlogia.tech](https://dlogia.tech) | [github.com/Juancanchala](https://github.com/Juancanchala)  
Medellín, Colombia
