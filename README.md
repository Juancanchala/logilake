# LogiLake
**Data Engineering Portfolio Project — D'LOGIA**

Pipeline completo de ingeniería de datos sobre el dataset **Brazilian E-commerce Olist** (99,441 órdenes reales),
implementando una **arquitectura Medallion** con PySpark, Delta Lake y un dashboard interactivo con
**SQL Agent** (GPT-4o + DuckDB WASM).

**Live:** [logilake.netlify.app](https://logilake.netlify.app)

---

## Arquitectura

```
data/raw/  (9 CSVs Olist)
      |
      v
+─────────────────────+
│   BRONZE            │  Ingesta raw → Delta Lake
│   PySpark batch     │  9 tablas, 1.55M filas
│   data/bronze/      │
+─────────┬───────────+
          |
          v
+─────────────────────+
│   SILVER            │  Limpieza + Quality checks
│   JOIN 9 tablas     │  Timestamps, nulos, flags OTIF
│   data/silver/      │
+─────────┬───────────+
          |
          v
+─────────────────────+
│   GOLD              │  KPIs de negocio
│   OTIF, Revenue     │  Agregaciones por mes, categoría
│   NPS, Delivery     │  y estado
│   data/gold/        │
+─────────┬───────────+
          |
          v
+─────────────────────+
│   SERVING           │  Export CSV — contrato de datos
│   5 archivos CSV    │  listos para consumo BI
│   data/serving/     │
│   dashboard/data/   │
+─────────┬───────────+
          |
          v
+─────────────────────+
│   DASHBOARD         │  Netlify — logilake.netlify.app
│   Chart.js + D3     │  KPIs, gráficas, mapa Brasil
│   SQL Agent         │  GPT-4o genera SQL
│   DuckDB WASM       │  Ejecuta en el browser
+─────────────────────+
```

---

## Stack Técnico

| Capa | Tecnología |
|---|---|
| **Procesamiento** | PySpark 3.5.0 + Delta Lake 3.1.0 |
| **Ejecución local** | WSL2 Ubuntu + conda (Java 11) |
| **Automatización** | `make run` — pipeline completo en un comando |
| **Export** | pandas `to_csv()` → CSVs en `data/serving/` |
| **Dashboard** | HTML + Chart.js 4.4.1 + D3 7.8.5 |
| **SQL Agent** | GPT-4o (Netlify Function) + DuckDB WASM |
| **Deploy** | Netlify — CI/CD automático en cada push |
| **Streaming (opcional)** | Apache Kafka + Spark Structured Streaming |

---

## Dataset

**[Brazilian E-commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** — Kaggle

| Tabla Bronze | Descripción | Filas |
|---|---|---|
| orders | Pedidos principales | 99,441 |
| order_items | Items por pedido | 112,650 |
| order_payments | Pagos | 103,886 |
| order_reviews | Reviews de clientes | 99,224 |
| customers | Clientes | 99,441 |
| sellers | Vendedores | 3,095 |
| products | Catálogo de productos | 32,951 |
| geolocation | Coordenadas por CEP | 1,000,163 |
| category_translation | Traducción categorías | 71 |

---

## Setup Local (WSL2)

### Requisitos

- Windows 10/11 con WSL2 + Ubuntu
- conda en WSL (`/home/<user>/miniconda3/`)
- Java 11 en WSL (`apt install openjdk-11-jdk`)
- `make` en Windows (Git Bash lo incluye)

### 1. Clonar e instalar dependencias

```bash
git clone https://github.com/Juancanchala/logilake.git
cd logilake
pip install -r requirements.txt
```

### 2. Descargar dataset Olist

```bash
pip install kaggle
# Configurar API key en ~/.kaggle/kaggle.json
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/
```

### 3. Correr el pipeline completo

```bash
make run
```

Esto ejecuta en orden:

```
Bronze  → Silver  → Gold  → Serving  → sync dashboard/data/
```

Al terminar, abre `dashboard/index.html` en el browser — los datos están actualizados.

### Comandos disponibles

```bash
make run      # Pipeline completo (recomendado)
make bronze   # Solo ingesta Bronze
make silver   # Solo transformación Silver
make gold     # Solo KPIs Gold
make serving  # Solo export CSV + sync dashboard
make sync     # Solo copia CSVs a dashboard/data sin correr el pipeline
```

### Kafka (opcional)

Kafka simula el escenario real de streaming de eventos desde un OMS.

```bash
cd kafka
docker-compose up -d
bash topic_config.sh create
python olist_producer.py
```

> El pipeline batch (`make run`) no requiere Kafka. Es un modo alternativo
> para demostrar integración con Spark Structured Streaming.

---

## KPIs producidos

| KPI | Descripción | Tabla Serving |
|---|---|---|
| **OTIF Rate** | % órdenes entregadas a tiempo y completas | kpi_global |
| **Total Revenue** | Revenue total en BRL | kpi_global |
| **Avg Review Score** | NPS proxy (1–5 estrellas) | kpi_global, kpi_nps |
| **Avg Delivery Days** | Días promedio de entrega real | kpi_monthly |
| **Revenue por Categoría** | Top 10 categorías por revenue | kpi_category |
| **OTIF por Estado** | Performance de entrega por estado BR | kpi_seller_state |
| **Cancellation Rate** | % órdenes canceladas por mes | kpi_monthly |

---

## Contrato de Datos (Serving)

Las capas Bronze → Silver → Gold pueden cambiar internamente. Lo que no cambia
es el schema de los 5 CSVs en `serving/` — ese es el contrato con el BI layer.

| Archivo | Filas | Descripción |
|---|---|---|
| `kpi_global.csv` | 1 | Totales globales del dataset |
| `kpi_monthly.csv` | 24 | KPIs agregados por mes |
| `kpi_nps.csv` | 5 | Distribución de ratings (1–5★) |
| `kpi_category.csv` | 44 | KPIs por categoría de producto |
| `kpi_seller_state.csv` | 22 | KPIs por estado de Brasil |

---

## Estructura del Repositorio

```
logilake/
├── kafka/
│   ├── docker-compose.yml        # Kafka + Zookeeper + Kafka UI
│   ├── olist_producer.py         # Producer CSV → Kafka (streaming demo)
│   └── topic_config.sh           # Gestión del topic
├── notebooks/
│   ├── bronze/01_bronze_ingest.ipynb
│   ├── silver/02_silver_transform.ipynb
│   ├── gold/03_gold_kpis.ipynb
│   └── serving/04_export_serving.ipynb
├── dashboard/
│   ├── index.html                # Dashboard principal (responsive)
│   ├── _redirects                # Netlify SPA routing
│   ├── js/
│   │   └── agent.js              # SQL Agent — DuckDB WASM + GPT-4o
│   └── data/                     # CSVs Serving (sincronizados por el pipeline)
├── netlify/
│   └── functions/
│       └── ask.js                # Proxy GPT-4o — protege la API key
├── scripts/
│   └── run_pipeline.sh           # Script maestro WSL — ejecuta los 4 notebooks
├── utils/
│   ├── schemas.py                # Schemas PySpark por capa
│   └── delta_helpers.py          # Helpers Delta Lake
├── data/
│   ├── raw/                      # CSVs Olist originales (gitignored)
│   ├── bronze/                   # Delta Lake Bronze (gitignored)
│   ├── silver/                   # Delta Lake Silver (gitignored)
│   ├── gold/                     # Delta Lake Gold (gitignored)
│   └── serving/                  # CSVs exportados (gitignored)
├── Makefile                      # make run / make bronze / etc.
├── netlify.toml                  # Config deploy — publish = dashboard/
├── requirements.txt
└── .gitignore
```

---

## Autor

**Juan Camilo Canchala** — Data & AI Engineer
**D'LOGIA** | [dlogia.tech](https://dlogia.tech) | [github.com/Juancanchala](https://github.com/Juancanchala)
Medellín, Colombia
