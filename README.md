# LogiLake
**Data Engineering Portfolio Project — D'LOGIA**

Pipeline completo de ingenieria de datos sobre el dataset **Brazilian E-commerce Olist** (100k ordenes reales), implementando una **arquitectura Medallion** con Kafka, PySpark, Delta Lake y Power BI.
Corre completamente en local con **PySpark 3.5.0** y **Delta Spark 3.1.0** — sin Databricks ni servicios cloud.

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
                   |   CSV EXPORT        |  Serving
                   |   pandas to_csv()   |  powerbi/data/
                   |   UTF-8, header     |  single-file
                   +--------+------------+
                            |
                            v
                   [Power BI Desktop]
                   Obtener datos -> Carpeta
                            |
                            v
                   [Power BI Service]
                   Publicar y compartir
```

**Flujo: Kafka -> Bronze -> Silver -> Gold -> CSV Export -> Power BI Desktop -> Power BI Service**

---

## Stack Tecnico

| Componente | Tecnologia | Version |
|---|---|---|
| Mensajeria | Apache Kafka (Docker local) | 7.5 (Confluent) |
| Procesamiento | PySpark + Structured Streaming | 3.5.0 |
| Storage | Delta Lake | 3.1.0 |
| Notebooks | Jupyter Notebook | local |
| Visualizacion | Power BI Desktop / Service | - |
| Versionamiento | GitHub | - |

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

### 1. Clonar repositorio e instalar dependencias

```bash
git clone https://github.com/Juancanchala/logilake.git
cd logilake
pip install -r requirements.txt
```

`requirements.txt` incluye: `pyspark==3.5.0`, `delta-spark==3.1.0`, `kafka-python==2.0.2`, `pandas>=2.0.0`, `jupyter`.

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

# Esperar ~30 segundos y crear el topic
bash topic_config.sh create
bash topic_config.sh describe

# Verificar en Kafka UI: http://localhost:8080
cd ..
```

> Si no tienes Docker, puedes saltarte Kafka. El notebook Bronze incluye
> modo de ingesta batch directa desde CSV.

### 4. Lanzar Jupyter y ejecutar notebooks

```bash
jupyter notebook
```

Ejecutar en orden desde `notebooks/`:

```
01_bronze_ingest.ipynb    -> Ingesta CSV/Kafka  -> Delta Lake data/bronze/
02_silver_transform.ipynb -> Transformacion DQ  -> Delta Lake data/silver/
03_gold_kpis.ipynb        -> KPIs de negocio    -> Delta Lake data/gold/
04_export_serving.ipynb   -> Export CSV limpio  -> powerbi/data/
```

### 5. Conectar Power BI

```
1. Ejecutar 04_export_serving.ipynb
2. Abrir Power BI Desktop
3. Inicio -> Obtener datos -> Carpeta -> seleccionar powerbi/data/
4. Publicar en Power BI Service
```

Ver instrucciones detalladas en [powerbi/README.md](powerbi/README.md).

---

## KPIs

| KPI | Descripcion | Tabla CSV |
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
|   |-- docker-compose.yml     # Kafka + Zookeeper + Kafka UI
|   |-- olist_producer.py      # Producer que lee CSVs -> Kafka
|   +-- topic_config.sh        # Gestion del topic
|-- notebooks/
|   |-- bronze/01_bronze_ingest.ipynb
|   |-- silver/02_silver_transform.ipynb
|   |-- gold/03_gold_kpis.ipynb
|   +-- serving/04_export_serving.ipynb
|-- utils/
|   |-- schemas.py             # Schemas PySpark por capa
|   +-- delta_helpers.py       # Helpers de Delta Lake
|-- powerbi/
|   |-- README.md              # Instrucciones de conexion Power BI
|   +-- data/                  # CSVs Gold exportados (gitignored)
|-- data/
|   +-- raw/                   # CSVs Olist (gitignored)
|-- requirements.txt
+-- README.md
```

Datos generados en ejecucion (todos gitignored):
- `data/bronze/`, `data/silver/`, `data/gold/` — tablas Delta Lake
- `data/checkpoints/` — checkpoints de Spark Streaming
- `powerbi/data/` — CSVs exportados para Power BI

---

## Notas de Arquitectura

### SparkSession local con Delta Lake

Cada notebook configura su propia `SparkSession` usando `configure_spark_with_delta_pip`
de la libreria `delta-spark`. Esto descarga automaticamente los JARs de Delta al
iniciar la sesion (solo la primera vez, luego se cachean en `~/.ivy2`).

### Por que Kafka si solo corre local

Kafka simula el escenario real de streaming de eventos de e-commerce.
En produccion equivaldria a eventos de un OMS publicando en tiempo real.
El producer Python reemplaza la fuente real y demuestra la integracion
completa Bronze <- Spark Structured Streaming <- Kafka.

El notebook Bronze incluye **modo batch** como alternativa cuando Kafka
no esta disponible, cargando los CSVs directamente.

### Exportacion CSV con pandas

El notebook Serving usa `toPandas().to_csv()` en lugar de
`coalesce(1).write.csv()` para producir un unico archivo por tabla
sin subdirectorios, listo para importar directamente en Power BI.

---

## Autor

**Juan Camilo** — Data & AI Engineer
**D'LOGIA** | [dlogia.tech](https://dlogia.tech) | [github.com/Juancanchala](https://github.com/Juancanchala)
Medellin, Colombia
