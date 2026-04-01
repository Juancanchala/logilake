# Power BI — Conexión a LogiLake CSVs

Este directorio contiene los archivos CSV exportados desde la capa Gold de LogiLake,
listos para ser consumidos por Power BI Desktop o publicados en Power BI Service.

---

## Estructura esperada

```
powerbi/
├── README.md                  # Este archivo
└── data/                      # CSVs exportados (gitignored)
    ├── kpi_global.csv
    ├── kpi_monthly.csv
    ├── kpi_category.csv
    ├── kpi_nps.csv
    └── kpi_seller_state.csv
```

> `powerbi/data/` está en `.gitignore`. Los CSVs se generan ejecutando
> `04_export_serving.ipynb` en Databricks y descargando los archivos desde DBFS.

---

## Paso 1 — Generar los CSVs

1. En Databricks, ejecutar `notebooks/serving/04_export_serving.ipynb`
2. Los CSVs se escriben en `/FileStore/logilake/serving/csv/`
3. Descargar cada archivo:
   - **Databricks UI** → Data → DBFS → `FileStore/logilake/serving/csv/`
   - Seleccionar el archivo `part-00000-*.csv` de cada subcarpeta
   - Renombrarlo a `kpi_<nombre>.csv` y copiarlo a `powerbi/data/`

---

## Paso 2 — Conectar Power BI Desktop

### Opción A: Importar CSV directamente

1. Abrir **Power BI Desktop**
2. Ir a **Inicio → Obtener datos → Texto/CSV**
3. Seleccionar cada archivo en `powerbi/data/`
4. Repetir para las 5 tablas Gold
5. En el Editor de Power Query verificar tipos de datos:
   - Columnas de fecha: `order_month` → Tipo **Fecha**
   - Métricas numéricas: `otif_rate`, `avg_delivery_days`, etc. → Tipo **Número decimal**
   - Textos: `category`, `seller_state` → Tipo **Texto**

### Opción B: Carpeta completa (recomendado)

1. Ir a **Inicio → Obtener datos → Carpeta**
2. Seleccionar la carpeta `powerbi/data/`
3. Power BI carga todos los CSVs y permite combinarlos o trabajarlos por separado

---

## Paso 3 — Modelo de datos sugerido

Relacionar las tablas usando `order_month` como campo de tiempo compartido:

```
kpi_global       (1 registro — KPIs globales)
kpi_monthly      (1 fila por mes — serie temporal)
kpi_category     (1 fila por categoría de producto)
kpi_nps          (1 fila por puntuación 1-5)
kpi_seller_state (1 fila por estado de Brasil)
```

No se requieren relaciones formales entre tablas; cada una alimenta
visualizaciones independientes.

---

## Paso 4 — Visualizaciones recomendadas

| Tabla | Visualización | Campos clave |
|---|---|---|
| `kpi_global` | Tarjetas KPI | `otif_rate`, `total_revenue`, `avg_review_score` |
| `kpi_monthly` | Gráfico de líneas | `order_month` (eje X), `otif_rate`, `avg_delivery_days` |
| `kpi_category` | Gráfico de barras | `category` (eje Y), `total_revenue` (eje X) |
| `kpi_nps` | Gráfico de barras apiladas | `review_score`, `order_count` |
| `kpi_seller_state` | Mapa de coropletas | `seller_state`, `otif_rate` |

---

## Paso 5 — Publicar en Power BI Service

1. En Power BI Desktop: **Inicio → Publicar → Mi área de trabajo**
2. En Power BI Service, ir al dataset publicado
3. Configurar **actualización programada** si los CSVs se actualizan periódicamente
   - O usar **OneDrive / SharePoint** como fuente para actualización automática:
     - Subir los CSVs a una carpeta de OneDrive
     - Conectar Power BI al archivo de OneDrive en lugar de la ruta local

---

## Notas

- Los CSVs exportados usan codificación **UTF-8** y separador **coma (,)**
- Las columnas de timestamp están en formato **ISO 8601** (`yyyy-MM-dd HH:mm:ss`)
- Valores monetarios en **BRL (Reales brasileños)**
- El campo `order_month` tiene formato `yyyy-MM` (ej. `2018-01`)
