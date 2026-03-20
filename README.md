# NYC TLC High Volume FHV — Pipeline de Datos

Pipeline end-to-end para procesar registros de viajes de Uber y Lyft del dataset de la Comision de Taxis y Limusinas de Nueva York (2021–2026), construido sobre AWS.

## Stack

- **Ingesta**: AWS Lambda (serverless, multipart upload en streaming)
- **Almacenamiento**: AWS S3 (particionado por año/mes)
- **ETL**: Python / Pandas
- **Procesamiento**: Apache Spark 3.5 (PySpark)
- **Visualizacion**: Streamlit + Plotly
- **CI/CD**: GitHub Actions (runner self-hosted)

## Arquitectura
```
NYC TLC CDN -> Lambda -> S3 raw/
                              |
                         Pandas ETL -> S3 processed/
                                              |
                                       PySpark -> S3 output/
                                                       |
                                               Streamlit Dashboard
```

## Dataset

Fuente: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Patron de archivo: `fhvhv_tripdata_YYYY-MM.parquet`  
CDN: `https://d37ci6vzurychx.cloudfront.net/trip-data/`  
Cobertura: enero 2021 a marzo 2026 (solo High Volume FHV)

Columnas principales: `hvfhs_license_num` (HV0003=Uber, HV0005=Lyft), `pickup_datetime`, `PULocationID`, `DOLocationID`, `base_passenger_fare`, `tips`, `trip_miles`


## Etapas del Pipeline

| Etapa | Script | Descripcion |
|-------|--------|-------------|
| Ingesta | `lambda_ingesta.py` | Descarga el Parquet desde el CDN de TLC via streaming multipart upload a S3. Registra metadata en `metadata/ingesta_log.json`. Si Lambda falla, descarga directamente desde el CDN. |
| ETL | `02_etl.py` | Limpia nulos y duplicados, mapea numeros de licencia a nombre de plataforma, calcula columnas derivadas y hace JOIN con el catalogo de zonas. Procesa en bloques de 1M filas para no superar 400MB de RAM. |
| Spark | `03_spark_pipeline.py` | Broadcast join con el catalogo de zonas, calcula cuatro tablas de agregacion: plataforma por año, demanda por hora, estadisticas por borough y top zonas por mes usando la funcion de ventana RANK(). |
| Dashboard | `04_dashboard.py` | Lee los resultados de Spark desde S3 y muestra KPIs y cinco graficas Plotly distribuidas en tres pestañas. |

## Resultados (enero 2024)

- Uber: 14.4M viajes, $366M en ingresos, tarifa promedio $25.36
- Lyft: 5.2M viajes, $125M en ingresos, tarifa promedio $24.10
- Zona con mas viajes: JFK Airport (374K viajes)

## Infraestructura

- EC2: t3.medium, Ubuntu 24, us-west-1
- Bucket S3: particionado como `raw/year=YYYY/month=MM/`
- IAM: rol de minimo privilegio con acceso limitado al bucket
- Dashboard: contenedor Docker, puerto 8501, desplegado automaticamente via GitHub Actions al hacer push a `dashboard/`


Christian Rubi — Data Academy Xideral — 2025
