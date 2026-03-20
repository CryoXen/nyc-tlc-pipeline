## Dataset

Fuente: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Patron de archivo: `fhvhv_tripdata_YYYY-MM.parquet`  
CDN: `https://d37ci6vzurychx.cloudfront.net/trip-data/`  
Cobertura: enero 2021 a marzo 2026 (solo High Volume FHV)

Columnas principales: `hvfhs_license_num` (HV0003=Uber, HV0005=Lyft), `pickup_datetime`, `PULocationID`, `DOLocationID`, `base_passenger_fare`, `tips`, `trip_miles`

## Configuracion

### Requisitos

- Python 3.12
- Java 11 (requerido por Spark)
- AWS CLI configurado
- Docker (para el dashboard)

### Instalar dependencias
```bash
