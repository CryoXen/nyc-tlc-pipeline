"""
ETL HVFHV - Sin combinacion en memoria
Cada chunk se sube a S3 como Parquet separado.
Spark los lee todos juntos en la fase siguiente.
"""
import boto3, io, os, pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import gc
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

BUCKET       = os.getenv("BUCKET")
s3           = boto3.client("s3")
COLS         = ["hvfhs_license_num","pickup_datetime","PULocationID","DOLocationID",
                "trip_miles","base_passenger_fare","tips","shared_request_flag"]
PLATFORM_MAP = {"HV0003": "Uber", "HV0005": "Lyft"}
CHUNK_SIZE   = 1_000_000

def load_zones():
    obj = s3.get_object(Bucket=BUCKET, Key="reference/taxi_zone_lookup.csv")
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def process_month(year, month, zones_df):
    local_f = f"/tmp/fhvhv_{year}_{month:02d}.parquet"
    raw_key = f"raw/year={year}/month={month:02d}/fhvhv_tripdata_{year}-{month:02d}.parquet"

    # Verificar si ya esta procesado
    resp = s3.list_objects_v2(Bucket=BUCKET,
                               Prefix=f"processed/year={year}/month={month:02d}/")
    if resp.get("KeyCount", 0) > 0:
        print(f"  Ya procesado, saltando.")
        return

    if not os.path.exists(local_f):
        print(f"  Descargando desde S3 a /tmp...")
        s3.download_file(BUCKET, raw_key, local_f)
        print(f"  Descargado")

    z = zones_df[["LocationID","Zone","Borough"]].rename(
        columns={"LocationID":"PULocationID",
                 "Zone":"pickup_zone",
                 "Borough":"pickup_borough"}
    )

    pf         = pq.ParquetFile(local_f)
    total_rows = pf.metadata.num_rows
    print(f"  Total filas: {total_rows:,} | chunks de {CHUNK_SIZE:,}")

    for i, batch in enumerate(pf.iter_batches(batch_size=CHUNK_SIZE, columns=COLS)):
        df = batch.to_pandas()

        # Limpieza
        df = df.dropna(subset=["pickup_datetime","PULocationID","DOLocationID"])
        df = df[df["base_passenger_fare"] > 0]
        df = df[df["trip_miles"] > 0]
        df = df.drop_duplicates()

        # Transformaciones
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["platform"]        = df["hvfhs_license_num"].map(PLATFORM_MAP).fillna("Other")
        df["hour"]            = df["pickup_datetime"].dt.hour
        df["day_of_week"]     = df["pickup_datetime"].dt.dayofweek
        df["year"]            = year
        df["month"]           = month
        df["total_fare"]      = df["base_passenger_fare"] + df["tips"].fillna(0)
        df = df.merge(z, on="PULocationID", how="left")

        # Subir chunk directo a S3 y liberar memoria
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf, compression="snappy")
        buf.seek(0)
        out_key = f"processed/year={year}/month={month:02d}/chunk_{i:02d}.parquet"
        s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
        print(f"  chunk {i+1:02d} subido ({len(df):,} filas) -> {out_key}")

        del df, buf; gc.collect()

    print(f"  Mes {year}-{month:02d} completado")

if __name__ == "__main__":
    print(f"Bucket: {BUCKET}")
    zones = load_zones()
    for year, month in [(2023,1),(2023,2),(2023,3)]:
        try:
            print(f"\nProcesando {year}-{month:02d}...")
            process_month(year, month, zones)
        except Exception as e:
            print(f"  Error {year}-{month}: {e}")
    print("\nETL completado")
