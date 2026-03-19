"""
run_pipeline.py — Orquestador interactivo del pipeline NYC TLC
"""
import boto3, os, io, sys, json, subprocess, requests
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import gc
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BUCKET       = os.getenv("BUCKET")
CDN_BASE     = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PLATFORM_MAP = {"HV0003": "Uber", "HV0005": "Lyft"}
COLS         = ["hvfhs_license_num", "pickup_datetime", "PULocationID", "DOLocationID",
                "trip_miles", "base_passenger_fare", "tips", "shared_request_flag"]
CHUNK_SIZE   = 1_000_000

s3 = boto3.client("s3")

GREEN  = "\033[92m"; YELLOW = "\033[93m"; RED    = "\033[91m"
BLUE   = "\033[94m"; BOLD   = "\033[1m";  RESET  = "\033[0m"

def ok(msg):    print(f"{GREEN}✓{RESET} {msg}")
def warn(msg):  print(f"{YELLOW}⚠{RESET}  {msg}")
def err(msg):   print(f"{RED}✗{RESET}  {msg}")
def info(msg):  print(f"{BLUE}→{RESET} {msg}")
def title(msg): print(f"\n{BOLD}{msg}{RESET}")

def ask_year_month():
    title("NYC TLC Pipeline — Selección de período")
    print("Años disponibles: 2021–2026  |  Meses: 1–12\n")
    while True:
        try:
            year  = int(input("  Ingresa el año  (ej. 2023): ").strip())
            month = int(input("  Ingresa el mes  (ej. 6):    ").strip())
            if not (2021 <= year <= 2026):
                warn("Año fuera de rango (2021-2026)."); continue
            if not (1 <= month <= 12):
                warn("Mes fuera de rango (1-12)."); continue
            now = datetime.utcnow()
            if year > now.year or (year == now.year and month > now.month):
                warn(f"El período {year}-{month:02d} aún no existe (futuro)."); continue
            return year, month
        except ValueError:
            warn("Ingresa números válidos.")

def check_cdn(year, month):
    url = f"{CDN_BASE}/fhvhv_tripdata_{year}-{month:02d}.parquet"
    info(f"Verificando en CDN: {url}")
    try:
        resp = requests.head(url, timeout=10)
        if resp.status_code == 200:
            size_mb = int(resp.headers.get("Content-Length", 0)) / 1024**2
            ok(f"Archivo encontrado en CDN ({size_mb:.0f} MB)")
            return True
        err(f"No encontrado en CDN (HTTP {resp.status_code})")
        return False
    except Exception as e:
        err(f"No se pudo verificar el CDN: {e}")
        return False

def check_s3_raw(year, month):
    key = f"raw/year={year}/month={month:02d}/fhvhv_tripdata_{year}-{month:02d}.parquet"
    try:
        s3.head_object(Bucket=BUCKET, Key=key); return True
    except:
        return False

def run_ingesta(year, month):
    title(f"FASE 1 — Ingesta: {year}-{month:02d}")
    if check_s3_raw(year, month):
        ok("Ya existe en S3 raw/, saltando ingesta."); return
    info("Invocando Lambda chris-nyc-tlc-ingest...")
    try:
        lam  = boto3.client("lambda")
        resp = lam.invoke(
            FunctionName="chris-nyc-tlc-ingest",
            InvocationType="RequestResponse",
            Payload=json.dumps({"year": year, "month": month}).encode()
        )
        result = json.loads(resp["Payload"].read())
        if result.get("statusCode") == 200:
            ok(f"Lambda completada: {json.loads(result['body'])}")
        else:
            raise Exception(f"Lambda respondió: {result}")
    except Exception as e:
        warn(f"Lambda falló ({e}). Descarga directa como fallback...")
        _download_direct(year, month)

def _download_direct(year, month):
    url     = f"{CDN_BASE}/fhvhv_tripdata_{year}-{month:02d}.parquet"
    local_f = f"/tmp/fhvhv_{year}_{month:02d}.parquet"
    s3_key  = f"raw/year={year}/month={month:02d}/fhvhv_tripdata_{year}-{month:02d}.parquet"
    if not os.path.exists(local_f):
        info("Descargando desde CDN...")
        import urllib.request
        urllib.request.urlretrieve(url, local_f)
    info("Subiendo a S3...")
    s3.upload_file(local_f, BUCKET, s3_key)
    ok(f"Subido: {s3_key}")

def run_etl(year, month):
    title(f"FASE 2 — ETL Pandas: {year}-{month:02d}")

    # Verificar si ya hay chunks procesados
    resp = s3.list_objects_v2(Bucket=BUCKET,
                               Prefix=f"processed/year={year}/month={month:02d}/")
    if resp.get("KeyCount", 0) > 0:
        ok(f"Ya procesado ({resp['KeyCount']} chunks en S3), saltando ETL.")
        return

    # Cargar taxi zones
    info("Cargando taxi zones...")
    obj   = s3.get_object(Bucket=BUCKET, Key="reference/taxi_zone_lookup.csv")
    zones = pd.read_csv(io.BytesIO(obj["Body"].read()))
    z     = zones[["LocationID","Zone","Borough"]].rename(
        columns={"LocationID":"PULocationID",
                 "Zone":"pickup_zone",
                 "Borough":"pickup_borough"}
    )

    # Descargar raw a /tmp
    local_f = f"/tmp/fhvhv_{year}_{month:02d}.parquet"
    raw_key = f"raw/year={year}/month={month:02d}/fhvhv_tripdata_{year}-{month:02d}.parquet"
    if not os.path.exists(local_f):
        info("Descargando raw desde S3 a /tmp...")
        s3.download_file(BUCKET, raw_key, local_f)
        ok("Descargado")

    pf         = pq.ParquetFile(local_f)
    total_rows = pf.metadata.num_rows
    info(f"Procesando {total_rows:,} filas en chunks de {CHUNK_SIZE:,}...")

    # ── CLAVE: subir cada chunk directo a S3, sin acumular en RAM ──
    for i, batch in enumerate(pf.iter_batches(batch_size=CHUNK_SIZE, columns=COLS)):
        df = batch.to_pandas()
        df = df.dropna(subset=["pickup_datetime","PULocationID","DOLocationID"])
        df = df[df["base_passenger_fare"] > 0]
        df = df[df["trip_miles"] > 0]
        df = df.drop_duplicates()
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["platform"]        = df["hvfhs_license_num"].map(PLATFORM_MAP).fillna("Other")
        df["hour"]            = df["pickup_datetime"].dt.hour
        df["day_of_week"]     = df["pickup_datetime"].dt.dayofweek
        df["year"]            = year
        df["month"]           = month
        df["total_fare"]      = df["base_passenger_fare"] + df["tips"].fillna(0)
        df = df.merge(z, on="PULocationID", how="left")

        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf, compression="snappy")
        buf.seek(0)
        out_key = f"processed/year={year}/month={month:02d}/chunk_{i:02d}.parquet"
        s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
        ok(f"Chunk {i+1:02d} subido ({len(df):,} filas)")

        del df, buf; gc.collect()

    ok(f"ETL {year}-{month:02d} completado")

def run_spark(year, month):
    title(f"FASE 3 — Spark Pipeline: {year}-{month:02d}")
    spark_script = Path(__file__).parent / "03_spark_pipeline.py"
    if not spark_script.exists():
        warn("03_spark_pipeline.py no encontrado. Saltando fase Spark.")
        return
    info("Ejecutando 03_spark_pipeline.py...")
    result = subprocess.run(
        [sys.executable, str(spark_script), "--year", str(year), "--month", str(month)],
        capture_output=False
    )
    if result.returncode == 0:
        ok("Spark completado")
    else:
        err("Spark falló. Revisa el output arriba.")

def print_summary(year, month):
    title("RESUMEN DEL PIPELINE")
    print(f"  Período : {year}-{month:02d}  |  Bucket: {BUCKET}\n")

    # Contar chunks procesados
    resp = s3.list_objects_v2(Bucket=BUCKET,
                               Prefix=f"processed/year={year}/month={month:02d}/")
    chunks = resp.get("KeyCount", 0)

    keys = [
        f"raw/year={year}/month={month:02d}/fhvhv_tripdata_{year}-{month:02d}.parquet",
        f"metadata/ingesta_log.json",
    ]
    for key in keys:
        try:
            obj  = s3.head_object(Bucket=BUCKET, Key=key)
            size = obj["ContentLength"] / 1024**2
            ok(f"{key}  ({size:.1f} MB)")
        except:
            warn(f"{key}  — no encontrado")

    if chunks > 0:
        ok(f"processed/year={year}/month={month:02d}/  ({chunks} chunks)")
    else:
        warn(f"processed/year={year}/month={month:02d}/  — vacío")

if __name__ == "__main__":
    print(f"\n{'='*55}")
    print(f"  NYC TLC Pipeline  |  Bucket: {BUCKET}")
    print(f"{'='*55}")

    year, month = ask_year_month()

    print()
    if not check_cdn(year, month):
        err(f"fhvhv_tripdata_{year}-{month:02d}.parquet no existe en NYC TLC.")
        err("Proceso abortado.")
        sys.exit(1)

    print(f"\n{BOLD}  Período seleccionado: {year}-{month:02d}{RESET}")
    confirm = input("  ¿Continuar con el pipeline completo? (s/n): ").strip().lower()
    if confirm != "s":
        warn("Proceso cancelado por el usuario.")
        sys.exit(0)

    try:
        run_ingesta(year, month)
        run_etl(year, month)
        run_spark(year, month)
        print_summary(year, month)
        print(f"\n{GREEN}{BOLD}Pipeline completado ✓{RESET}\n")
    except KeyboardInterrupt:
        err("\nInterrumpido por el usuario.")
        sys.exit(1)
    except Exception as e:
        err(f"Error inesperado: {e}")
        raise
