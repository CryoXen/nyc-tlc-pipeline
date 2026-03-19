"""
Pipeline PySpark - Lee chunks procesados desde S3
Acepta --year y --month como argumentos opcionales
"""
import boto3, glob, os, io, sys, argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv(Path(__file__).parent.parent / ".env")

BUCKET = os.getenv("BUCKET")

# Argumentos opcionales --year y --month
parser = argparse.ArgumentParser()
parser.add_argument("--year",  type=int, default=None)
parser.add_argument("--month", type=int, default=None)
args = parser.parse_args()

# Spark Session
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

spark = SparkSession.builder \
    .appName("NYC-TLC-HVFHV-Pipeline") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("✓ SparkSession iniciada")

# Descargar chunks procesados de S3 a local
s3        = boto3.client("s3")
local_dir = "data/processed"
os.makedirs(local_dir, exist_ok=True)

# Decidir qué prefijo leer
if args.year and args.month:
    prefix = f"processed/year={args.year}/month={args.month:02d}/"
    print(f"→ Leyendo: {prefix}")
else:
    prefix = "processed/"
    print(f"→ Leyendo todos los meses disponibles")

# Listar y descargar chunks desde S3
resp   = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
chunks = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

if not chunks:
    print(f"✗ No hay chunks en s3://{BUCKET}/{prefix}")
    spark.stop()
    sys.exit(1)

print(f"→ Descargando {len(chunks)} chunks desde S3...")
for key in chunks:
    parts    = key.split("/")
    year_p   = parts[1]
    month_p  = parts[2]
    fname    = parts[-1]
    local_f  = f"{local_dir}/{year_p}_{month_p}_{fname}"
    if not os.path.exists(local_f):
        s3.download_file(BUCKET, key, local_f)
        print(f"  ✓ {fname}")
    else:
        print(f"  ✓ {fname} (ya existe)")

# Leer todos los parquets descargados
df = spark.read.option("mergeSchema", "true").option("int96RebaseModeInRead", "CORRECTED").parquet(f"{local_dir}/*.parquet")
print(f"✓ Total viajes cargados: {df.count():,}")
df.printSchema()

# Cargar taxi zones (broadcast)
obj      = s3.get_object(Bucket=BUCKET, Key="reference/taxi_zone_lookup.csv")
zones_pd = pd.read_csv(io.BytesIO(obj["Body"].read()))
zones_sp = spark.createDataFrame(zones_pd)

df_enriched = df.join(
    F.broadcast(zones_sp.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough")
    )),
    on="DOLocationID", how="left"
)

# Agregación 1: plataforma + año
agg_platform = df_enriched.groupBy("platform", "year").agg(
    F.count("*").alias("total_trips"),
    F.sum("total_fare").alias("total_revenue"),
    F.avg("total_fare").alias("avg_fare"),
    F.avg("trip_miles").alias("avg_miles"),
    F.sum(F.when(F.col("shared_request_flag") == "Y", 1).otherwise(0)).alias("shared_trips")
).orderBy("year", "platform")

print("\n=== Revenue por Plataforma y Año ===")
agg_platform.show()

# Agregación 2: demanda por hora
agg_hourly = df_enriched.groupBy("platform", "hour").agg(
    F.count("*").alias("trips"),
    F.avg("total_fare").alias("avg_fare")
).orderBy("platform", "hour")

# Agregación 3: boroughs
agg_borough = df_enriched.groupBy("pickup_borough", "year").agg(
    F.count("*").alias("trips"),
    F.sum("total_fare").alias("revenue")
).orderBy(F.desc("trips"))

# Window Function: top 10 zonas por mes
agg_zone_month = df_enriched.groupBy("year", "month", "pickup_zone").agg(
    F.count("*").alias("trips")
)
window_zone = Window.partitionBy("year", "month").orderBy(F.desc("trips"))
top_zones = agg_zone_month \
    .withColumn("rank", F.rank().over(window_zone)) \
    .filter(F.col("rank") <= 10)

print("\n=== Top 10 Zonas por Mes ===")
top_zones.show(20)

# Guardar a S3
def save_to_s3(df_spark, name):
    local_path = f"data/output/{name}"
    os.makedirs(local_path, exist_ok=True)
    df_spark.coalesce(1).write.mode("overwrite").parquet(local_path)
    for f in glob.glob(f"{local_path}/*.parquet"):
        s3.upload_file(f, BUCKET, f"output/{name}/{os.path.basename(f)}")
    print(f"✓ {name} → S3")

save_to_s3(agg_platform, "platform_by_year")
save_to_s3(agg_hourly,   "demand_by_hour")
save_to_s3(agg_borough,  "borough_stats")
save_to_s3(top_zones,    "top_zones_by_month")

spark.stop()
print("\n✓ Pipeline Spark completado")
