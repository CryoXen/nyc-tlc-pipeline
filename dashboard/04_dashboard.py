"""
04_dashboard.py — NYC TLC Analytics Dashboard
Lee resultados desde data/output/ (montado desde host) o S3 como fallback
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import glob, boto3, io, os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

st.set_page_config(
    page_title="NYC TLC Analytics",
    page_icon="🚕",
    layout="wide"
)

BUCKET = os.getenv("BUCKET")
s3     = boto3.client("s3")

@st.cache_data
def load_result(name):
    local_path = f"data/output/{name}"
    files = glob.glob(f"{local_path}/*.parquet")
    if files:
        return pd.concat([pd.read_parquet(f) for f in files])
    st.info(f"Leyendo {name} desde S3...")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"output/{name}/")
    dfs  = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            dfs.append(pd.read_parquet(io.BytesIO(data["Body"].read())))
    if dfs:
        os.makedirs(local_path, exist_ok=True)
        df = pd.concat(dfs)
        df.to_parquet(f"{local_path}/data.parquet", index=False)
        return df
    return pd.DataFrame()

@st.cache_data
def load_ingesta_log():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="metadata/ingesta_log.json")
        import json
        return json.loads(obj["Body"].read())
    except:
        return []

with st.spinner("Cargando datos..."):
    df_platform = load_result("platform_by_year")
    df_hourly   = load_result("demand_by_hour")
    df_borough  = load_result("borough_stats")
    df_zones    = load_result("top_zones_by_month")

if df_platform.empty:
    st.error("No hay datos. Corre primero: python3 src/run_pipeline.py")
    st.stop()

st.title("🚕 NYC TLC · High Volume FHV Analytics")
st.caption(f"Uber & Lyft · AWS Lambda + Pandas ETL + PySpark + Streamlit · Bucket: {BUCKET}")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Viajes",    f"{df_platform['total_trips'].sum():,.0f}")
k2.metric("Revenue Total",   f"${df_platform['total_revenue'].sum():,.0f}")
k3.metric("Tarifa Promedio", f"${df_platform['avg_fare'].mean():.2f}")
k4.metric("Períodos",        f"{df_platform['year'].nunique()} año(s)")

st.divider()

c1, c2 = st.columns(2)
with c1:
    st.subheader("📈 Viajes por plataforma")
    fig = px.bar(df_platform, x="year", y="total_trips",
                 color="platform", barmode="group",
                 color_discrete_map={"Uber": "#1a1a1a", "Lyft": "#FF00BF"})
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("💰 Tarifa promedio por año")
    fig2 = px.line(df_platform, x="year", y="avg_fare",
                   color="platform", markers=True,
                   color_discrete_map={"Uber": "#1a1a1a", "Lyft": "#FF00BF"})
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("🕐 Demanda por hora del día")
fig3 = px.area(df_hourly, x="hour", y="trips", color="platform",
               color_discrete_map={"Uber": "#1a1a1a", "Lyft": "#FF00BF"})
st.plotly_chart(fig3, use_container_width=True)

c3, c4 = st.columns([1, 2])
with c3:
    st.subheader("🗽 Viajes por Borough")
    if not df_borough.empty:
        borough_agg = df_borough.groupby("pickup_borough")["trips"].sum().reset_index()
        fig4 = px.bar(borough_agg.sort_values("trips"),
                      x="trips", y="pickup_borough", orientation="h",
                      color="trips", color_continuous_scale="Oranges")
        st.plotly_chart(fig4, use_container_width=True)

with c4:
    st.subheader("📍 Top 10 Zonas de Pickup")
    if not df_zones.empty:
        years_available = sorted(df_zones["year"].unique())
        year_sel = st.selectbox("Año:", years_available,
                                index=len(years_available)-1)
        top = (df_zones[df_zones["year"] == year_sel]
               .groupby("pickup_zone")["trips"].sum()
               .nlargest(10).reset_index())
        fig5 = px.bar(top, x="trips", y="pickup_zone", orientation="h",
                      color="trips", color_continuous_scale="Viridis")
        st.plotly_chart(fig5, use_container_width=True)

st.divider()
with st.expander("📋 Log de ingesta"):
    log = load_ingesta_log()
    if log:
        st.dataframe(pd.DataFrame(log), use_container_width=True)
    else:
        st.info("No hay log disponible.")

with st.expander("🏗 Arquitectura del Pipeline"):
    st.markdown("""
    ```
    NYC TLC CDN → Lambda chris-nyc-tlc-ingest → S3 raw/ + metadata/ingesta_log.json
    S3 raw/     → Pandas ETL (chunks 1M filas) → S3 processed/
    S3 processed/ → PySpark pipeline           → S3 output/
    S3 output/  → Streamlit Dashboard (esta app)
    IAM: LabRole (least privilege)
    ```
    """)
