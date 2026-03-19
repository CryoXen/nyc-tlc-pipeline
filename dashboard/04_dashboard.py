import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob, boto3, io, os
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="NYC TLC Analytics", page_icon="🚕", layout="wide")
BUCKET = os.getenv("S3_BUCKET")
s3     = boto3.client("s3")

@st.cache_data
def load_result(name):
    files = glob.glob(f"data/output/{name}/*.parquet")
    if files:
        return pd.concat([pd.read_parquet(f) for f in files])
    # fallback: leer desde S3
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"output/{name}/")
    dfs  = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            dfs.append(pd.read_parquet(io.BytesIO(data["Body"].read())))
    return pd.concat(dfs) if dfs else pd.DataFrame()

df_platform = load_result("platform_by_year")
df_hourly   = load_result("demand_by_hour")
df_borough  = load_result("borough_stats")
df_zones    = load_result("top_zones_by_month")

# ── Header ────────────────────────────────────────
st.title("🚕 NYC TLC · High Volume FHV Analytics")
st.caption("Uber & Lyft trips 2022–2024 · AWS Lambda + Pandas ETL + PySpark + Streamlit")

# ── KPIs ──────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Viajes",    f"{df_platform['total_trips'].sum():,.0f}")
k2.metric("Revenue Total",   f"${df_platform['total_revenue'].sum():,.0f}")
k3.metric("Tarifa Promedio", f"${df_platform['avg_fare'].mean():.2f}")
k4.metric("Años analizados", f"{df_platform['year'].nunique()}")

st.divider()

# ── Fila 1: Tendencia anual + Uber vs Lyft ────────
c1, c2 = st.columns(2)
with c1:
    st.subheader("📈 Viajes por año y plataforma")
    fig = px.bar(df_platform, x="year", y="total_trips",
                 color="platform", barmode="group",
                 color_discrete_map={"Uber":"#000000","Lyft":"#FF00BF"})
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("💰 Revenue promedio por año")
    fig2 = px.line(df_platform, x="year", y="avg_fare",
                   color="platform", markers=True,
                   color_discrete_map={"Uber":"#000000","Lyft":"#FF00BF"})
    st.plotly_chart(fig2, use_container_width=True)

# ── Fila 2: Demanda por hora ──────────────────────
st.subheader("🕐 Demanda por hora del día")
fig3 = px.area(df_hourly, x="hour", y="trips", color="platform",
               color_discrete_map={"Uber":"#000000","Lyft":"#FF00BF"})
st.plotly_chart(fig3, use_container_width=True)

# ── Fila 3: Top boroughs + Top zonas ─────────────
c3, c4 = st.columns([1,2])
with c3:
    st.subheader("🗽 Viajes por Borough")
    borough_agg = df_borough.groupby("pickup_borough")["trips"].sum().reset_index()
    fig4 = px.bar(borough_agg.sort_values("trips"),
                  x="trips", y="pickup_borough", orientation="h",
                  color="trips", color_continuous_scale="Oranges")
    st.plotly_chart(fig4, use_container_width=True)

with c4:
    st.subheader("📍 Top 10 Zonas de Pickup")
    year_sel = st.selectbox("Año:", sorted(df_zones["year"].unique()))
    top = df_zones[df_zones["year"]==year_sel] \
          .groupby("pickup_zone")["trips"].sum() \
          .nlargest(10).reset_index()
    fig5 = px.bar(top, x="trips", y="pickup_zone", orientation="h",
                  color="trips", color_continuous_scale="Viridis")
    st.plotly_chart(fig5, use_container_width=True)