"""
04_dashboard.py — NYC TLC Analytics Dashboard
Lee resultados desde data/output/ (montado) o S3 como fallback
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob, boto3, io, os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

st.set_page_config(
    page_title="NYC TLC Analytics",
    page_icon=None,
    layout="wide"
)

BUCKET = os.getenv("BUCKET")
s3     = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-west-1"))

# Paleta de colores accesible — sin negro
COLORS = {
    "Uber": "#2563EB",   # azul
    "Lyft": "#DC2626",   # rojo
    "Other": "#6B7280"   # gris
}
COLOR_SEQ = ["#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED"]

@st.cache_data(show_spinner=False)
def load_result(name):
    local_path = f"data/output/{name}"
    files = glob.glob(f"{local_path}/*.parquet")
    if files:
        return pd.concat([pd.read_parquet(f) for f in files])
    try:
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
    except Exception as e:
        st.error(f"Error cargando {name}: {e}")
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def load_ingesta_log():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="metadata/ingesta_log.json")
        import json
        return pd.DataFrame(json.loads(obj["Body"].read()))
    except:
        return pd.DataFrame()

# ── Cargar datos ─────────────────────────────────────────────────
with st.spinner("Cargando datos desde S3..."):
    df_platform = load_result("platform_by_year")
    df_hourly   = load_result("demand_by_hour")
    df_borough  = load_result("borough_stats")
    df_zones    = load_result("top_zones_by_month")

if df_platform.empty:
    st.error("No hay datos disponibles. Ejecuta primero: python3 src/run_pipeline.py")
    st.stop()

# ── Header ────────────────────────────────────────────────────────
st.title("NYC TLC — High Volume FHV Analytics")
st.caption(f"Uber y Lyft · Pipeline: Lambda + Pandas ETL + PySpark · Bucket: {BUCKET}")
st.divider()

# ── Sidebar: Filtros ──────────────────────────────────────────────
st.sidebar.title("Filtros")
st.sidebar.markdown("---")

years_available  = sorted(df_platform["year"].unique().tolist())
months_available = list(range(1, 13))
month_names      = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",
                    6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",
                    10:"Octubre",11:"Noviembre",12:"Diciembre"}

selected_years = st.sidebar.multiselect(
    "Año",
    options=years_available,
    default=years_available
)

selected_months = st.sidebar.multiselect(
    "Mes",
    options=months_available,
    format_func=lambda x: month_names[x],
    default=months_available
)

platforms_available = sorted(df_platform["platform"].unique().tolist())
selected_platforms  = st.sidebar.multiselect(
    "Plataforma",
    options=platforms_available,
    default=platforms_available
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Datos disponibles**")
for y in years_available:
    months_in_year = df_zones[df_zones["year"] == y]["month"].unique() if not df_zones.empty else []
    st.sidebar.markdown(f"- {int(y)}: {len(months_in_year)} mes(es)")

# ── Aplicar filtros ───────────────────────────────────────────────
df_plat_f = df_platform[
    (df_platform["year"].isin(selected_years)) &
    (df_platform["platform"].isin(selected_platforms))
]

df_zone_f = df_zones[
    (df_zones["year"].isin(selected_years)) &
    (df_zones["month"].isin(selected_months))
] if not df_zones.empty else df_zones

df_borough_f = df_borough[
    df_borough["year"].isin(selected_years)
] if not df_borough.empty else df_borough

df_hourly_f = df_hourly[
    df_hourly["platform"].isin(selected_platforms)
] if not df_hourly.empty else df_hourly

# ── KPIs ──────────────────────────────────────────────────────────
if not df_plat_f.empty:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total de Viajes",    f"{df_plat_f['total_trips'].sum():,.0f}")
    k2.metric("Revenue Total",      f"${df_plat_f['total_revenue'].sum():,.0f}")
    k3.metric("Tarifa Promedio",    f"${df_plat_f['avg_fare'].mean():.2f}")
    k4.metric("Periodos Analizados",f"{len(selected_years)} año(s)")
    st.divider()

# ── Fila 1: Viajes y tarifas ──────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Viajes por plataforma y año")
    if not df_plat_f.empty:
        fig = px.bar(
            df_plat_f, x="year", y="total_trips",
            color="platform", barmode="group",
            color_discrete_map=COLORS,
            labels={"total_trips": "Viajes", "year": "Año", "platform": "Plataforma"}
        )
        fig.update_layout(legend_title="Plataforma")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Tarifa promedio por año")
    if not df_plat_f.empty:
        fig2 = px.line(
            df_plat_f, x="year", y="avg_fare",
            color="platform", markers=True,
            color_discrete_map=COLORS,
            labels={"avg_fare": "Tarifa promedio ($)", "year": "Año", "platform": "Plataforma"}
        )
        fig2.update_layout(legend_title="Plataforma")
        st.plotly_chart(fig2, use_container_width=True)

# ── Fila 2: Demanda por hora ──────────────────────────────────────
st.subheader("Demanda por hora del dia")
if not df_hourly_f.empty:
    fig3 = px.area(
        df_hourly_f, x="hour", y="trips",
        color="platform",
        color_discrete_map=COLORS,
        labels={"trips": "Viajes", "hour": "Hora del dia", "platform": "Plataforma"}
    )
    fig3.update_layout(legend_title="Plataforma")
    st.plotly_chart(fig3, use_container_width=True)

# ── Fila 3: Borough y zonas ───────────────────────────────────────
col3, col4 = st.columns([1, 2])

with col3:
    st.subheader("Viajes por Borough")
    if not df_borough_f.empty:
        borough_agg = (df_borough_f
                       .groupby("pickup_borough")["trips"]
                       .sum().reset_index()
                       .sort_values("trips"))
        fig4 = px.bar(
            borough_agg,
            x="trips", y="pickup_borough",
            orientation="h",
            color="trips",
            color_continuous_scale="Blues",
            labels={"trips": "Viajes", "pickup_borough": "Borough"}
        )
        st.plotly_chart(fig4, use_container_width=True)

with col4:
    st.subheader("Top 10 zonas de pickup")
    if not df_zone_f.empty:
        c_year, c_month = st.columns(2)
        with c_year:
            year_sel = st.selectbox(
                "Año",
                options=sorted(df_zone_f["year"].unique()),
                index=len(df_zone_f["year"].unique()) - 1,
                key="zone_year"
            )
        with c_month:
            months_in_year = sorted(
                df_zone_f[df_zone_f["year"] == year_sel]["month"].unique()
            )
            month_sel = st.selectbox(
                "Mes",
                options=months_in_year,
                format_func=lambda x: month_names.get(int(x), str(x)),
                key="zone_month"
            )

        top = (df_zone_f[
                    (df_zone_f["year"]  == year_sel) &
                    (df_zone_f["month"] == month_sel)
               ]
               .groupby("pickup_zone")["trips"].sum()
               .nlargest(10).reset_index()
               .sort_values("trips"))

        fig5 = px.bar(
            top, x="trips", y="pickup_zone",
            orientation="h",
            color="trips",
            color_continuous_scale="Teal",
            labels={"trips": "Viajes", "pickup_zone": "Zona"}
        )
        st.plotly_chart(fig5, use_container_width=True)

st.divider()

# ── Log de ingesta ────────────────────────────────────────────────
with st.expander("Registro de ingesta (ingesta_log.json)"):
    df_log = load_ingesta_log()
    if not df_log.empty:
        df_log["ingested_at"] = pd.to_datetime(df_log["ingested_at"])
        df_log = df_log.sort_values("ingested_at", ascending=False)
        st.dataframe(
            df_log[["filename","year","month","file_size_mb","duration_s","status","ingested_at"]],
            use_container_width=True
        )
        ok_count  = len(df_log[df_log["status"] == "SUCCESS"])
        err_count = len(df_log[df_log["status"] == "ERROR"])
        st.caption(f"Total registros: {len(df_log)} | Exitosos: {ok_count} | Errores: {err_count}")
    else:
        st.info("No hay registros de ingesta disponibles.")

# ── Arquitectura ──────────────────────────────────────────────────
with st.expander("Arquitectura del Pipeline"):
    st.markdown("""
    ```
    NYC TLC CDN
        → Lambda chris-nyc-tlc-ingest (us-west-1)
        → S3 raw/year=YYYY/month=MM/
        → metadata/ingesta_log.json

    S3 raw/
        → Pandas ETL (chunks 1M filas, sin acumular RAM)
        → S3 processed/year=YYYY/month=MM/chunk_XX.parquet

    S3 processed/
        → PySpark pipeline (joins, agregaciones, RANK window)
        → S3 output/ (platform_by_year, demand_by_hour, borough_stats, top_zones)

    S3 output/
        → Streamlit Dashboard (esta app, Dockerizada)
        → GitHub Actions CI/CD (auto-deploy en cada push)

    IAM: testCurso-role-mdvpg0zi (least privilege)
    ```
    """)
