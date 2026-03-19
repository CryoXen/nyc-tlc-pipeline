"""
04_dashboard.py — NYC TLC Analytics Dashboard
Pestanas: Vision General | Analisis por Periodo | Registro de Ingesta
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import glob, boto3, io, os, json
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="NYC TLC Analytics",
    layout="wide",
    initial_sidebar_state="collapsed"
)

BUCKET = os.getenv("BUCKET")
s3     = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-west-1"))

COLORS = {
    "Uber":  "#2563EB",
    "Lyft":  "#DC2626",
    "Other": "#6B7280"
}

MONTH_NAMES = {
    1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
    5:"Mayo", 6:"Junio", 7:"Julio", 8:"Agosto",
    9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"
}

def load_result(name):
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"output/{name}/")
        dfs  = []
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                data = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                dfs.append(pd.read_parquet(io.BytesIO(data["Body"].read())))
        return pd.concat(dfs) if dfs else pd.DataFrame()
    except Exception as e:
        st.error(f"Error cargando {name}: {e}")
        return pd.DataFrame()

def load_ingesta_log():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="metadata/ingesta_log.json")
        return pd.DataFrame(json.loads(obj["Body"].read()))
    except:
        return pd.DataFrame()

# ── Cargar datos ──────────────────────────────────────────────────
with st.spinner("Cargando datos desde S3..."):
    df_platform = load_result("platform_by_year")
    df_hourly   = load_result("demand_by_hour")
    df_borough  = load_result("borough_stats")
    df_zones    = load_result("top_zones_by_month")

if df_platform.empty:
    st.title("NYC TLC Analytics")
    st.error("No hay datos disponibles. Ejecuta primero: python3 src/run_pipeline.py")
    st.stop()

# ── Header ────────────────────────────────────────────────────────
st.title("NYC TLC — High Volume FHV Analytics")
st.caption(f"Uber y Lyft · Lambda + Pandas ETL + PySpark · Bucket: {BUCKET}")
st.divider()

# ── Pestanas ──────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "Vision General",
    "Analisis por Periodo",
    "Registro de Ingesta"
])

# ══════════════════════════════════════════════════════════════════
# TAB 1 — VISION GENERAL
# ══════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Resumen de todos los datos procesados")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Viajes",    f"{df_platform['total_trips'].sum():,.0f}")
    k2.metric("Revenue Total",   f"${df_platform['total_revenue'].sum():,.0f}")
    k3.metric("Tarifa Promedio", f"${df_platform['avg_fare'].mean():.2f}")
    k4.metric("Anos procesados", f"{df_platform['year'].nunique()}")
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Viajes por plataforma y ano**")
        fig = px.bar(
            df_platform.sort_values("year"),
            x="year", y="total_trips",
            color="platform", barmode="group",
            color_discrete_map=COLORS,
            labels={"total_trips":"Viajes","year":"Ano","platform":"Plataforma"}
        )
        fig.update_layout(xaxis=dict(tickmode="linear"), plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Tarifa promedio por ano**")
        fig2 = px.line(
            df_platform.sort_values("year"),
            x="year", y="avg_fare",
            color="platform", markers=True,
            color_discrete_map=COLORS,
            labels={"avg_fare":"Tarifa ($)","year":"Ano","platform":"Plataforma"}
        )
        fig2.update_layout(xaxis=dict(tickmode="linear"), plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.markdown("**Demanda por hora del dia**")
    fig3 = px.area(
        df_hourly.sort_values("hour"),
        x="hour", y="trips", color="platform",
        color_discrete_map=COLORS,
        labels={"trips":"Viajes","hour":"Hora","platform":"Plataforma"}
    )
    fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig3, use_container_width=True)

    st.divider()
    col3, col4 = st.columns(2)
    with col3:
        st.markdown("**Viajes por Borough**")
        borough_agg = (df_borough.groupby("pickup_borough")["trips"]
                       .sum().reset_index().sort_values("trips", ascending=True))
        fig4 = px.bar(
            borough_agg, x="trips", y="pickup_borough", orientation="h",
            color="trips", color_continuous_scale="Blues",
            labels={"trips":"Viajes","pickup_borough":"Borough"}
        )
        fig4.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig4, use_container_width=True)

    with col4:
        st.markdown("**Distribucion Uber vs Lyft**")
        pie_data = df_platform.groupby("platform")["total_trips"].sum().reset_index()
        fig5 = px.pie(
            pie_data, names="platform", values="total_trips",
            color="platform", color_discrete_map=COLORS, hole=0.4
        )
        fig5.update_traces(textinfo="percent+label")
        st.plotly_chart(fig5, use_container_width=True)

    st.divider()
    st.markdown("**Tabla resumen**")
    df_t = df_platform.copy()
    df_t["total_trips"]   = df_t["total_trips"].map("{:,.0f}".format)
    df_t["total_revenue"] = df_t["total_revenue"].map("${:,.0f}".format)
    df_t["avg_fare"]      = df_t["avg_fare"].map("${:.2f}".format)
    df_t["avg_miles"]     = df_t["avg_miles"].map("{:.2f} mi".format)
    df_t = df_t.rename(columns={
        "platform":"Plataforma","year":"Ano",
        "total_trips":"Viajes","total_revenue":"Revenue",
        "avg_fare":"Tarifa Prom.","avg_miles":"Millas Prom.",
        "shared_trips":"Compartidos"
    })
    st.dataframe(df_t, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════
# TAB 2 — ANALISIS POR PERIODO
# ══════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Analisis detallado por periodo")

    years_available = sorted(df_platform["year"].unique().tolist())
    f1, f2, f3 = st.columns(3)

    with f1:
        year_sel = st.selectbox(
            "Ano", options=years_available,
            index=len(years_available)-1
        )
    with f2:
        months_in_year = sorted(
            df_zones[df_zones["year"]==year_sel]["month"].unique().tolist()
        ) if not df_zones.empty else list(range(1,13))
        month_sel = st.selectbox(
            "Mes", options=months_in_year,
            format_func=lambda x: MONTH_NAMES.get(int(x), str(x))
        )
    with f3:
        platform_sel = st.multiselect(
            "Plataforma", options=["Uber","Lyft"],
            default=["Uber","Lyft"]
        )

    st.divider()

    df_year     = df_platform[(df_platform["year"]==year_sel) & (df_platform["platform"].isin(platform_sel))]
    df_hour_f   = df_hourly[df_hourly["platform"].isin(platform_sel)]
    df_borough_f= df_borough[df_borough["year"]==year_sel] if not df_borough.empty else pd.DataFrame()
    df_zone_f   = df_zones[(df_zones["year"]==year_sel)&(df_zones["month"]==month_sel)] if not df_zones.empty else pd.DataFrame()

    if not df_year.empty:
        st.markdown(f"**Metricas — {int(year_sel)} · {MONTH_NAMES.get(int(month_sel), '')}**")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Viajes del ano",     f"{df_year['total_trips'].sum():,.0f}")
        k2.metric("Revenue del ano",    f"${df_year['total_revenue'].sum():,.0f}")
        k3.metric("Tarifa promedio",    f"${df_year['avg_fare'].mean():.2f}")
        k4.metric("Viajes compartidos", f"{df_year['shared_trips'].sum():,.0f}")
        st.divider()

    col1, col2 = st.columns([3,2])
    with col1:
        st.markdown(f"**Top 10 zonas — {MONTH_NAMES.get(int(month_sel),'')} {int(year_sel)}**")
        if not df_zone_f.empty:
            top_z = (df_zone_f.groupby("pickup_zone")["trips"]
                     .sum().nlargest(10).reset_index()
                     .sort_values("trips", ascending=True))
            fig6 = px.bar(
                top_z, x="trips", y="pickup_zone", orientation="h",
                color="trips", color_continuous_scale="Blues",
                labels={"trips":"Viajes","pickup_zone":"Zona"}
            )
            fig6.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info(f"No hay datos de zonas para {MONTH_NAMES.get(int(month_sel),'')} {int(year_sel)}")

    with col2:
        st.markdown(f"**Borough — {int(year_sel)}**")
        if not df_borough_f.empty:
            b_year = (df_borough_f.groupby("pickup_borough")["trips"]
                      .sum().reset_index().sort_values("trips", ascending=True))
            fig7 = px.bar(
                b_year, x="trips", y="pickup_borough", orientation="h",
                color="trips", color_continuous_scale="Teal",
                labels={"trips":"Viajes","pickup_borough":"Borough"}
            )
            fig7.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig7, use_container_width=True)

    st.divider()
    st.markdown(f"**Demanda por hora — {int(year_sel)}**")
    if not df_hour_f.empty:
        fig8 = px.line(
            df_hour_f.sort_values("hour"),
            x="hour", y="trips", color="platform", markers=True,
            color_discrete_map=COLORS,
            labels={"trips":"Viajes","hour":"Hora","platform":"Plataforma"}
        )
        fig8.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig8, use_container_width=True)

    st.divider()
    if not df_year.empty:
        st.markdown(f"**Detalle — {int(year_sel)}**")
        df_d = df_year.copy()
        df_d["total_trips"]   = df_d["total_trips"].map("{:,.0f}".format)
        df_d["total_revenue"] = df_d["total_revenue"].map("${:,.0f}".format)
        df_d["avg_fare"]      = df_d["avg_fare"].map("${:.2f}".format)
        df_d["avg_miles"]     = df_d["avg_miles"].map("{:.2f} mi".format)
        df_d = df_d.rename(columns={
            "platform":"Plataforma","year":"Ano",
            "total_trips":"Viajes","total_revenue":"Revenue",
            "avg_fare":"Tarifa Prom.","avg_miles":"Millas Prom.",
            "shared_trips":"Compartidos"
        })
        st.dataframe(df_d, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════
# TAB 3 — REGISTRO DE INGESTA
# ══════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Historial de archivos ingestados")

    df_log = load_ingesta_log()
    if not df_log.empty:
        df_log["ingested_at"] = pd.to_datetime(df_log["ingested_at"])
        df_log = df_log.sort_values("ingested_at", ascending=False)

        l1, l2, l3, l4 = st.columns(4)
        l1.metric("Total archivos",  len(df_log))
        l2.metric("Exitosos",        len(df_log[df_log["status"]=="SUCCESS"]))
        l3.metric("Errores",         len(df_log[df_log["status"]=="ERROR"]))
        l4.metric("GB procesados",   f"{df_log['file_size_mb'].sum()/1024:.2f} GB")
        st.divider()

        df_show = df_log[["filename","year","month","file_size_mb","duration_s","status","ingested_at"]].copy()
        df_show["file_size_mb"] = df_show["file_size_mb"].map("{:.1f} MB".format)
        df_show["duration_s"]   = df_show["duration_s"].map("{:.1f} s".format)
        df_show = df_show.rename(columns={
            "filename":"Archivo","year":"Ano","month":"Mes",
            "file_size_mb":"Tamano","duration_s":"Duracion",
            "status":"Estado","ingested_at":"Fecha"
        })
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("**Tamano de archivos por periodo**")
        df_log["label"] = df_log["year"].astype(str)+"-"+df_log["month"].astype(str).str.zfill(2)
        fig_log = px.bar(
            df_log.sort_values("ingested_at"),
            x="label", y="file_size_mb",
            color="status",
            color_discrete_map={"SUCCESS":"#2563EB","ERROR":"#DC2626"},
            labels={"file_size_mb":"Tamano (MB)","label":"Periodo","status":"Estado"}
        )
        fig_log.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_log, use_container_width=True)
    else:
        st.info("No hay registros de ingesta aun.")
        st.markdown("""
        Para ingestar datos ejecuta:
        ```bash
        source venv/bin/activate
        python3 src/run_pipeline.py
        ```
        """)

    st.divider()
    with st.expander("Arquitectura del Pipeline"):
        st.markdown("""
        ```
        NYC TLC CDN
            Lambda chris-nyc-tlc-ingest (us-west-1)
            S3 raw/year=YYYY/month=MM/
            metadata/ingesta_log.json

        S3 raw/
            Pandas ETL (chunks 1M filas, sin acumular RAM)
            S3 processed/year=YYYY/month=MM/chunk_XX.parquet

        S3 processed/
            PySpark (joins, agregaciones, RANK window function)
            S3 output/ → platform_by_year, demand_by_hour,
                         borough_stats, top_zones_by_month

        S3 output/
            Streamlit Dashboard (Docker + GitHub Actions CI/CD)

        IAM: testCurso-role-mdvpg0zi (least privilege)
        ```
        """)
