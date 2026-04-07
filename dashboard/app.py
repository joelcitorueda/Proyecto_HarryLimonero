import streamlit as st
import pandas as pd
from dashboard.db import get_connection

# =========================
# CONFIGURACION
# =========================
st.set_page_config(
    page_title="BI Terminal Tarija",
    layout="wide",
    page_icon="📊"
)


st.markdown("""
<style>
.main {
    background-color: #0E1117;
}
h1, h2, h3 {
    color: #00C9A7;
}
.metric-box {
    background: linear-gradient(135deg, #1E1E2F 0%, #1A1A2E 100%);
    padding: 20px;
    border-radius: 15px;
    text-align: center;
    box-shadow: 0 8px 20px rgba(0,0,0,0.3);
}
.metric-value {
    font-size: 2rem;
    font-weight: bold;
    color: #00C9A7;
}
.metric-label {
    font-size: 1rem;
    color: #ccc;
}
</style>
""", unsafe_allow_html=True)

# =========================
# HEADER
# =========================
st.title("Sistema BI - Terminal Tarija")
st.markdown("### Inteligencia de Negocios para la toma de decisiones")

st.markdown("---")

# =========================
# CARGAR DATOS EJECUTIVOS
# =========================
conn = get_connection()

df = pd.read_sql("SELECT * FROM vw_resumen_ejecutivo", conn)

# =========================
# KPIs PRINCIPALES
# =========================
col1, col2, col3, col4 = st.columns(4)

col1.markdown(f"""
<div class="metric-box">
    <div class="metric-value">{int(df['total_pasajes_vendidos'][0])}</div>
    <div class="metric-label">Pasajes Vendidos</div>
</div>
""", unsafe_allow_html=True)

col2.markdown(f"""
<div class="metric-box">
    <div class="metric-value">{round(df['ingreso_total_bs'][0],2)} Bs</div>
    <div class="metric-label">Ingreso Total</div>
</div>
""", unsafe_allow_html=True)

col3.markdown(f"""
<div class="metric-box">
    <div class="metric-value">{df['tasa_cancelacion_global_pct'][0]}%</div>
    <div class="metric-label">Cancelaciones</div>
</div>
""", unsafe_allow_html=True)

col4.markdown(f"""
<div class="metric-box">
    <div class="metric-value">{df['ticket_promedio_bs'][0]} Bs</div>
    <div class="metric-label">Ticket Promedio</div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# =========================
# STORYTELLING
# =========================
st.subheader("Insight Ejecutivo")

st.info("""
El sistema permite analizar el comportamiento de ventas en la terminal.
Se identifican rutas con alta demanda y oportunidades de optimizacion.
Se detectan perdidas por cancelaciones.
Se evalua el crecimiento del negocio en el tiempo.
""")

st.markdown("---")

# =========================
# TARJETAS DE NAVEGACION
# =========================
col1, col2, col3 = st.columns(3)

col1.markdown("### Analisis de Ingresos")
col1.success("Permite evaluar rentabilidad por rutas y periodos")

col2.markdown("### Analisis de Rutas")
col2.success("Detecta rutas saturadas o con baja ocupacion")

col3.markdown("### Empresas")
col3.success("Evalua desempeno y nivel de cancelaciones")

st.markdown("---")

# =========================
# KPIs DE NEGOCIO
# =========================
st.subheader("KPIs Estrategicos")

st.markdown("""
- KPI 1: Tasa de Ocupacion → Detecta eficiencia operativa
- KPI 2: Ingreso por KM → Evalua rentabilidad
- KPI 3: Demanda Estacional → Optimiza planificacion
- KPI 4: Cancelaciones → Control de calidad empresarial
- OKR: Crecimiento anual → Medicion estrategica
""")

# =========================
# FOOTER
# =========================
st.markdown("---")
st.caption("Proyecto BI - UPDS | Inteligencia de Negocios")