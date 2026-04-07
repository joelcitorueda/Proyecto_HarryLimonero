import streamlit as st
import pandas as pd
from dashboard.db import get_connection, get_connection2
import plotly.express as px

st.set_page_config(page_title=" Ventas - Terminal Tarija", layout="wide", page_icon="📊")

# ESTILOS MEJORADOS - CONTRATE OPTIMIZADO
st.markdown("""
<style>
/* Fondo negro elegante */
.stApp {
    background: #0a0a0a;
}
.main {
    background: transparent;
}
/* Títulos - más claros */
h1, h2, h3, .stSubheader {
    color: #00FFD0 !important;
    text-shadow: 0 0 20px rgba(0,255,208,0.2);
}
h1 {
    font-size: 2.5rem !important;
    border-bottom: 2px solid #00FFD0;
    display: inline-block;
    padding-bottom: 10px;
    margin-bottom: 20px;
    color: #00FFD0 !important;
}
/* Texto general más claro */
.stMarkdown, p, span, label {
    color: #E0E0E0 !important;
}
/* Tarjetas KPI */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #0f0f1a 100%);
    border-radius: 15px;
    padding: 15px;
    border: 1px solid #00FFD0;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    transition: all 0.3s ease;
}
[data-testid="metric-container"]:hover {
    transform: translateY(-3px);
    border-color: #00FFD0;
    box-shadow: 0 0 20px rgba(0,255,208,0.3);
    background: linear-gradient(135deg, #22223b 0%, #14142a 100%);
}
[data-testid="metric-label"] {
    color: #00FFD0 !important;
    font-weight: 600 !important;
    font-size: 1rem !important;
}
[data-testid="metric-value"] {
    color: #FFFFFF !important;
    font-size: 2rem !important;
    font-weight: bold !important;
}
/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f0f1a 0%, #0a0a0f 100%);
    border-right: 1px solid #00FFD0;
}
[data-testid="stSidebar"] .stMarkdown {
    color: #00FFD0 !important;
}
[data-testid="stSidebar"] h1, 
[data-testid="stSidebar"] h2, 
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p {
    color: #E0E0E0 !important;
}
/* Selectores */
.stMultiSelect [data-baseweb="select"] {
    background-color: #1a1a2e;
    border-color: #00FFD0;
}
.stMultiSelect [data-baseweb="select"] span {
    color: #E0E0E0 !important;
}
/* Contenedores de gráficos */
.stPlotlyChart {
    background: rgba(26,26,46,0.9);
    border-radius: 15px;
    padding: 15px;
    border: 1px solid rgba(0,255,208,0.3);
    margin-top: 20px;
}
/* Subtítulos */
.stSubheader {
    font-size: 1.5rem !important;
    margin-top: 30px !important;
    color: #00FFD0 !important;
}
/* Divisores */
hr {
    border-color: rgba(0,255,208,0.3);
}
/* Texto de info y warnings */
.stAlert {
    background-color: rgba(0,255,208,0.1) !important;
    color: #E0E0E0 !important;
}
/* Labels de filtros */
.css-1p1n2l6, .st-emotion-cache-1p1n2l6 {
    color: #00FFD0 !important;
}
</style>
""", unsafe_allow_html=True)

st.title("  Ventas")

# CONEXIÓN
conn = get_connection2()

# DATA
df = pd.read_sql("""
SELECT fecha_viaje, monto_pagado, canal_venta, estado_pasaje
FROM ventas_pasajes
""", conn)

df['fecha_viaje'] = pd.to_datetime(df['fecha_viaje'])
df['mes'] = df['fecha_viaje'].dt.to_period('M').astype(str)

# =====================
# FILTROS
# =====================
st.sidebar.markdown("# 🔎 Filtros")
st.sidebar.markdown("---")

canal = st.sidebar.multiselect(
    " Canal de Venta",
    df['canal_venta'].unique(),
    default=df['canal_venta'].unique()
)

estado = st.sidebar.multiselect(
    " Estado del Pasaje",
    df['estado_pasaje'].unique(),
    default=df['estado_pasaje'].unique()
)

df = df[(df['canal_venta'].isin(canal)) & (df['estado_pasaje'].isin(estado))]

# =====================
# KPIs
# =====================
st.markdown("---")
col1, col2, col3, col4 = st.columns(4)

col1.metric(" Ingresos Totales", f"{df['monto_pagado'].sum():,.2f} Bs")
col2.metric(" Pasajes Vendidos", f"{len(df):,}")
col3.metric(" Cancelados", f"{(df['estado_pasaje'] == 'Cancelado').sum():,}")
col4.metric(" Ticket Promedio", f"{df['monto_pagado'].mean():.2f} Bs")

# =====================
# GRÁFICOS
# =====================

st.markdown("---")
st.subheader(" Ventas por Mes")

ventas_mes = df.groupby('mes')['monto_pagado'].sum().reset_index()
fig1 = px.line(ventas_mes, x='mes', y='monto_pagado', 
               markers=True, line_shape='spline',
               color_discrete_sequence=['#00FFD0'])
fig1.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Mes",
    yaxis_title="Ingresos ($)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
)
st.plotly_chart(fig1, use_container_width=True)

st.subheader(" Ventas por Canal")
fig2 = px.pie(df, names='canal_venta', 
              color_discrete_sequence=px.colors.sequential.Tealgrn,
              hole=0.3)
fig2.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0'
)
fig2.update_traces(textfont_color='white', textfont_size=12)
st.plotly_chart(fig2, use_container_width=True)

st.subheader(" Estado de Pasajes")
estado_df = df['estado_pasaje'].value_counts().reset_index()
estado_df.columns = ['estado', 'cantidad']
fig3 = px.bar(estado_df, x='estado', y='cantidad',
              color='estado',
              color_discrete_sequence=px.colors.sequential.Tealgrn)
fig3.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    xaxis_title="Estado",
    yaxis_title="Cantidad",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
)
fig3.update_traces(textfont_color='white')
st.plotly_chart(fig3, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("<p style='text-align: center; color: #00FFD0; opacity: 0.8;'> Terminal Tarija BI - Inteligencia de Negocios</p>", unsafe_allow_html=True)