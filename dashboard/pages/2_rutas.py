import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px

st.set_page_config(page_title=" Rutas - Terminal Tarija", layout="wide", page_icon="🛣️")

# ESTILO CON FONDO NEGRO
st.markdown("""
<style>
/* Fondo negro */
.stApp {
    background: #0a0a0a;
}
.main {
    background: transparent;
}

/* Títulos */
h1, h2, h3, .stSubheader {
    color: #00FFD0 !important;
    font-weight: 600;
    text-shadow: 0 0 20px rgba(0,255,208,0.2);
}

h1 {
    font-size: 2.5rem !important;
    border-bottom: 3px solid #00FFD0;
    display: inline-block;
    padding-bottom: 10px;
    margin-bottom: 20px;
}

/* Tarjeta de métrica */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #0f0f1a 100%);
    border-radius: 15px;
    padding: 20px;
    border: 1px solid #00FFD0;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    transition: all 0.3s ease;
    margin-bottom: 30px;
}

[data-testid="metric-container"]:hover {
    transform: translateY(-3px);
    box-shadow: 0 0 20px rgba(0,255,208,0.2);
    border-color: #00FFD0;
}

[data-testid="metric-label"] {
    color: #00FFD0 !important;
    font-weight: 600 !important;
    font-size: 1rem !important;
}

[data-testid="metric-value"] {
    color: #FFFFFF !important;
    font-size: 1.8rem !important;
    font-weight: 700 !important;
}

/* Contenedor del gráfico */
.stPlotlyChart {
    background: rgba(26,26,46,0.9);
    border-radius: 15px;
    padding: 15px;
    border: 1px solid rgba(0,255,208,0.3);
    margin-top: 20px;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f0f1a 0%, #0a0a0f 100%);
    border-right: 1px solid #00FFD0;
}

[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] p {
    color: #E0E0E0 !important;
}

/* Texto general */
.stMarkdown, p, span, label {
    color: #E0E0E0 !important;
}

/* Divisores */
hr {
    border-color: rgba(0,255,208,0.3);
    margin: 20px 0;
}

/* Footer */
.footer {
    text-align: center;
    color: #00FFD0;
    opacity: 0.7;
    padding: 20px;
    font-size: 0.8rem;
}
</style>
""", unsafe_allow_html=True)

st.title(" Rutas")

# CONEXIÓN
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-QC47F12;"
    "DATABASE=TerminalTarijaDB;"
    "Trusted_Connection=yes;"
)

# DATA
df = pd.read_sql("""
SELECT 
    g1.ciudad AS origen,
    g2.ciudad AS destino,
    COUNT(*) AS total_viajes,
    SUM(v.monto_pagado) AS ingresos
FROM ventas_pasajes v
JOIN rutas r ON v.id_ruta = r.id_ruta
JOIN geografia g1 ON r.ciudad_origen_id = g1.id_ciudad
JOIN geografia g2 ON r.ciudad_destino_id = g2.id_ciudad
GROUP BY g1.ciudad, g2.ciudad
ORDER BY total_viajes DESC
""", conn)

# TOP 10
df_top = df.head(10)

st.markdown("---")
st.metric(" Ruta más usada", f"{df_top.iloc[0]['origen']} → {df_top.iloc[0]['destino']}")

# GRÁFICO MEJORADO
st.markdown("---")
st.subheader(" Top 10 Rutas por Cantidad de Viajes")

fig = px.bar(df_top, 
             x='destino', 
             y='total_viajes', 
             color='origen',
             title="",
             labels={'destino': 'Destino', 'total_viajes': 'Total Viajes', 'origen': 'Origen'},
             color_discrete_sequence=px.colors.sequential.Tealgrn)

fig.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Ciudad de Destino",
    yaxis_title="Número de Viajes",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='#00FFD0'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='#00FFD0'),
    showlegend=True,
    legend_title_text='Origen'
)

fig.update_traces(
    texttemplate='%{y}',
    textposition='outside',
    marker_line_width=1,
    marker_line_color='#00FFD0',
    opacity=0.9
)

st.plotly_chart(fig, use_container_width=True)

# MOSTRAR DATOS ADICIONALES
with st.expander(" Ver datos detallados"):
    st.dataframe(df_top.style.format({'ingresos': '${:,.2f}'}), use_container_width=True)

# Footer
st.markdown("---")
st.markdown("<p class='footer'> Terminal Tarija BI - Análisis de Rutas</p>", unsafe_allow_html=True)