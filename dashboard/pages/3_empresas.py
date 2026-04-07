import streamlit as st
import pandas as pd
from dashboard.db import get_connection, get_connection2
import plotly.express as px

st.set_page_config(page_title=" Empresas - Terminal Tarija", layout="wide", page_icon="🏢")

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

/* Tabla de datos */
.dataframe {
    background-color: #1a1a2e !important;
    color: #E0E0E0 !important;
    border-color: #00FFD0 !important;
}
</style>
""", unsafe_allow_html=True)

st.title(" Empresas")

# CONEXIÓN
conn = get_connection2()

# DATA
df = pd.read_sql("""
SELECT 
    e.nombre_empresa,
    COUNT(*) AS total_pasajes,
    SUM(v.monto_pagado) AS ingresos
FROM ventas_pasajes v
JOIN empresas e ON v.id_empresa = e.id_empresa
GROUP BY e.nombre_empresa
ORDER BY ingresos DESC
""", conn)

# Calcular porcentaje del mercado
df['porcentaje'] = (df['ingresos'] / df['ingresos'].sum()) * 100

# KPIs
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(" Empresa Top", df.iloc[0]['nombre_empresa'])
with col2:
    st.metric(" Ingresos Top", f"{df.iloc[0]['ingresos']:,.2f} Bs")
with col3:
    st.metric(" Participación Top", f"{df.iloc[0]['porcentaje']:.1f}%")

st.markdown("---")
st.subheader(" Ingresos por Empresa")

# GRÁFICO MEJORADO
fig = px.bar(df, 
             x='nombre_empresa', 
             y='ingresos',
             text='ingresos',
             title="",
             labels={'nombre_empresa': 'Empresa', 'ingresos': 'Ingresos ($)'},
             color='ingresos',
             color_continuous_scale='Tealgrn')

fig.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Empresa",
    yaxis_title="Ingresos ($)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='#00FFD0', tickangle=-45),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)', linecolor='#00FFD0'),
    showlegend=False
)

fig.update_traces(
    texttemplate='$%{y:,.0f}',
    textposition='outside',
    marker_line_width=1,
    marker_line_color='#00FFD0',
    opacity=0.9,
    textfont_color='#00FFD0'
)

st.plotly_chart(fig, use_container_width=True)

# GRÁFICO DE PASTEL - PARTICIPACIÓN
st.subheader(" Participación de Mercado")

fig2 = px.pie(df, 
              values='ingresos', 
              names='nombre_empresa',
              title="",
              hole=0.3,
              color_discrete_sequence=px.colors.sequential.Tealgrn)

fig2.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12
)

fig2.update_traces(
    textfont_color='#FFFFFF',
    textfont_size=12,
    textinfo='percent+label',
    pull=[0.05 if i == 0 else 0 for i in range(len(df))]
)

st.plotly_chart(fig2, use_container_width=True)

# MOSTRAR DATOS DETALLADOS
with st.expander(" Ver datos detallados"):
    df_display = df.copy()
    df_display['ingresos'] = df_display['ingresos'].apply(lambda x: f"${x:,.2f}")
    df_display['porcentaje'] = df_display['porcentaje'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(df_display, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("<p class='footer'> Terminal Tarija BI - Análisis de Empresas</p>", unsafe_allow_html=True)