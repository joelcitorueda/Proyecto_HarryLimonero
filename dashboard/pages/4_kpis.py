import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px

st.set_page_config(page_title="KPIs Estratégicos y OKR - Terminal Tarija", layout="wide")

# ESTILO CON FONDO NEGRO
st.markdown("""
<style>
.stApp {
    background: #0a0a0a;
}
.main {
    background: transparent;
}
h1, h2, h3, .stSubheader {
    color: #00FFD0 !important;
    font-weight: 600;
}
h1 {
    font-size: 2.5rem !important;
    border-bottom: 3px solid #00FFD0;
    display: inline-block;
    padding-bottom: 10px;
    margin-bottom: 30px;
}
.stSubheader {
    font-size: 1.6rem !important;
    margin-top: 30px !important;
    margin-bottom: 20px !important;
    border-left: 4px solid #00FFD0;
    padding-left: 15px;
}
.stPlotlyChart {
    background: rgba(26,26,46,0.9);
    border-radius: 15px;
    padding: 15px;
    border: 1px solid rgba(0,255,208,0.3);
    margin-top: 10px;
    margin-bottom: 20px;
}
.stAlert {
    border-radius: 10px !important;
    margin-top: 10px !important;
    margin-bottom: 20px !important;
}
.stMarkdown, p, span, label {
    color: #E0E0E0 !important;
}
hr {
    border-color: rgba(0,255,208,0.3);
    margin: 30px 0;
}
.footer {
    text-align: center;
    color: #00FFD0;
    opacity: 0.7;
    padding: 20px;
    font-size: 0.8rem;
}
.question-box {
    background: rgba(0,255,208,0.05);
    border-left: 4px solid #00FFD0;
    padding: 15px;
    border-radius: 10px;
    margin-bottom: 20px;
}
.question-text {
    color: #00FFD0;
    font-size: 1.1rem;
    font-weight: bold;
    margin-bottom: 10px;
}
.answer-text {
    color: #E0E0E0;
    font-size: 1rem;
    line-height: 1.5;
}
.metric-card {
    background: rgba(26,26,46,0.9);
    border-radius: 10px;
    padding: 10px;
    text-align: center;
    border: 1px solid rgba(0,255,208,0.3);
}
</style>
""", unsafe_allow_html=True)

st.title("KPIs Estrategicos y OKR")
st.markdown("*Sistema de Inteligencia de Negocios - Terminal de Buses de Tarija*")

# CONEXION A GOLD
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-QC47F12;"
    "DATABASE=TerminalTarijaGold;"
    "Trusted_Connection=yes;"
)

# ============================================================================
# RESUMEN EJECUTIVO (Tarjetas principales)
# ============================================================================
st.markdown("---")
st.subheader("Resumen Ejecutivo")

df_resumen = pd.read_sql("SELECT * FROM vw_resumen_ejecutivo", conn)

if len(df_resumen) > 0:
    col_r1, col_r2, col_r3, col_r4, col_r5 = st.columns(5)
    
    with col_r1:
        st.metric("Pasajes Vendidos", f"{df_resumen.iloc[0]['total_pasajes_vendidos']:,.0f}")
    with col_r2:
        st.metric("Ingreso Total", f"{df_resumen.iloc[0]['ingreso_total_bs']:,.2f} Bs")
    with col_r3:
        st.metric("Ticket Promedio", f"{df_resumen.iloc[0]['ticket_promedio_bs']:.2f} Bs")
    with col_r4:
        tasa = df_resumen.iloc[0]['tasa_cancelacion_global_pct']
        st.metric("Cancelacion Global", f"{tasa:.1f}%")
    with col_r5:
        st.metric("Rutas Activas", f"{df_resumen.iloc[0]['rutas_activas']}")

# ============================================================================
# KPI 1 - TASA DE OCUPACION POR RUTA
# ============================================================================
st.markdown("---")
st.subheader("KPI 1: Tasa de Ocupacion por Ruta")

with st.container():
    st.markdown("""
    <div class="question-box">
        <div class="question-text">Pregunta de negocio:</div>
        <div class="answer-text">¿Cuantos asientos disponibles se venden realmente?</div>
        <div class="answer-text" style="font-size:0.9rem; margin-top:5px;">
        Formula: (Pasajes vendidos / Asientos totales ofertados) x 100<br>
        Umbral optimo: >75% = ruta rentable | <40% = ruta candidata a reducir frecuencias
        </div>
    </div>
    """, unsafe_allow_html=True)

df1 = pd.read_sql("""
    SELECT TOP 15 
        origen, 
        destino, 
        tasa_ocupacion_pct, 
        nivel_ocupacion,
        ingresos_bs,
        distancia_km,
        tarifa_base
    FROM vw_kpi1_tasa_ocupacion 
    WHERE anio = (SELECT MAX(anio) FROM vw_kpi1_tasa_ocupacion)
    ORDER BY tasa_ocupacion_pct DESC
""", conn)

fig1 = px.bar(df1, x='destino', y='tasa_ocupacion_pct',
              color='nivel_ocupacion',
              title="Top 15 Rutas por Tasa de Ocupacion - Ultimo Año",
              labels={'destino': 'Destino', 'tasa_ocupacion_pct': 'Tasa de Ocupacion (%)'},
              color_discrete_map={
                  'Alta': '#00FFD0',
                  'Media': '#FFC800',
                  'Baja': '#FF6B6B'
              })

fig1.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Destino",
    yaxis_title="Tasa de Ocupacion (%)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickangle=-45),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)', ticksuffix='%')
)

fig1.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
st.plotly_chart(fig1, use_container_width=True)

# Analisis detallado
rutas_alta = df1[df1['nivel_ocupacion'] == 'Alta']
rutas_baja = df1[df1['nivel_ocupacion'] == 'Baja']

if len(rutas_alta) > 0:
    st.success(f"Rutas con ocupacion ALTA (≥75%): {len(rutas_alta)} rutas - Requieren aumentar frecuencia de buses")
if len(rutas_baja) > 0:
    st.warning(f"Rutas con ocupacion BAJA (<40%): {len(rutas_baja)} rutas - Generan perdidas, revisar precios o eliminar")

# Tabla detallada
with st.expander("Ver detalle completo de rutas"):
    st.dataframe(df1, use_container_width=True)

# ============================================================================
# KPI 2 - INGRESO POR PASAJERO-KM
# ============================================================================
st.subheader("KPI 2: Ingreso por Pasajero-Kilometro")

with st.container():
    st.markdown("""
    <div class="question-box">
        <div class="question-text">Pregunta de negocio:</div>
        <div class="answer-text">¿Las rutas largas son mas rentables que las cortas en terminos relativos?</div>
        <div class="answer-text" style="font-size:0.9rem; margin-top:5px;">
        Formula: Ingresos totales / (Pasajes x distancia_km)<br>
        Benchmark: Comparado con volumen CEPALSTAT de Bolivia
        </div>
    </div>
    """, unsafe_allow_html=True)

df2 = pd.read_sql("""
    SELECT TOP 15 
        ciudad_origen,
        ciudad_destino,
        distancia_km,
        ingreso_por_pasajero_km,
        ingreso_total_bs,
        total_pasajes,
        tarifa_promedio_real,
        tarifa_base_referencia,
        diferencia_tarifa_bs,
        cepal_vol_pasajeros_km_bolivia
    FROM vw_kpi2_ingreso_por_km 
    WHERE anio = (SELECT MAX(anio) FROM vw_kpi2_ingreso_por_km)
    ORDER BY ingreso_por_pasajero_km DESC
""", conn)

# Obtener referencia CEPALSTAT
referencia_cepal = df2['cepal_vol_pasajeros_km_bolivia'].mean() if 'cepal_vol_pasajeros_km_bolivia' in df2.columns else 0.12

fig2 = px.bar(df2, x='ciudad_destino', y='ingreso_por_pasajero_km',
              title="Top 15 Rutas por Ingreso por Pasajero-KM - Ultimo Año",
              labels={'ciudad_destino': 'Ciudad Destino', 'ingreso_por_pasajero_km': 'Ingreso por Pasajero-KM (Bs)'},
              color='ingreso_por_pasajero_km',
              color_continuous_scale='Tealgrn')

if referencia_cepal > 0:
    fig2.add_hline(y=referencia_cepal, line_dash="dash", line_color="#FFC800", 
                   annotation_text=f"Referencia CEPALSTAT: Bs {referencia_cepal:.4f}/km", 
                   annotation_position="top right")

fig2.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Ciudad Destino",
    yaxis_title="Ingreso por Pasajero-KM (Bs)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickangle=-45),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
)

fig2.update_traces(texttemplate='Bs %{y:.4f}', textposition='outside')
st.plotly_chart(fig2, use_container_width=True)

# Analisis rutas largas vs cortas
st.info(f"**Analisis comparativo:** Referencia CEPALSTAT Bolivia: Bs {referencia_cepal:.4f} por pasajero-km")
df2['comparacion_cepal'] = df2['ingreso_por_pasajero_km'].apply(
    lambda x: "Supera referencia" if x > referencia_cepal else "Debajo referencia"
)
st.dataframe(df2[['ciudad_destino', 'distancia_km', 'ingreso_por_pasajero_km', 'comparacion_cepal', 'diferencia_tarifa_bs']], 
             use_container_width=True)

# ============================================================================
# KPI 3 - DEMANDA ESTACIONAL
# ============================================================================
st.subheader("KPI 3: Demanda Estacional")

with st.container():
    st.markdown("""
    <div class="question-box">
        <div class="question-text">Pregunta de negocio:</div>
        <div class="answer-text">¿En que meses se dispara la demanda? ¿Cuanto varia entre el mes pico y el valle?</div>
        <div class="answer-text" style="font-size:0.9rem; margin-top:5px;">
        Formula: Pasajes del mes / Promedio mensual del año x 100<br>
        >130 = mes pico (necesita mas frecuencias) | <70 = mes valle (puede reducir oferta)
        </div>
    </div>
    """, unsafe_allow_html=True)

df3 = pd.read_sql("""
    SELECT 
        anio,
        mes,
        nombre_mes,
        temporada,
        es_semana_santa,
        es_feria_tarija,
        es_fin_anio,
        pasajes_mes,
        ingreso_mes,
        cancelaciones_mes,
        indice_estacional,
        tipo_periodo,
        brecha_pico_valle
    FROM vw_kpi3_demanda_estacional
    WHERE anio = (SELECT MAX(anio) FROM vw_kpi3_demanda_estacional)
    ORDER BY mes
""", conn)

orden_meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 
               'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
df3['nombre_mes'] = pd.Categorical(df3['nombre_mes'], categories=orden_meses, ordered=True)
df3 = df3.sort_values('nombre_mes')

fig3 = px.line(df3, x='nombre_mes', y='indice_estacional',
               markers=True, 
               title=f"Indice de Demanda Estacional - {df3['anio'].iloc[0]}",
               labels={'nombre_mes': 'Mes', 'indice_estacional': 'Indice Estacional (base 100 = promedio anual)'},
               color_discrete_sequence=['#00FFD0'])

# Agregar zonas
fig3.add_hrect(y0=130, y1=df3['indice_estacional'].max() + 20, 
               line_width=0, fillcolor="#00FFD0", opacity=0.1,
               annotation_text="Pico Alto (>130)", annotation_position="top right")
fig3.add_hrect(y0=70, y1=130, 
               line_width=0, fillcolor="#FFC800", opacity=0.1,
               annotation_text="Normal (70-130)", annotation_position="top right")
fig3.add_hrect(y0=df3['indice_estacional'].min() - 20, y1=70, 
               line_width=0, fillcolor="#FF6B6B", opacity=0.1,
               annotation_text="Valle (<70)", annotation_position="top right")

fig3.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Mes",
    yaxis_title="Indice Estacional (base 100)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
)

fig3.update_traces(line=dict(width=3), marker=dict(size=8))
st.plotly_chart(fig3, use_container_width=True)

# Calculos de variacion
pico_mes = df3.loc[df3['indice_estacional'].idxmax(), 'nombre_mes']
pico_valor = df3['indice_estacional'].max()
valle_mes = df3.loc[df3['indice_estacional'].idxmin(), 'nombre_mes']
valle_valor = df3['indice_estacional'].min()
variacion = ((pico_valor - valle_valor) / valle_valor) * 100

st.info(f"**Variacion estacional:** El mes pico es {pico_mes} con indice {pico_valor:.0f} vs mes valle {valle_mes} con indice {valle_valor:.0f} - Diferencia del {variacion:.0f}%")

# Eventos especiales
st.markdown("**Eventos especiales identificados en la base de datos:**")
col_e1, col_e2, col_e3 = st.columns(3)
eventos = df3[df3['es_semana_santa'] == 1]
with col_e1:
    st.metric("Semana Santa", f"{len(eventos)} meses" if len(eventos) > 0 else "No registrado")
eventos2 = df3[df3['es_feria_tarija'] == 1]
with col_e2:
    st.metric("Feria de Tarija", f"{len(eventos2)} meses" if len(eventos2) > 0 else "No registrado")
eventos3 = df3[df3['es_fin_anio'] == 1]
with col_e3:
    st.metric("Fin de Año", f"{len(eventos3)} meses" if len(eventos3) > 0 else "No registrado")

# Tabla de demanda mensual
with st.expander("Ver demanda mensual detallada"):
    st.dataframe(df3, use_container_width=True)

# ============================================================================
# KPI 4 - CANCELACIONES POR EMPRESA
# ============================================================================
st.subheader("KPI 4: Cancelaciones por Empresa")

with st.container():
    st.markdown("""
    <div class="question-box">
        <div class="question-text">Pregunta de negocio:</div>
        <div class="answer-text">¿Que empresa cancela mas y cuanto dinero se pierde?</div>
        <div class="answer-text" style="font-size:0.9rem; margin-top:5px;">
        Formula: (Pasajes cancelados / Total pasajes) x 100<br>
        Insumo directo para regulacion y fiscalizacion ATT
        </div>
    </div>
    """, unsafe_allow_html=True)

df4 = pd.read_sql("""
    SELECT TOP 15 
        nombre_empresa,
        tipo_empresa,
        estado_empresa,
        total_pasajes,
        total_cancelados,
        total_efectivos,
        tasa_cancelacion_pct,
        ingreso_perdido_bs,
        ingreso_efectivo_bs,
        nivel_riesgo
    FROM vw_kpi4_cancelaciones_empresa
    WHERE anio = (SELECT MAX(anio) FROM vw_kpi4_cancelaciones_empresa)
    ORDER BY tasa_cancelacion_pct DESC
""", conn)

fig4 = px.bar(df4, x='nombre_empresa', y='tasa_cancelacion_pct',
              color='nivel_riesgo',
              title="Top 15 Empresas con Mayor Tasa de Cancelacion - Ultimo Año",
              labels={'nombre_empresa': 'Empresa', 'tasa_cancelacion_pct': 'Tasa de Cancelacion (%)'},
              color_discrete_map={
                  'Riesgo Alto': '#FF6B6B',
                  'Riesgo Medio': '#FFC800',
                  'Riesgo Bajo': '#00FFD0'
              })

fig4.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Empresa",
    yaxis_title="Tasa de Cancelacion (%)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickangle=-45),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)', ticksuffix='%')
)

fig4.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
st.plotly_chart(fig4, use_container_width=True)

# Analisis de riesgo
empresas_alto = df4[df4['nivel_riesgo'] == 'Riesgo Alto']
empresas_medio = df4[df4['nivel_riesgo'] == 'Riesgo Medio']

if len(empresas_alto) > 0:
    st.error(f"**EMPRESAS CON RIESGO ALTO (≥20% cancelaciones):** {', '.join(empresas_alto['nombre_empresa'].values)} - Requieren fiscalizacion ATT inmediata")
if len(empresas_medio) > 0:
    st.warning(f"**Empresas con Riesgo Medio (10-20% cancelaciones):** {', '.join(empresas_medio['nombre_empresa'].values)}")

# Perdidas economicas
total_perdido = df4['ingreso_perdido_bs'].sum()
st.info(f"**Impacto economico total por cancelaciones:** Bs {total_perdido:,.2f} de ingresos perdidos")

with st.expander("Ver detalle de cancelaciones por empresa"):
    st.dataframe(df4, use_container_width=True)

# ============================================================================
# OKR - CRECIMIENTO DE INGRESOS
# ============================================================================
st.subheader("OKR: Crecimiento de Ingresos Anual")

with st.container():
    st.markdown("""
    <div class="question-box">
        <div class="question-text">Pregunta de negocio:</div>
        <div class="answer-text">¿Vamos en camino al objetivo de 15% de crecimiento anual?</div>
        <div class="answer-text" style="font-size:0.9rem; margin-top:5px;">
        Formula: (Ingreso año actual - Ingreso año anterior) / Ingreso año anterior x 100<br>
        Objetivo estrategico: ≥15% de crecimiento anual
        </div>
    </div>
    """, unsafe_allow_html=True)

df5 = pd.read_sql("""
    SELECT 
        anio,
        total_pasajes,
        ingreso_total_bs,
        total_cancelados,
        ingreso_anio_anterior,
        crecimiento_pct,
        estado_okr
    FROM vw_okr_crecimiento_ingresos
    ORDER BY anio
""", conn)

fig5 = px.line(df5, x='anio', y='crecimiento_pct',
               markers=True, 
               title="Crecimiento Anual de Ingresos vs Objetivo Estrategico",
               labels={'anio': 'Año', 'crecimiento_pct': 'Crecimiento (%)'},
               color_discrete_sequence=['#00FFD0'])

fig5.add_hline(y=15, line_dash="dash", line_color="#FFC800", 
               annotation_text="Objetivo 15%", annotation_position="top right")

fig5.update_layout(
    plot_bgcolor='rgba(26,26,46,0)',
    paper_bgcolor='rgba(26,26,46,0)',
    font_color='#E0E0E0',
    font_size=12,
    xaxis_title="Año",
    yaxis_title="Crecimiento (%)",
    xaxis=dict(gridcolor='rgba(255,255,255,0.1)', tickmode='linear'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.1)', ticksuffix='%')
)

fig5.update_traces(line=dict(width=3), marker=dict(size=10))
st.plotly_chart(fig5, use_container_width=True)

# Evaluacion de cumplimiento
for _, row in df5.iterrows():
    if pd.notna(row['crecimiento_pct']):
        if row['crecimiento_pct'] >= 15:
            st.success(f"Año {row['anio']}: {row['crecimiento_pct']:.1f}% de crecimiento - {row['estado_okr']}")
        else:
            st.warning(f"Año {row['anio']}: {row['crecimiento_pct']:.1f}% de crecimiento - {row['estado_okr']}")

# Tabla historica
with st.expander("Ver historico de ingresos anuales"):
    st.dataframe(df5, use_container_width=True)
