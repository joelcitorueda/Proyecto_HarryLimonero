-- ============================================================
-- PROYECTO BI: Sistema BI - Terminal de Buses de Tarija
-- CAPA: GOLD - KPIs Estratégicos
-- ARCHIVO: gold/kpis.sql
-- BASE DE DATOS: TerminalTarijaGold
--
-- PREGUNTAS DE NEGOCIO QUE RESUELVE CADA KPI:
--
-- KPI 1 — ¿Cuánto porcentaje de los asientos disponibles
--          se venden realmente por ruta?
--          → Detecta rutas con baja ocupación donde se
--            pierde dinero y rutas saturadas donde falta oferta.
--
-- KPI 2 — ¿Cuánto ingresa la terminal por cada kilómetro
--          de distancia recorrida por pasajero?
--          → Compara si las tarifas largas son más rentables
--            que las cortas. Justifica ajustes tarifarios.
--
-- KPI 3 — ¿En qué meses o temporadas se concentra la
--          demanda de pasajes?
--          → Permite planificar cuántas frecuencias abrir
--            en temporada alta y cuántas cerrar en baja.
--
-- KPI 4 — ¿Qué empresa tiene más cancelaciones y cuánto
--          dinero se pierde por eso?
--          → Identifica operadores con problemas de
--            confiabilidad. Insumo para fiscalización ATT.
--
-- OKR  — ¿Cuánto creció el ingreso total este año vs el
--          anterior? ¿Vamos en camino al objetivo anual?
-- ============================================================

USE TerminalTarijaGold;
GO

-- ============================================================
-- KPI 1: TASA DE OCUPACIÓN POR RUTA
--
-- PREGUNTA: ¿Cuánto porcentaje de los asientos disponibles
--           se vendieron realmente en cada ruta?
--
-- FÓRMULA: (Pasajes vendidos / Asientos totales ofertados) × 100
-- UMBRAL ÓPTIMO: > 75% = ruta rentable
--               < 40% = ruta candidata a reducir frecuencias
-- ============================================================
CREATE OR ALTER VIEW vw_kpi1_tasa_ocupacion AS
SELECT
    r.ciudad_origen                         AS origen,
    r.ciudad_destino                        AS destino,
    r.tipo_corredor,
    t.anio,
    t.nombre_mes                            AS mes,
    t.mes                                   AS num_mes,
    t.temporada,

    -- Total de viajes realizados (excluye cancelados)
    COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END)
                                            AS pasajes_vendidos,

    -- Capacidad total ofertada en ese período
    SUM(CASE WHEN f.es_cancelado = 0
             THEN f.capacidad_unidad ELSE 0 END)
                                            AS asientos_ofertados,

    -- KPI: Tasa de ocupación
    CASE
        WHEN SUM(CASE WHEN f.es_cancelado = 0
                      THEN f.capacidad_unidad ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            100.0 * COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END) /
            SUM(CASE WHEN f.es_cancelado = 0
                     THEN f.capacidad_unidad ELSE 0 END),
            2)
    END                                     AS tasa_ocupacion_pct,

    -- Clasificación semáforo para el dashboard
    CASE
        WHEN ROUND(
            100.0 * COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END) /
            NULLIF(SUM(CASE WHEN f.es_cancelado = 0
                           THEN f.capacidad_unidad ELSE 0 END), 0),
            2) >= 75 THEN 'Alta'
        WHEN ROUND(
            100.0 * COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END) /
            NULLIF(SUM(CASE WHEN f.es_cancelado = 0
                           THEN f.capacidad_unidad ELSE 0 END), 0),
            2) >= 40 THEN 'Media'
        ELSE 'Baja'
    END                                     AS nivel_ocupacion,

    -- Ingresos generados en esa ruta/período
    SUM(CASE WHEN f.es_cancelado = 0
             THEN f.monto_pagado ELSE 0 END) AS ingresos_bs,

    r.distancia_km,
    r.tarifa_base

FROM fact_ventas f
JOIN dim_ruta    r ON f.sk_ruta   = r.sk_ruta
JOIN dim_tiempo  t ON f.sk_tiempo = t.sk_tiempo
GROUP BY
    r.ciudad_origen, r.ciudad_destino, r.tipo_corredor,
    t.anio, t.nombre_mes, t.mes, t.temporada,
    r.distancia_km, r.tarifa_base;
GO


-- ============================================================
-- KPI 2: INGRESO POR PASAJERO-KILÓMETRO
--
-- PREGUNTA: ¿Cuántos bolivianos ingresa la terminal por cada
--           km recorrido? ¿Las rutas largas son más rentables
--           que las cortas en términos relativos?
--
-- FÓRMULA: Ingresos totales / (Pasajes × distancia_km)
-- BENCHMARK: Comparado con el volumen CEPALSTAT de Bolivia
-- ============================================================
CREATE OR ALTER VIEW vw_kpi2_ingreso_por_km AS
SELECT
    r.ciudad_origen,
    r.ciudad_destino,
    r.tipo_corredor,
    r.distancia_km,
    t.anio,
    t.trimestre,

    -- Volumen de pasajes (no cancelados)
    COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END)
                                            AS total_pasajes,

    -- Ingreso total de la ruta en el período
    SUM(CASE WHEN f.es_cancelado = 0
             THEN f.monto_pagado ELSE 0 END) AS ingreso_total_bs,

    -- Pasajeros-kilómetro producidos internamente
    SUM(CASE WHEN f.es_cancelado = 0
             THEN CAST(f.distancia_km AS BIGINT) ELSE 0 END)
                                            AS pasajeros_km_terminal,

    -- KPI: Ingreso por pasajero-kilómetro
    CASE
        WHEN SUM(CASE WHEN f.es_cancelado = 0
                      THEN f.distancia_km ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN f.es_cancelado = 0
                     THEN f.monto_pagado ELSE 0 END) /
            NULLIF(SUM(CASE WHEN f.es_cancelado = 0
                            THEN CAST(f.distancia_km AS FLOAT)
                            ELSE 0 END), 0),
            4)
    END                                     AS ingreso_por_pasajero_km,

    -- Contexto CEPALSTAT: volumen nacional (pasajeros-km aéreo Bolivia)
    AVG(f.cepal_volumen_pasajeros_km)       AS cepal_vol_pasajeros_km_bolivia,

    -- Tarifa promedio cobrada vs tarifa base
    AVG(CASE WHEN f.es_cancelado = 0
             THEN f.monto_pagado END)       AS tarifa_promedio_real,
    AVG(f.tarifa_base)                      AS tarifa_base_referencia,

    -- Diferencia tarifa real vs base
    ROUND(
        AVG(CASE WHEN f.es_cancelado = 0 THEN f.monto_pagado END) -
        AVG(f.tarifa_base),
    2)                                      AS diferencia_tarifa_bs

FROM fact_ventas f
JOIN dim_ruta   r ON f.sk_ruta   = r.sk_ruta
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
GROUP BY
    r.ciudad_origen, r.ciudad_destino, r.tipo_corredor,
    r.distancia_km, t.anio, t.trimestre;
GO


-- ============================================================
-- KPI 3: ÍNDICE DE DEMANDA ESTACIONAL
--
-- PREGUNTA: ¿En qué meses o temporadas se dispara la demanda?
--           ¿Cuánto varía la demanda entre el mes más alto
--           y el más bajo del año?
--
-- FÓRMULA: Pasajes del mes / Promedio mensual del año × 100
-- > 130 = mes pico (necesita más frecuencias)
-- < 70  = mes valle (puede reducir oferta)
-- ============================================================
CREATE OR ALTER VIEW vw_kpi3_demanda_estacional AS
WITH ventas_mensuales AS (
    SELECT
        t.anio,
        t.mes,
        t.nombre_mes,
        t.temporada,
        t.es_semana_santa,
        t.es_feria_tarija,
        t.es_fin_anio,
        COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END) AS pasajes_mes,
        SUM(CASE WHEN f.es_cancelado = 0
                 THEN f.monto_pagado ELSE 0 END)        AS ingreso_mes,
        COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) AS cancelaciones_mes
    FROM fact_ventas f
    JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
    GROUP BY
        t.anio, t.mes, t.nombre_mes, t.temporada,
        t.es_semana_santa, t.es_feria_tarija, t.es_fin_anio
),
promedio_anual AS (
    SELECT
        anio,
        AVG(CAST(pasajes_mes AS FLOAT)) AS promedio_mensual_anio,
        MAX(pasajes_mes)                AS mes_pico_pasajes,
        MIN(pasajes_mes)                AS mes_valle_pasajes
    FROM ventas_mensuales
    GROUP BY anio
)
SELECT
    vm.anio,
    vm.mes,
    vm.nombre_mes,
    vm.temporada,
    vm.es_semana_santa,
    vm.es_feria_tarija,
    vm.es_fin_anio,
    vm.pasajes_mes,
    vm.ingreso_mes,
    vm.cancelaciones_mes,
    pa.promedio_mensual_anio,
    pa.mes_pico_pasajes,
    pa.mes_valle_pasajes,

    -- KPI: Índice estacional (100 = promedio, >100 = sobre el promedio)
    CASE
        WHEN pa.promedio_mensual_anio = 0 THEN 0
        ELSE ROUND(
            100.0 * vm.pasajes_mes / pa.promedio_mensual_anio,
            1)
    END                                 AS indice_estacional,

    -- Clasificación para el dashboard
    CASE
        WHEN 100.0 * vm.pasajes_mes /
             NULLIF(pa.promedio_mensual_anio, 0) >= 130 THEN 'Pico alto'
        WHEN 100.0 * vm.pasajes_mes /
             NULLIF(pa.promedio_mensual_anio, 0) >= 100 THEN 'Normal alto'
        WHEN 100.0 * vm.pasajes_mes /
             NULLIF(pa.promedio_mensual_anio, 0) >= 70  THEN 'Normal bajo'
        ELSE 'Valle'
    END                                 AS tipo_periodo,

    -- Brecha pico-valle del año (amplitud de la estacionalidad)
    pa.mes_pico_pasajes - pa.mes_valle_pasajes
                                        AS brecha_pico_valle

FROM ventas_mensuales vm
JOIN promedio_anual   pa ON vm.anio = pa.anio;
GO


-- ============================================================
-- KPI 4: TASA DE CANCELACIÓN POR EMPRESA
--
-- PREGUNTA: ¿Qué empresa cancela más servicios?
--           ¿Cuánto dinero se pierde por cancelaciones?
--           → Insumo directo para regulación y fiscalización
-- ============================================================
CREATE OR ALTER VIEW vw_kpi4_cancelaciones_empresa AS
SELECT
    e.nombre_empresa,
    e.tipo_empresa,
    e.estado_empresa,
    t.anio,
    t.nombre_mes,

    COUNT(*)                                    AS total_pasajes,
    COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END)
                                                AS total_cancelados,
    COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END)
                                                AS total_efectivos,

    -- KPI: Tasa de cancelación
    ROUND(
        100.0 * COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) /
        NULLIF(COUNT(*), 0),
    2)                                          AS tasa_cancelacion_pct,

    -- Ingreso perdido por cancelaciones (a precio de lista)
    SUM(CASE WHEN f.es_cancelado = 1
             THEN f.tarifa_base ELSE 0 END)     AS ingreso_perdido_bs,

    -- Ingreso efectivo generado
    SUM(CASE WHEN f.es_cancelado = 0
             THEN f.monto_pagado ELSE 0 END)    AS ingreso_efectivo_bs,

    -- Clasificación de riesgo para el dashboard
    CASE
        WHEN ROUND(
            100.0 * COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) /
            NULLIF(COUNT(*), 0), 2) >= 20 THEN 'Riesgo Alto'
        WHEN ROUND(
            100.0 * COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) /
            NULLIF(COUNT(*), 0), 2) >= 10 THEN 'Riesgo Medio'
        ELSE 'Riesgo Bajo'
    END                                         AS nivel_riesgo

FROM fact_ventas f
JOIN dim_empresa e ON f.sk_empresa = e.sk_empresa
JOIN dim_tiempo  t ON f.sk_tiempo  = t.sk_tiempo
GROUP BY
    e.nombre_empresa, e.tipo_empresa, e.estado_empresa,
    t.anio, t.nombre_mes;
GO


-- ============================================================
-- OKR: CRECIMIENTO DE INGRESOS ANUAL
--
-- PREGUNTA: ¿Cuánto creció el ingreso total este año
--           respecto al anterior? ¿Vamos en camino hacia
--           el objetivo de crecimiento del 15%?
-- ============================================================
CREATE OR ALTER VIEW vw_okr_crecimiento_ingresos AS
WITH ingresos_anuales AS (
    SELECT
        t.anio,
        COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END) AS total_pasajes,
        SUM(CASE WHEN f.es_cancelado = 0
                 THEN f.monto_pagado ELSE 0 END)        AS ingreso_total_bs,
        COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) AS total_cancelados
    FROM fact_ventas f
    JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
    GROUP BY t.anio
)
SELECT
    ia.anio,
    ia.total_pasajes,
    ia.ingreso_total_bs,
    ia.total_cancelados,
    LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio)
                                            AS ingreso_anio_anterior,

    -- OKR: % de crecimiento vs año anterior
    CASE
        WHEN LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio) IS NULL
        THEN NULL
        ELSE ROUND(
            100.0 * (ia.ingreso_total_bs -
                LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio)) /
            NULLIF(LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio), 0),
        2)
    END                                     AS crecimiento_pct,

    -- Objetivo: 15% de crecimiento anual
    CASE
        WHEN LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio) IS NULL
        THEN NULL
        WHEN ROUND(
            100.0 * (ia.ingreso_total_bs -
                LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio)) /
            NULLIF(LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio), 0),
        2) >= 15 THEN 'OKR Cumplido ✓'
        WHEN ROUND(
            100.0 * (ia.ingreso_total_bs -
                LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio)) /
            NULLIF(LAG(ia.ingreso_total_bs) OVER (ORDER BY ia.anio), 0),
        2) >= 0  THEN 'OKR En progreso'
        ELSE 'OKR No cumplido ✗'
    END                                     AS estado_okr

FROM ingresos_anuales ia;
GO


-- ============================================================
-- VISTA RESUMEN EJECUTIVO (para la tarjeta principal
-- del dashboard — métricas globales de un vistazo)
-- ============================================================
CREATE OR ALTER VIEW vw_resumen_ejecutivo AS
SELECT
    COUNT(CASE WHEN f.es_cancelado = 0 THEN 1 END)
                                AS total_pasajes_vendidos,
    SUM(CASE WHEN f.es_cancelado = 0
             THEN f.monto_pagado ELSE 0 END)
                                AS ingreso_total_bs,
    ROUND(
        100.0 * COUNT(CASE WHEN f.es_cancelado = 1 THEN 1 END) /
        NULLIF(COUNT(*), 0), 2) AS tasa_cancelacion_global_pct,
    ROUND(
        AVG(CASE WHEN f.es_cancelado = 0
                 THEN f.monto_pagado END), 2)
                                AS ticket_promedio_bs,
    COUNT(DISTINCT f.sk_ruta)   AS rutas_activas,
    COUNT(DISTINCT f.sk_empresa)AS empresas_activas,
    MAX(t.fecha)                AS ultimo_registro
FROM fact_ventas f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo;
GO


-- ============================================================
-- VERIFICACIÓN FINAL: Contar registros en todas las vistas
-- ============================================================
SELECT 'vw_kpi1_tasa_ocupacion'      AS vista, COUNT(*) AS filas FROM vw_kpi1_tasa_ocupacion
UNION ALL
SELECT 'vw_kpi2_ingreso_por_km',      COUNT(*) FROM vw_kpi2_ingreso_por_km
UNION ALL
SELECT 'vw_kpi3_demanda_estacional',  COUNT(*) FROM vw_kpi3_demanda_estacional
UNION ALL
SELECT 'vw_kpi4_cancelaciones_empresa', COUNT(*) FROM vw_kpi4_cancelaciones_empresa
UNION ALL
SELECT 'vw_okr_crecimiento_ingresos', COUNT(*) FROM vw_okr_crecimiento_ingresos
UNION ALL
SELECT 'vw_resumen_ejecutivo',        COUNT(*) FROM vw_resumen_ejecutivo;
GO

PRINT 'KPIs y vistas creadas exitosamente en TerminalTarijaGold.';
GO