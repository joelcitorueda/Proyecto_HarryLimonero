-- ============================================================
-- PROYECTO BI: Sistema BI - Terminal de Buses de Tarija
-- CAPA: GOLD - Star Schema (Modelo Estrella)
-- BASE DE DATOS: TerminalTarijaGold
-- ============================================================

-- Crear base de datos Gold
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TerminalTarijaGold')
    CREATE DATABASE TerminalTarijaGold;
GO

USE TerminalTarijaGold;
GO

-- ============================================================
-- LIMPIEZA (re-ejecución segura)
-- ============================================================
IF OBJECT_ID('fact_ventas',     'U') IS NOT NULL DROP TABLE fact_ventas;
IF OBJECT_ID('dim_tiempo',      'U') IS NOT NULL DROP TABLE dim_tiempo;
IF OBJECT_ID('dim_ruta',        'U') IS NOT NULL DROP TABLE dim_ruta;
IF OBJECT_ID('dim_empresa',     'U') IS NOT NULL DROP TABLE dim_empresa;
IF OBJECT_ID('dim_pasajero',    'U') IS NOT NULL DROP TABLE dim_pasajero;
IF OBJECT_ID('dim_unidad',      'U') IS NOT NULL DROP TABLE dim_unidad;
IF OBJECT_ID('dim_canal',       'U') IS NOT NULL DROP TABLE dim_canal;
GO

-- ============================================================
-- DIMENSIÓN: dim_tiempo
-- Responde: ¿En qué fechas, meses y temporadas se vende más?
-- ============================================================
CREATE TABLE dim_tiempo (
    sk_tiempo       INT PRIMARY KEY,        -- surrogate key: YYYYMMDD
    fecha           DATE NOT NULL,
    anio            INT  NOT NULL,
    trimestre       INT  NOT NULL,          -- 1-4
    mes             INT  NOT NULL,          -- 1-12
    nombre_mes      VARCHAR(20) NOT NULL,
    semana_anio     INT  NOT NULL,
    dia_semana      INT  NOT NULL,          -- 1=Domingo, 7=Sabado
    nombre_dia      VARCHAR(20) NOT NULL,
    es_fin_semana   BIT  NOT NULL DEFAULT 0,
    temporada       VARCHAR(30) NOT NULL,   -- Alta / Media / Baja
    -- Temporadas clave para transporte boliviano
    es_semana_santa BIT  NOT NULL DEFAULT 0,
    es_feria_tarija BIT  NOT NULL DEFAULT 0,
    es_fin_anio     BIT  NOT NULL DEFAULT 0
);
GO

-- ============================================================
-- DIMENSIÓN: dim_ruta
-- Responde: ¿Qué rutas son más rentables? ¿Cuáles tienen
--           mayor demanda? ¿Qué corredor conviene reforzar?
-- ============================================================
CREATE TABLE dim_ruta (
    sk_ruta             INT PRIMARY KEY,
    id_ruta_origen      INT NOT NULL,       -- FK lógica al origen
    ciudad_origen       VARCHAR(100) NOT NULL,
    departamento_origen VARCHAR(100) NOT NULL,
    ciudad_destino      VARCHAR(100) NOT NULL,
    departamento_destino VARCHAR(100) NOT NULL,
    distancia_km        INT  NOT NULL,
    duracion_horas      DECIMAL(4,1) NOT NULL,
    tipo_corredor       VARCHAR(30)  NOT NULL, -- Interdepartamental/Interprovincial/Fronterizo
    tarifa_base         DECIMAL(8,2) NOT NULL,
    ingreso_por_km      AS (tarifa_base / distancia_km) PERSISTED -- columna calculada
);
GO

-- ============================================================
-- DIMENSIÓN: dim_empresa
-- Responde: ¿Qué empresa opera mejor? ¿Cuáles tienen más
--           cancelaciones? ¿Cuál genera más ingresos?
-- ============================================================
CREATE TABLE dim_empresa (
    sk_empresa          INT PRIMARY KEY,
    id_empresa_origen   INT NOT NULL,
    nombre_empresa      VARCHAR(150) NOT NULL,
    tipo_empresa        VARCHAR(30)  NOT NULL,  -- Flota/Trufi/Microbus/Mixta
    estado_empresa      VARCHAR(20)  NOT NULL,  -- Activa/Suspendida/Inhabilitada
    capacidad_flota_total INT        NOT NULL
);
GO

-- ============================================================
-- DIMENSIÓN: dim_pasajero
-- Responde: ¿De dónde vienen los pasajeros? ¿Cuál es el
--           perfil del viajero frecuente?
-- ============================================================
CREATE TABLE dim_pasajero (
    sk_pasajero         INT PRIMARY KEY,
    id_pasajero_origen  INT NOT NULL,
    ciudad_origen       VARCHAR(100) NOT NULL,
    departamento_origen VARCHAR(100) NOT NULL,
    nacionalidad        VARCHAR(50)  NOT NULL
);
GO

-- ============================================================
-- DIMENSIÓN: dim_unidad
-- Responde: ¿Qué tipo de servicio prefieren los pasajeros?
--           ¿Qué marca de bus genera más ventas?
-- ============================================================
CREATE TABLE dim_unidad (
    sk_unidad           INT PRIMARY KEY,
    id_unidad_origen    INT NOT NULL,
    placa               VARCHAR(20) NOT NULL,
    marca               VARCHAR(50) NOT NULL,
    modelo              VARCHAR(50) NOT NULL,
    anio_fabricacion    INT         NOT NULL,
    capacidad_asientos  INT         NOT NULL,
    tipo_servicio       VARCHAR(30) NOT NULL  -- Semi-cama/Cama/Ejecutivo/Regular
);
GO

-- ============================================================
-- DIMENSIÓN: dim_canal
-- Responde: ¿Por qué canal se vende más? ¿Vale la pena
--           invertir en el canal Web/Telefono?
-- ============================================================
CREATE TABLE dim_canal (
    sk_canal    INT PRIMARY KEY,
    canal_venta VARCHAR(30) NOT NULL,
    es_digital  BIT         NOT NULL DEFAULT 0  -- Web=1, Ventanilla/Agente=0
);
GO

-- ============================================================
-- TABLA DE HECHOS: fact_ventas
-- Granularidad: 1 fila = 1 pasaje vendido
-- ============================================================
CREATE TABLE fact_ventas (
    sk_venta        INT PRIMARY KEY,
    -- Claves foráneas a dimensiones
    sk_tiempo       INT NOT NULL FOREIGN KEY REFERENCES dim_tiempo(sk_tiempo),
    sk_ruta         INT NOT NULL FOREIGN KEY REFERENCES dim_ruta(sk_ruta),
    sk_empresa      INT NOT NULL FOREIGN KEY REFERENCES dim_empresa(sk_empresa),
    sk_pasajero     INT NOT NULL FOREIGN KEY REFERENCES dim_pasajero(sk_pasajero),
    sk_unidad       INT NOT NULL FOREIGN KEY REFERENCES dim_unidad(sk_unidad),
    sk_canal        INT NOT NULL FOREIGN KEY REFERENCES dim_canal(sk_canal),

    -- IDs originales (para trazabilidad Bronze→Gold)
    id_venta_origen INT NOT NULL,

    -- MÉTRICAS (los hechos que medimos)
    monto_pagado        DECIMAL(8,2) NOT NULL,  -- ingreso real de la venta
    tarifa_base         DECIMAL(8,2) NOT NULL,  -- precio de lista de la ruta
    descuento_aplicado  DECIMAL(5,2) NOT NULL DEFAULT 0,
    numero_asiento      INT          NOT NULL,
    capacidad_unidad    INT          NOT NULL,  -- desnormalizado para KPIs
    distancia_km        INT          NOT NULL,  -- desnormalizado para KPIs
    es_cancelado        BIT          NOT NULL DEFAULT 0,

    -- MÉTRICAS CALCULADAS (derivadas, útiles para el dashboard)
    ingreso_por_km      AS (CAST(monto_pagado AS DECIMAL(10,4)) /
                            NULLIF(distancia_km, 0)) PERSISTED,

    -- CONTEXTO CEPALSTAT (enriquecimiento externo)
    cepal_volumen_pasajeros_km  DECIMAL(18,3) NULL,
    cepal_modo_transporte       VARCHAR(50)   NULL
);
GO

-- ============================================================
-- ÍNDICES para performance OLAP (consultas del dashboard)
-- ============================================================
CREATE INDEX idx_fact_tiempo   ON fact_ventas (sk_tiempo);
CREATE INDEX idx_fact_ruta     ON fact_ventas (sk_ruta);
CREATE INDEX idx_fact_empresa  ON fact_ventas (sk_empresa);
CREATE INDEX idx_fact_canal    ON fact_ventas (sk_canal);
CREATE INDEX idx_fact_fecha    ON fact_ventas (sk_tiempo, sk_ruta);
GO

-- ============================================================
-- VERIFICACIÓN
-- ============================================================
SELECT
    t.name  AS tabla,
    p.rows  AS registros_estimados
FROM sys.tables t
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0,1)
ORDER BY t.name;
GO

PRINT 'TerminalTarijaGold creada exitosamente con Star Schema.';
GO