-- ============================================================
-- PROYECTO BI: Sistema BI - Terminal de Buses de Tarija
-- MATERIA: Inteligencia de Negocios (INV-0170) - UPDS
-- EQUIPO: Olivera Cardozo Roberto Carlos
--         Socompi Flores Franklin Ruben
--         Rueda Flores Julian Joel
-- CAPA: BRONZE - Base de Datos Transaccional de Origen
-- ============================================================

-- Crear y usar la base de datos
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TerminalTarijaDB')
    CREATE DATABASE TerminalTarijaDB;
GO

USE TerminalTarijaDB;
GO

-- ============================================================
-- LIMPIEZA DE TABLAS (para re-ejecucion segura)
-- ============================================================
IF OBJECT_ID('ventas_pasajes', 'U') IS NOT NULL DROP TABLE ventas_pasajes;
IF OBJECT_ID('itinerarios', 'U') IS NOT NULL DROP TABLE itinerarios;
IF OBJECT_ID('pasajeros', 'U') IS NOT NULL DROP TABLE pasajeros;
IF OBJECT_ID('unidades_flota', 'U') IS NOT NULL DROP TABLE unidades_flota;
IF OBJECT_ID('rutas', 'U') IS NOT NULL DROP TABLE rutas;
IF OBJECT_ID('empresas', 'U') IS NOT NULL DROP TABLE empresas;
IF OBJECT_ID('geografia', 'U') IS NOT NULL DROP TABLE geografia;
GO

-- ============================================================
-- TABLA 1: GEOGRAFIA
-- Ciudades y departamentos de Bolivia
-- ============================================================
CREATE TABLE geografia (
    id_ciudad       INT PRIMARY KEY IDENTITY(1,1),
    ciudad          VARCHAR(100) NOT NULL,
    departamento    VARCHAR(100) NOT NULL,
    es_fronteriza   BIT DEFAULT 0,
    latitud         DECIMAL(9,6),
    longitud        DECIMAL(9,6)
);
GO

INSERT INTO geografia (ciudad, departamento, es_fronteriza, latitud, longitud) VALUES
('Tarija',          'Tarija',           0, -21.535833, -64.730278),
('Santa Cruz de la Sierra', 'Santa Cruz', 0, -17.783333, -63.182222),
('La Paz',          'La Paz',           0, -16.500000, -68.150000),
('Cochabamba',      'Cochabamba',       0, -17.393889, -66.156944),
('Sucre',           'Chuquisaca',       0, -19.043611, -65.259722),
('Oruro',           'Oruro',            0, -17.983333, -67.116667),
('Potosi',          'Potosi',           0, -19.583333, -65.750000),
('Beni',            'Beni',             0, -14.833333, -64.900000),
('Trinidad',        'Beni',             0, -14.833333, -64.900000),
('Yacuiba',         'Tarija',           1, -21.983333, -63.683333),
('Bermejo',         'Tarija',           1, -22.733333, -64.333333),
('Villamontes',     'Tarija',           0, -21.250000, -63.483333),
('Entre Rios',      'Tarija',           0, -21.516667, -64.166667),
('San Lorenzo',     'Tarija',           0, -21.433333, -64.766667),
('Uriondo',         'Tarija',           0, -21.683333, -64.666667),
('Camiri',          'Santa Cruz',       0, -20.050000, -63.516667),
('Padcaya',         'Tarija',           1, -22.016667, -64.716667),
('Concepcion',      'Santa Cruz',       0, -16.150000, -62.033333),
('Cobija',          'Pando',            1, -11.033333, -68.750000),
('Villazón',        'Potosi',           1, -22.100000, -65.600000);
GO

-- ============================================================
-- TABLA 2: EMPRESAS DE TRANSPORTE
-- ============================================================
CREATE TABLE empresas (
    id_empresa          INT PRIMARY KEY IDENTITY(1,1),
    nombre_empresa      VARCHAR(150) NOT NULL,
    nit                 VARCHAR(20) UNIQUE,
    telefono            VARCHAR(20),
    estado              VARCHAR(20) DEFAULT 'Activa' CHECK (estado IN ('Activa', 'Suspendida', 'Inhabilitada')),
    tipo                VARCHAR(30) CHECK (tipo IN ('Flota', 'Trufi', 'Microbus', 'Mixta')),
    ciudad_origen_id    INT FOREIGN KEY REFERENCES geografia(id_ciudad),
    capacidad_flota_total INT,
    fecha_registro      DATE
);
GO

INSERT INTO empresas (nombre_empresa, nit, telefono, estado, tipo, ciudad_origen_id, capacidad_flota_total, fecha_registro) VALUES
('Osastur Tarija',          '1023456789', '046643210', 'Activa',      'Flota',    1, 180, '2010-03-15'),
('Trans Bolivar',           '2034567890', '046641234', 'Activa',      'Flota',    1, 220, '2008-07-20'),
('Flota Tarija SCZ',        '3045678901', '046645678', 'Activa',      'Flota',    1, 200, '2012-01-10'),
('Cooperativa Bermejo',     '4056789012', '046642222', 'Activa',      'Trufi',    1,  90, '2015-06-05'),
('Trans Chaco Sur',         '5067890123', '046643333', 'Activa',      'Flota',    1, 160, '2009-11-30'),
('Microbus Yacuiba Express','6078901234', '046644444', 'Activa',      'Microbus', 10, 75, '2016-02-14'),
('Flota Norte Integrado',   '7089012345', '022334455', 'Activa',      'Flota',    2, 240, '2007-08-22'),
('Trans Andino',            '8090123456', '044556677', 'Activa',      'Flota',    3, 210, '2011-04-18'),
('Cooperativa Frontera',    '9001234567', '046646666', 'Suspendida',  'Trufi',    11, 60, '2018-09-01'),
('Expreso del Sur',         '1112345678', '046647777', 'Activa',      'Flota',    1, 195, '2013-05-27'),
('Trans Chuquisaca',        '2223456789', '046418888', 'Activa',      'Flota',    5, 170, '2014-12-03'),
('Flota Oruro Tarija',      '3334567890', '025219999', 'Activa',      'Flota',    6, 155, '2010-10-10'),
('Minibus Valle Central',   '4445678901', '046641111', 'Activa',      'Microbus', 1,  50, '2019-03-22'),
('Trans Villazón',          '5556789012', '046622222', 'Inhabilitada','Flota',    20, 130, '2006-07-15'),
('Servicio Rapido Tarijeño','6667890123', '046633333', 'Activa',      'Mixta',    1, 110, '2017-11-08');
GO

-- ============================================================
-- TABLA 3: RUTAS
-- ============================================================
CREATE TABLE rutas (
    id_ruta         INT PRIMARY KEY IDENTITY(1,1),
    ciudad_origen_id  INT FOREIGN KEY REFERENCES geografia(id_ciudad),
    ciudad_destino_id INT FOREIGN KEY REFERENCES geografia(id_ciudad),
    distancia_km    INT,
    duracion_horas  DECIMAL(4,1),
    tipo_corredor   VARCHAR(30) CHECK (tipo_corredor IN ('Interdepartamental', 'Interprovincial', 'Fronterizo')),
    tarifa_base     DECIMAL(8,2),
    activa          BIT DEFAULT 1
);
GO

INSERT INTO rutas (ciudad_origen_id, ciudad_destino_id, distancia_km, duracion_horas, tipo_corredor, tarifa_base, activa) VALUES
(1,  2,  680, 12.0, 'Interdepartamental', 120.00, 1), -- Tarija - Santa Cruz
(1,  3,  740, 14.0, 'Interdepartamental', 140.00, 1), -- Tarija - La Paz
(1,  4,  610, 11.5, 'Interdepartamental', 110.00, 1), -- Tarija - Cochabamba
(1,  5,  320,  6.0, 'Interdepartamental',  70.00, 1), -- Tarija - Sucre
(1,  6,  850, 15.0, 'Interdepartamental', 150.00, 1), -- Tarija - Oruro
(1,  7,  480,  9.0, 'Interdepartamental',  90.00, 1), -- Tarija - Potosi
(1, 10,  180,  3.5, 'Interprovincial',     40.00, 1), -- Tarija - Yacuiba
(1, 11,  150,  3.0, 'Fronterizo',          35.00, 1), -- Tarija - Bermejo
(1, 12,  200,  4.0, 'Interprovincial',     45.00, 1), -- Tarija - Villamontes
(1, 13,   90,  2.0, 'Interprovincial',     25.00, 1), -- Tarija - Entre Rios
(1, 14,   25,  0.5, 'Interprovincial',     10.00, 1), -- Tarija - San Lorenzo
(1, 15,   35,  0.7, 'Interprovincial',     12.00, 1), -- Tarija - Uriondo
(1, 20,  380,  7.5, 'Fronterizo',          80.00, 1), -- Tarija - Villazón
(2,  3,  900, 16.0, 'Interdepartamental', 160.00, 1), -- Santa Cruz - La Paz
(5,  7,  160,  3.5, 'Interdepartamental',  45.00, 1); -- Sucre - Potosi
GO

-- ============================================================
-- TABLA 4: PASAJEROS
-- ============================================================
CREATE TABLE pasajeros (
    id_pasajero     INT PRIMARY KEY IDENTITY(1,1),
    nombre          VARCHAR(100) NOT NULL,
    apellido        VARCHAR(100) NOT NULL,
    ci              VARCHAR(20) UNIQUE,
    telefono        VARCHAR(20),
    email           VARCHAR(120),
    ciudad_id       INT FOREIGN KEY REFERENCES geografia(id_ciudad),
    nacionalidad    VARCHAR(50) DEFAULT 'Boliviana',
    fecha_nacimiento DATE
);
GO

-- Insertar 20 pasajeros base con nombres bolivianos reales
INSERT INTO pasajeros (nombre, apellido, ci, telefono, email, ciudad_id, nacionalidad, fecha_nacimiento) VALUES
('Juan Carlos',     'Mamani Quispe',     '7123456', '77123456', 'jmamani@gmail.com',    1, 'Boliviana', '1990-05-12'),
('Maria Elena',     'Flores Tarqui',     '6234567', '66234567', 'mflores@hotmail.com',  1, 'Boliviana', '1985-08-23'),
('Pedro Antonio',   'Gutierrez Vaca',    '5345678', '55345678', 'pgutierrez@gmail.com', 2, 'Boliviana', '1992-11-30'),
('Ana Luisa',       'Condori Huanca',    '4456789', '44456789', NULL,                   1, 'Boliviana', '1988-03-17'),
('Carlos Alberto',  'Vargas Torrez',     '3567890', '33567890', 'cvargas@gmail.com',    3, 'Boliviana', '1995-07-04'),
('Rosa Angelica',   'Choque Limachi',    '2678901', '22678901', NULL,                   1, 'Boliviana', '1982-12-09'),
('Miguel Angel',    'Quispe Mamani',     '1789012', '11789012', 'mquispe@yahoo.com',    4, 'Boliviana', '1998-02-28'),
('Sandra Patricia', 'Torrez Mendoza',    '8890123', '88890123', 'storrez@gmail.com',    1, 'Boliviana', '1991-09-15'),
('Luis Fernando',   'Mendoza Arce',      '9901234', '99901234', NULL,                   5, 'Boliviana', '1987-06-21'),
('Gloria Beatriz',  'Arce Villanueva',   '1012345', '10123456', 'garce@gmail.com',      1, 'Boliviana', '1993-04-03'),
('Roberto Carlos',  'Villanueva Ramos',  '1123456', '11234567', NULL,                   2, 'Boliviana', '1996-10-18'),
('Patricia Isabel', 'Ramos Aguilar',     '1234560', '12345678', 'pramos@hotmail.com',   1, 'Boliviana', '1984-01-25'),
('Fernando Jose',   'Aguilar Salinas',   '1345671', '13456789', NULL,                   6, 'Boliviana', '1990-07-30'),
('Claudia Veronica','Salinas Ibañez',    '1456782', '14567890', 'csalinas@gmail.com',   1, 'Boliviana', '1997-11-14'),
('Jorge Eduardo',   'Ibañez Castro',     '1567893', '15678901', NULL,                   7, 'Boliviana', '1983-05-07'),
('Monica Cecilia',  'Castro Ponce',      '1678904', '16789012', 'mcastro@gmail.com',    1, 'Boliviana', '1994-08-19'),
('Ricardo Ernesto', 'Ponce Montaño',     '1789015', '17890123', NULL,                   1, 'Argentina', '1989-03-22'),
('Silvia Rosario',  'Montaño Perez',     '1890126', '18901234', 'smontano@yahoo.com',   10,'Boliviana', '1986-12-05'),
('Daniel Alejandro','Perez Oporto',      '1901237', '19012345', NULL,                   11,'Boliviana', '1999-06-16'),
('Carmen Rosa',     'Oporto Flores',     '2012348', '20123456', 'coporto@gmail.com',    1, 'Boliviana', '1981-09-28');
GO

-- ============================================================
-- TABLA 5: UNIDADES DE FLOTA
-- ============================================================
CREATE TABLE unidades_flota (
    id_unidad       INT PRIMARY KEY IDENTITY(1,1),
    id_empresa      INT FOREIGN KEY REFERENCES empresas(id_empresa),
    placa           VARCHAR(20) UNIQUE NOT NULL,
    marca           VARCHAR(50),
    modelo          VARCHAR(50),
    anio_fabricacion INT,
    capacidad_asientos INT,
    tipo_servicio   VARCHAR(30) CHECK (tipo_servicio IN ('Semi-cama', 'Cama', 'Ejecutivo', 'Regular')),
    estado_unidad   VARCHAR(20) DEFAULT 'Operativa' CHECK (estado_unidad IN ('Operativa', 'Mantenimiento', 'Baja'))
);
GO

INSERT INTO unidades_flota (id_empresa, placa, marca, modelo, anio_fabricacion, capacidad_asientos, tipo_servicio, estado_unidad) VALUES
(1,  'TAR-001', 'Mercedes Benz', 'OF-1721', 2018, 44, 'Semi-cama',  'Operativa'),
(1,  'TAR-002', 'Volvo',         'B380',    2020, 42, 'Cama',       'Operativa'),
(1,  'TAR-003', 'Scania',        'K410',    2017, 46, 'Semi-cama',  'Operativa'),
(2,  'TAR-011', 'Mercedes Benz', 'OF-1318', 2015, 44, 'Regular',    'Operativa'),
(2,  'TAR-012', 'Volvo',         'B320',    2019, 42, 'Semi-cama',  'Operativa'),
(3,  'TAR-021', 'Scania',        'K360',    2016, 46, 'Cama',       'Operativa'),
(3,  'TAR-022', 'Mercedes Benz', 'OF-2041', 2021, 44, 'Ejecutivo',  'Operativa'),
(4,  'TAR-031', 'Toyota',        'Coaster', 2018, 22, 'Regular',    'Operativa'),
(4,  'TAR-032', 'Toyota',        'Coaster', 2019, 22, 'Regular',    'Operativa'),
(5,  'TAR-041', 'Volvo',         'B380',    2017, 44, 'Semi-cama',  'Operativa'),
(5,  'TAR-042', 'Scania',        'K410',    2020, 46, 'Cama',       'Operativa'),
(6,  'YAC-001', 'Hyundai',       'County',  2016, 26, 'Regular',    'Operativa'),
(7,  'SCZ-001', 'Mercedes Benz', 'OF-1721', 2019, 44, 'Cama',       'Operativa'),
(8,  'LPZ-001', 'Volvo',         'B420',    2020, 42, 'Ejecutivo',  'Operativa'),
(10, 'TAR-051', 'Mercedes Benz', 'OF-1318', 2014, 44, 'Semi-cama',  'Mantenimiento'),
(10, 'TAR-052', 'Scania',        'K310',    2018, 46, 'Regular',    'Operativa'),
(11, 'SUC-001', 'Volvo',         'B320',    2017, 42, 'Semi-cama',  'Operativa'),
(12, 'ORU-001', 'Mercedes Benz', 'OF-2041', 2016, 44, 'Regular',    'Operativa'),
(13, 'TAR-061', 'Toyota',        'Hiace',   2020, 15, 'Regular',    'Operativa'),
(15, 'TAR-071', 'Mercedes Benz', 'OF-1318', 2015, 44, 'Semi-cama',  'Operativa');
GO

-- ============================================================
-- TABLA 6: ITINERARIOS
-- ============================================================
CREATE TABLE itinerarios (
    id_itinerario   INT PRIMARY KEY IDENTITY(1,1),
    id_ruta         INT FOREIGN KEY REFERENCES rutas(id_ruta),
    id_empresa      INT FOREIGN KEY REFERENCES empresas(id_empresa),
    id_unidad       INT FOREIGN KEY REFERENCES unidades_flota(id_unidad),
    hora_salida     TIME,
    dias_operacion  VARCHAR(50), -- 'Lunes,Miercoles,Viernes' o 'Diario'
    frecuencia      VARCHAR(30) DEFAULT 'Diario',
    activo          BIT DEFAULT 1
);
GO

INSERT INTO itinerarios (id_ruta, id_empresa, id_unidad, hora_salida, dias_operacion, frecuencia) VALUES
(1,  1,  1,  '08:00', 'Diario',                     'Diario'),
(1,  1,  2,  '20:00', 'Diario',                     'Diario'),
(1,  3,  6,  '18:30', 'Diario',                     'Diario'),
(1,  5,  10, '07:00', 'Lunes,Miercoles,Viernes',     'Interdiario'),
(2,  2,  4,  '19:00', 'Diario',                     'Diario'),
(2,  8,  14, '20:30', 'Diario',                     'Diario'),
(3,  3,  7,  '21:00', 'Diario',                     'Diario'),
(4,  11, 17, '09:00', 'Diario',                     'Diario'),
(5,  12, 18, '18:00', 'Lunes,Jueves',               'Semanal'),
(6,  10, 16, '10:00', 'Diario',                     'Diario'),
(7,  4,  8,  '06:00', 'Diario',                     'Diario'),
(7,  6,  12, '07:30', 'Diario',                     'Diario'),
(8,  4,  9,  '07:00', 'Diario',                     'Diario'),
(10, 13, 19, '06:30', 'Diario',                     'Diario'),
(11, 13, 19, '08:00', 'Diario',                     'Diario');
GO

-- ============================================================
-- TABLA 7 (TRANSACCIONAL): VENTAS_PASAJES
-- Tabla principal con 1,000+ registros usando bucle WHILE
-- ============================================================
CREATE TABLE ventas_pasajes (
    id_venta            INT PRIMARY KEY IDENTITY(1,1),
    id_itinerario       INT FOREIGN KEY REFERENCES itinerarios(id_itinerario),
    id_pasajero         INT FOREIGN KEY REFERENCES pasajeros(id_pasajero),
    id_ruta             INT FOREIGN KEY REFERENCES rutas(id_ruta),
    id_empresa          INT FOREIGN KEY REFERENCES empresas(id_empresa),
    fecha_venta         DATETIME,
    fecha_viaje         DATE,
    numero_asiento      INT,
    monto_pagado        DECIMAL(8,2),
    canal_venta         VARCHAR(30) CHECK (canal_venta IN ('Ventanilla', 'Telefono', 'Web', 'Agente')),
    estado_pasaje       VARCHAR(20) DEFAULT 'Confirmado' CHECK (estado_pasaje IN ('Confirmado', 'Cancelado', 'Pendiente', 'Viajado')),
    descuento_aplicado  DECIMAL(5,2) DEFAULT 0,
    -- Columnas con datos sucios intencionales (5%) para Silver
    nombre_pasajero_registrado VARCHAR(120), -- a veces NULL, espacios extras, mayúsculas mezcladas
    observaciones       VARCHAR(200)         -- a veces NULL
);
GO

-- ============================================================
-- GENERACION MASIVA DE DATOS: Bucle WHILE - 1,200 registros
-- ============================================================
DECLARE @i INT = 1;
DECLARE @total INT = 1200;

DECLARE @id_itinerario  INT;
DECLARE @id_pasajero    INT;
DECLARE @id_ruta        INT;
DECLARE @id_empresa     INT;
DECLARE @fecha_venta    DATETIME;
DECLARE @fecha_viaje    DATE;
DECLARE @numero_asiento INT;
DECLARE @tarifa_base    DECIMAL(8,2);
DECLARE @monto         DECIMAL(8,2);
DECLARE @canal          VARCHAR(30);
DECLARE @estado         VARCHAR(20);
DECLARE @descuento      DECIMAL(5,2);
DECLARE @nombre_reg     VARCHAR(120);
DECLARE @obs            VARCHAR(200);
DECLARE @dias_anticip   INT;

-- Tablas auxiliares de valores para variación aleatoria
DECLARE @canales TABLE (id INT, canal VARCHAR(30));
INSERT INTO @canales VALUES (1,'Ventanilla'),(2,'Telefono'),(3,'Web'),(4,'Agente');

DECLARE @estados TABLE (id INT, estado VARCHAR(20), peso INT);
INSERT INTO @estados VALUES
    (1,'Viajado',    65),
    (2,'Confirmado', 20),
    (3,'Cancelado',  10),
    (4,'Pendiente',   5);

WHILE @i <= @total
BEGIN
    -- Seleccionar itinerario aleatorio (1 a 15)
    SET @id_itinerario = (ABS(CHECKSUM(NEWID())) % 15) + 1;

    -- Obtener ruta y empresa desde el itinerario
    SELECT @id_ruta    = id_ruta,
           @id_empresa = id_empresa
    FROM itinerarios
    WHERE id_itinerario = @id_itinerario;

    -- Pasajero aleatorio (1 a 20)
    SET @id_pasajero = (ABS(CHECKSUM(NEWID())) % 20) + 1;

    -- Fecha de viaje: últimos 3 años (2022-2025)
    SET @dias_anticip = ABS(CHECKSUM(NEWID())) % 1095; -- 0 a 1095 días atrás
    SET @fecha_viaje  = DATEADD(DAY, -@dias_anticip, CAST('2025-12-31' AS DATE));

    -- Fecha de venta: entre 1 y 30 días antes del viaje
    SET @fecha_venta  = DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % 30 + 1), @fecha_viaje);

    -- Número de asiento
    SET @numero_asiento = (ABS(CHECKSUM(NEWID())) % 46) + 1;

    -- Tarifa base desde rutas
    SELECT @tarifa_base = tarifa_base FROM rutas WHERE id_ruta = @id_ruta;

    -- Descuento: 70% sin descuento, 20% descuento 10%, 10% descuento 20%
    SET @descuento = CASE
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 7 THEN 0.00
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 9 THEN 10.00
        ELSE 20.00
    END;

    -- Monto pagado con variación ±15% sobre tarifa base menos descuento
    SET @monto = @tarifa_base * (1 + (CAST(ABS(CHECKSUM(NEWID())) % 30 AS DECIMAL) - 15) / 100.0)
                 * (1 - @descuento / 100.0);
    SET @monto = ROUND(@monto, 2);

    -- Canal de venta aleatorio
    SET @canal = (SELECT TOP 1 canal FROM @canales ORDER BY NEWID());

    -- Estado según temporalidad (si el viaje ya pasó, marcar como Viajado)
    IF @fecha_viaje < CAST(GETDATE() AS DATE)
        SET @estado = CASE
            WHEN ABS(CHECKSUM(NEWID())) % 10 < 8 THEN 'Viajado'
            WHEN ABS(CHECKSUM(NEWID())) % 10 < 9 THEN 'Cancelado'
            ELSE 'Confirmado'
        END
    ELSE
        SET @estado = CASE
            WHEN ABS(CHECKSUM(NEWID())) % 10 < 7 THEN 'Confirmado'
            WHEN ABS(CHECKSUM(NEWID())) % 10 < 9 THEN 'Pendiente'
            ELSE 'Cancelado'
        END;

    -- *** DATOS SUCIOS intencionales: ~5% de registros (cada 20 aprox.) ***
    IF (@i % 20 = 0) -- NULL en nombre registrado
        SET @nombre_reg = NULL;
    ELSE IF (@i % 19 = 0) -- Nombre con espacios extra (dato sucio)
        SET @nombre_reg = '  JUAN   carlos  ';
    ELSE IF (@i % 23 = 0) -- Mayúsculas mezcladas inconsistentes
        SET @nombre_reg = 'mARIA eLENA fLoReS';
    ELSE
    BEGIN
        SELECT @nombre_reg = UPPER(nombre) + ' ' + UPPER(apellido)
        FROM pasajeros WHERE id_pasajero = @id_pasajero;
    END;

    -- Observaciones: NULL el 30% del tiempo
    SET @obs = CASE
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 3 THEN NULL
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 5 THEN 'Pasaje con equipaje extra'
        WHEN ABS(CHECKSUM(NEWID())) % 10 < 7 THEN 'Pasajero adulto mayor'
        ELSE 'Sin observaciones'
    END;

    -- Insertar registro
    INSERT INTO ventas_pasajes (
        id_itinerario, id_pasajero, id_ruta, id_empresa,
        fecha_venta, fecha_viaje, numero_asiento,
        monto_pagado, canal_venta, estado_pasaje,
        descuento_aplicado, nombre_pasajero_registrado, observaciones
    ) VALUES (
        @id_itinerario, @id_pasajero, @id_ruta, @id_empresa,
        @fecha_venta, @fecha_viaje, @numero_asiento,
        @monto, @canal, @estado,
        @descuento, @nombre_reg, @obs
    );

    SET @i = @i + 1;
END;
GO

-- ============================================================
-- VERIFICACION FINAL
-- ============================================================
PRINT '========================================';
PRINT 'BASE DE DATOS db_terminal_origen CREADA';
PRINT '========================================';

SELECT 'geografia'      AS tabla, COUNT(*) AS registros FROM geografia
UNION ALL
SELECT 'empresas',       COUNT(*) FROM empresas
UNION ALL
SELECT 'rutas',          COUNT(*) FROM rutas
UNION ALL
SELECT 'pasajeros',      COUNT(*) FROM pasajeros
UNION ALL
SELECT 'unidades_flota', COUNT(*) FROM unidades_flota
UNION ALL
SELECT 'itinerarios',    COUNT(*) FROM itinerarios
UNION ALL
SELECT 'ventas_pasajes', COUNT(*) FROM ventas_pasajes;
GO

-- Vista previa de ventas con datos sucios para verificar el 5%
PRINT '';
PRINT 'Registros con datos sucios (nombre NULL o malformado):';
SELECT COUNT(*) AS registros_sucios
FROM ventas_pasajes
WHERE nombre_pasajero_registrado IS NULL
   OR nombre_pasajero_registrado LIKE '%  %'   -- doble espacio
   OR nombre_pasajero_registrado != LTRIM(RTRIM(nombre_pasajero_registrado));
GO

PRINT 'Script ejecutado exitosamente. Listo para la Capa Bronze.';
GO