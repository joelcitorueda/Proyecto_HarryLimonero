"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: GOLD - Carga del Star Schema
ARCHIVO: gold/load_gold.py
DESCRIPCIÓN: Lee los CSVs Silver, los transforma al modelo
             estrella y los carga en TerminalTarijaGold
             Uso: python gold/load_gold.py
============================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import glob
import os
from datetime import datetime, date

SILVER_DIR = os.path.join("data", "silver")

# Cadena de conexión SQLAlchemy para SQL Server con Windows Auth
ENGINE_GOLD = (
    "mssql+pyodbc://localhost/TerminalTarijaGold"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Trusted_Connection=yes"
    "&TrustServerCertificate=yes"
)

TIMESTAMP = datetime.now().strftime("%Y%m%d")


# ──────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────

def separador(titulo):
    print(f"\n{'─'*55}")
    print(f"  {titulo}")
    print(f"{'─'*55}")


def cargar_silver(patron):
    archivos = glob.glob(os.path.join(SILVER_DIR, patron))
    if not archivos:
        raise FileNotFoundError(f"No se encontró '{patron}' en {SILVER_DIR}")
    archivo = sorted(archivos)[-1]
    df = pd.read_csv(archivo, encoding="utf-8-sig")
    print(f"  [OK] {os.path.basename(archivo)} → {len(df)} registros")
    return df


def insertar_tabla(engine, df, nombre_tabla, if_exists="append"):
    """Inserta un DataFrame en SQL Server usando SQLAlchemy."""
    try:
        df.to_sql(
            nombre_tabla,
            engine,
            if_exists=if_exists,
            index=False,
            chunksize=500,
        )
        print(f"  [OK] {nombre_tabla}: {len(df)} filas insertadas")
    except Exception as e:
        print(f"  [ERROR] {nombre_tabla}: {e}")
        raise


# ──────────────────────────────────────────────
# CONSTRUCCIÓN DE DIMENSIONES
# ──────────────────────────────────────────────

def build_dim_tiempo(df_ventas):
    """Construye dim_tiempo a partir de todas las fechas únicas de ventas."""
    separador("Construyendo dim_tiempo")

    fechas_venta  = pd.to_datetime(df_ventas["fecha_venta"],  errors="coerce")
    fechas_viaje  = pd.to_datetime(df_ventas["fecha_viaje"],  errors="coerce")
    todas_fechas  = pd.concat([fechas_venta, fechas_viaje]).dropna().unique()
    todas_fechas  = pd.to_datetime(sorted(todas_fechas))

    # Meses con nombre en español
    meses_es = {
        1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
        5:"Mayo",  6:"Junio",   7:"Julio", 8:"Agosto",
        9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"
    }
    dias_es = {
        0:"Lunes", 1:"Martes", 2:"Miercoles", 3:"Jueves",
        4:"Viernes", 5:"Sabado", 6:"Domingo"
    }

    filas = []
    for fecha in todas_fechas:
        mes  = fecha.month
        anio = fecha.year
        dia  = fecha.weekday()

        # Temporada alta/media/baja para Bolivia
        # Alta: Semana Santa (abr), Carnaval (feb), vacaciones (jul), Navidad (dic)
        # Media: marzo, mayo, agosto, noviembre
        # Baja: resto
        if mes in [2, 4, 7, 12]:
            temporada = "Alta"
        elif mes in [3, 5, 8, 11]:
            temporada = "Media"
        else:
            temporada = "Baja"

        # Semana Santa: semana 14-15 de abril aproximadamente
        semana = fecha.isocalendar()[1]
        es_semana_santa = 1 if (mes == 4 and semana in [14, 15, 16]) else 0

        # Feria de Tarija: 1ra semana de abril
        es_feria_tarija = 1 if (mes == 4 and fecha.day <= 7) else 0

        # Fin de año: 24-31 diciembre y 1 enero
        es_fin_anio = 1 if (mes == 12 and fecha.day >= 24) or \
                          (mes == 1 and fecha.day == 1) else 0

        filas.append({
            "sk_tiempo":       int(fecha.strftime("%Y%m%d")),
            "fecha":           fecha.date(),
            "anio":            anio,
            "trimestre":       (mes - 1) // 3 + 1,
            "mes":             mes,
            "nombre_mes":      meses_es[mes],
            "semana_anio":     semana,
            "dia_semana":      dia + 1,
            "nombre_dia":      dias_es[dia],
            "es_fin_semana":   1 if dia >= 5 else 0,
            "temporada":       temporada,
            "es_semana_santa": es_semana_santa,
            "es_feria_tarija": es_feria_tarija,
            "es_fin_anio":     es_fin_anio,
        })

    df_dim = pd.DataFrame(filas).drop_duplicates(subset=["sk_tiempo"])
    print(f"  Fechas únicas procesadas: {len(df_dim)}")
    return df_dim


def build_dim_ruta(df_rutas, df_geo):
    """Construye dim_ruta enriquecida con nombres de ciudades."""
    separador("Construyendo dim_ruta")

    geo = df_geo[["id_ciudad", "ciudad", "departamento"]].copy()

    df = df_rutas.merge(
        geo.rename(columns={"id_ciudad": "ciudad_origen_id",
                            "ciudad": "ciudad_origen",
                            "departamento": "departamento_origen"}),
        on="ciudad_origen_id", how="left"
    ).merge(
        geo.rename(columns={"id_ciudad": "ciudad_destino_id",
                            "ciudad": "ciudad_destino",
                            "departamento": "departamento_destino"}),
        on="ciudad_destino_id", how="left"
    )

    dim = pd.DataFrame({
        "id_ruta_origen":       df["id_ruta"],
        "ciudad_origen":        df["ciudad_origen"].fillna("Desconocido"),
        "departamento_origen":  df["departamento_origen"].fillna("Desconocido"),
        "ciudad_destino":       df["ciudad_destino"].fillna("Desconocido"),
        "departamento_destino": df["departamento_destino"].fillna("Desconocido"),
        "distancia_km":         df["distancia_km"],
        "duracion_horas":       df["duracion_horas"],
        "tipo_corredor":        df["tipo_corredor"],
        "tarifa_base":          df["tarifa_base"],
    })

    # Agregar surrogate key
    dim = dim.reset_index(drop=True)
    dim.insert(0, "sk_ruta", range(1, len(dim) + 1))

    print(f"  Rutas procesadas: {len(dim)}")
    return dim


def build_dim_empresa(df_empresas):
    separador("Construyendo dim_empresa")

    dim = pd.DataFrame({
        "id_empresa_origen":    df_empresas["id_empresa"],
        "nombre_empresa":       df_empresas["nombre_empresa"],
        "tipo_empresa":         df_empresas["tipo"],
        "estado_empresa":       df_empresas["estado"],
        "capacidad_flota_total":df_empresas["capacidad_flota_total"].fillna(0).astype(int),
    })
    dim.insert(0, "sk_empresa", range(1, len(dim) + 1))

    print(f"  Empresas procesadas: {len(dim)}")
    return dim


def build_dim_pasajero(df_pasajeros, df_geo):
    separador("Construyendo dim_pasajero")

    geo = df_geo[["id_ciudad", "ciudad", "departamento"]].copy()

    df = df_pasajeros.merge(
        geo.rename(columns={"id_ciudad": "ciudad_id",
                            "ciudad": "ciudad_origen",
                            "departamento": "departamento_origen"}),
        on="ciudad_id", how="left"
    )

    dim = pd.DataFrame({
        "id_pasajero_origen":   df["id_pasajero"],
        "ciudad_origen":        df["ciudad_origen"].fillna("Desconocido"),
        "departamento_origen":  df["departamento_origen"].fillna("Desconocido"),
        "nacionalidad":         df["nacionalidad"].fillna("Boliviana"),
    })
    dim.insert(0, "sk_pasajero", range(1, len(dim) + 1))

    print(f"  Pasajeros procesados: {len(dim)}")
    return dim


def build_dim_unidad(df_unidades):
    separador("Construyendo dim_unidad")

    dim = pd.DataFrame({
        "id_unidad_origen":  df_unidades["id_unidad"],
        "placa":             df_unidades["placa"],
        "marca":             df_unidades["marca"],
        "modelo":            df_unidades["modelo"],
        "anio_fabricacion":  df_unidades["anio_fabricacion"],
        "capacidad_asientos":df_unidades["capacidad_asientos"],
        "tipo_servicio":     df_unidades["tipo_servicio"],
    })
    dim.insert(0, "sk_unidad", range(1, len(dim) + 1))

    print(f"  Unidades procesadas: {len(dim)}")
    return dim


# ──────────────────────────────────────────────
# CONSTRUCCIÓN DE FACT TABLE
# ──────────────────────────────────────────────

def build_fact_ventas(df_integrado, dim_tiempo, dim_ruta,
                      dim_empresa, dim_pasajero, dim_unidad):
    separador("Construyendo fact_ventas")

    df = df_integrado.copy()

    # Mapas de IDs originales → surrogate keys
    map_tiempo  = dict(zip(
        dim_tiempo["sk_tiempo"].astype(str),
        dim_tiempo["sk_tiempo"]
    ))
    map_ruta    = dict(zip(dim_ruta["id_ruta_origen"],    dim_ruta["sk_ruta"]))
    map_empresa = dict(zip(dim_empresa["id_empresa_origen"], dim_empresa["sk_empresa"]))
    map_pasajero= dict(zip(dim_pasajero["id_pasajero_origen"], dim_pasajero["sk_pasajero"]))
    map_unidad  = dict(zip(dim_unidad["id_unidad_origen"],  dim_unidad["sk_unidad"]))

    # Canal → sk_canal (1=Ventanilla, 2=Telefono, 3=Web, 4=Agente)
    map_canal = {
        "Ventanilla": 1,
        "Telefono":   2,
        "Web":        3,
        "Agente":     4,
    }

    # Convertir fecha_viaje a sk_tiempo (YYYYMMDD int)
    df["sk_tiempo"] = pd.to_datetime(
        df["fecha_viaje"], errors="coerce"
    ).dt.strftime("%Y%m%d").map(map_tiempo)

    # Resolver surrogate keys
    df["sk_ruta"]     = df["id_ruta"].map(map_ruta)
    df["sk_empresa"]  = df["id_empresa"].map(map_empresa)
    df["sk_pasajero"] = df["id_pasajero"].map(map_pasajero)
    df["sk_canal"]    = df["canal_venta"].str.capitalize().map(map_canal)

    # Para unidad: viene desde itinerario, usamos id_empresa como proxy
    # Si no hay id_unidad directo, asignamos la primera unidad de esa empresa
    df["sk_unidad"] = 1  # valor por defecto
    if "id_unidad" in df.columns:
        df["sk_unidad"] = df["id_unidad"].map(map_unidad).fillna(1).astype(int)

    # Métricas desnormalizadas (para evitar JOINs costosos en el dashboard)
    ruta_dist = dict(zip(dim_ruta["sk_ruta"], dim_ruta["distancia_km"]))
    ruta_tarifa = dict(zip(dim_ruta["sk_ruta"], dim_ruta["tarifa_base"]))
    unidad_cap = dict(zip(dim_unidad["sk_unidad"], dim_unidad["capacidad_asientos"]))

    df["distancia_km"]   = df["sk_ruta"].map(ruta_dist).fillna(0).astype(int)
    df["tarifa_base"]    = df["sk_ruta"].map(ruta_tarifa).fillna(0)
    df["capacidad_unidad"] = df["sk_unidad"].map(unidad_cap).fillna(44).astype(int)
    df["es_cancelado"]   = (df["estado_pasaje"].str.capitalize() == "Cancelado").astype(int)

    # Seleccionar columnas finales para fact
    fact = pd.DataFrame({
        "sk_venta": range(1, len(df) + 1),
        "sk_tiempo":                df["sk_tiempo"].fillna(20220101).astype(int),
        "sk_ruta":                  df["sk_ruta"].fillna(1).astype(int),
        "sk_empresa":               df["sk_empresa"].fillna(1).astype(int),
        "sk_pasajero":              df["sk_pasajero"].fillna(1).astype(int),
        "sk_unidad":                df["sk_unidad"],
        "sk_canal":                 df["sk_canal"].fillna(1).astype(int),
        "id_venta_origen":          df["id_venta"],
        "monto_pagado":             df["monto_pagado"],
        "tarifa_base":              df["tarifa_base"],
        "descuento_aplicado":       df["descuento_aplicado"].fillna(0),
        "numero_asiento":           df["numero_asiento"].fillna(1).astype(int),
        "capacidad_unidad":         df["capacidad_unidad"],
        "distancia_km":             df["distancia_km"],
        "es_cancelado":             df["es_cancelado"],
        "cepal_volumen_pasajeros_km": df.get("cepal_volumen_pasajeros_km", 0).fillna(0),
        "cepal_modo_transporte":    df.get("cepal_modo_transporte", "Sin dato").fillna("Sin dato"),
    })

    print(f"  Registros en fact_ventas: {len(fact)}")
    print(f"  Nulos en sk_tiempo  : {fact['sk_tiempo'].isnull().sum()}")
    print(f"  Nulos en sk_ruta    : {fact['sk_ruta'].isnull().sum()}")
    print(f"  Nulos en sk_empresa : {fact['sk_empresa'].isnull().sum()}")
    return fact


# ──────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ──────────────────────────────────────────────

def run_load_gold():
    inicio = datetime.now()

    print("\n" + "═" * 55)
    print("  PIPELINE GOLD — CARGA STAR SCHEMA")
    print(f"  Base de datos: TerminalTarijaGold")
    print(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 55)

    # ── CARGAR SILVER ────────────────────────────────────────
    separador("Cargando datos Silver")
    df_geo       = cargar_silver("geografia_silver_*.csv")
    df_emp       = cargar_silver("empresas_silver_*.csv")
    df_rutas     = cargar_silver("rutas_silver_*.csv")
    df_pasajeros = cargar_silver("pasajeros_silver_*.csv")
    df_unidades  = cargar_silver("unidades_flota_silver_*.csv")
    df_ventas    = cargar_silver("ventas_pasajes_silver_*.csv")
    df_integrado = cargar_silver("dataset_integrado_silver_*.csv")

    # ── CONSTRUIR DIMENSIONES ────────────────────────────────
    dim_tiempo   = build_dim_tiempo(df_ventas)
    dim_ruta     = build_dim_ruta(df_rutas, df_geo)
    dim_empresa  = build_dim_empresa(df_emp)
    dim_pasajero = build_dim_pasajero(df_pasajeros, df_geo)
    dim_unidad   = build_dim_unidad(df_unidades)

    # ── CONSTRUIR FACT ───────────────────────────────────────
    fact_ventas = build_fact_ventas(
        df_integrado, dim_tiempo, dim_ruta,
        dim_empresa, dim_pasajero, dim_unidad
    )

    # ── CONECTAR A SQL SERVER Y CARGAR ───────────────────────
    separador("Conectando a TerminalTarijaGold")
    try:
        engine = create_engine(ENGINE_GOLD, fast_executemany=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  [OK] Conexión exitosa a TerminalTarijaGold")
    except Exception as e:
        print(f"  [ERROR] No se pudo conectar: {e}")
        raise

    separador("Insertando dimensiones")

    # Limpiar tablas antes de insertar (orden por FK)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM fact_ventas"))
        conn.execute(text("DELETE FROM dim_tiempo"))
        conn.execute(text("DELETE FROM dim_ruta"))
        conn.execute(text("DELETE FROM dim_empresa"))
        conn.execute(text("DELETE FROM dim_pasajero"))
        conn.execute(text("DELETE FROM dim_unidad"))
        conn.execute(text("DELETE FROM dim_canal"))
        conn.commit()
        print("  [OK] Tablas limpiadas")

    # Reinsertar dim_canal
    df_canal = pd.DataFrame({
        "sk_canal":    [1, 2, 3, 4],
        "canal_venta": ["Ventanilla", "Telefono", "Web", "Agente"],
        "es_digital":  [0, 1, 1, 0]
    })

    insertar_tabla(engine, dim_tiempo,   "dim_tiempo",   if_exists="append")
    insertar_tabla(engine, dim_ruta,     "dim_ruta",     if_exists="append")
    insertar_tabla(engine, dim_empresa,  "dim_empresa",  if_exists="append")
    insertar_tabla(engine, dim_pasajero, "dim_pasajero", if_exists="append")
    insertar_tabla(engine, dim_unidad,   "dim_unidad",   if_exists="append")
    insertar_tabla(engine, df_canal,     "dim_canal",    if_exists="append")

    separador("Insertando fact_ventas")
    insertar_tabla(engine, fact_ventas, "fact_ventas", if_exists="append")

    # ── RESUMEN FINAL ────────────────────────────────────────
    fin = datetime.now()
    duracion = (fin - inicio).seconds

    print("\n" + "═" * 55)
    print("  PIPELINE GOLD COMPLETADO ✓")
    print(f"  Duración: {duracion} segundos")
    print(f"\n  Registros cargados:")
    print(f"    dim_tiempo    : {len(dim_tiempo)}")
    print(f"    dim_ruta      : {len(dim_ruta)}")
    print(f"    dim_empresa   : {len(dim_empresa)}")
    print(f"    dim_pasajero  : {len(dim_pasajero)}")
    print(f"    dim_unidad    : {len(dim_unidad)}")
    print(f"    dim_canal     : 4")
    print(f"    fact_ventas   : {len(fact_ventas)}")
    print("═" * 55 + "\n")


if __name__ == "__main__":
    run_load_gold()