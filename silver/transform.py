"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: SILVER - Limpieza, Transformación y Join
ARCHIVO: silver/transform.py
DESCRIPCIÓN: Limpia los datos Bronze, aplica validaciones
             con assert y hace el join con CEPALSTAT.
             Guarda resultados limpios en data/silver/
             Uso: python silver/transform.py
============================================================
"""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

BRONZE_SQL_DIR = os.path.join("data", "bronze", "sql")
BRONZE_CEP_DIR = os.path.join("data", "bronze", "cepal")
SILVER_DIR     = os.path.join("data", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d")


# ══════════════════════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════════════════════

def separador(titulo):
    print(f"\n{'─'*55}")
    print(f"  {titulo}")
    print(f"{'─'*55}")


def cargar_csv(directorio, patron):
    """Carga el CSV más reciente que coincida con el patrón."""
    archivos = glob.glob(os.path.join(directorio, patron))
    if not archivos:
        raise FileNotFoundError(
            f"No se encontró '{patron}' en {directorio}. "
            f"Ejecuta run_bronze.py primero."
        )
    archivo = sorted(archivos)[-1]
    df = pd.read_csv(archivo, encoding="utf-8-sig")
    print(f"  [OK] Cargado: {os.path.basename(archivo)} — {len(df)} registros")
    return df


def guardar_silver(df, nombre):
    """Guarda un DataFrame limpio en la carpeta Silver."""
    ruta = os.path.join(SILVER_DIR, f"{nombre}_{TIMESTAMP}.csv")
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"  [OK] Guardado: {nombre}_{TIMESTAMP}.csv — {len(df)} registros")
    return ruta


def validar(condicion, mensaje_error):
    """
    Assert personalizado con mensaje claro.
    Si falla, imprime el error y relanza la excepción.
    """
    try:
        assert condicion, mensaje_error
        print(f"  [ASSERT ✓] {mensaje_error}")
    except AssertionError:
        print(f"  [ASSERT ✗] FALLÓ: {mensaje_error}")
        raise


# ══════════════════════════════════════════════════════════════
# LIMPIEZA DE TABLAS SQL
# ══════════════════════════════════════════════════════════════

def limpiar_geografia(df_raw):
    separador("Limpiando: geografia")
    df = df_raw.copy()
    registros_originales = len(df)

    # Normalizar textos
    df["ciudad"]       = df["ciudad"].str.strip().str.title()
    df["departamento"] = df["departamento"].str.strip().str.title()

    # Rellenar nulos en latitud/longitud con 0 (no críticos)
    df["latitud"]  = df["latitud"].fillna(0.0)
    df["longitud"] = df["longitud"].fillna(0.0)

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["id_ciudad"].isnull().sum() == 0,
            "id_ciudad no tiene nulos")
    validar(df["ciudad"].isnull().sum() == 0,
            "ciudad no tiene nulos")
    validar(df["departamento"].isnull().sum() == 0,
            "departamento no tiene nulos")
    validar(len(df) == registros_originales,
            f"Registros intactos ({registros_originales})")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


def limpiar_empresas(df_raw):
    separador("Limpiando: empresas")
    df = df_raw.copy()
    registros_originales = len(df)

    # Normalizar textos
    df["nombre_empresa"] = df["nombre_empresa"].str.strip().str.title()
    df["estado"]         = df["estado"].str.strip().str.capitalize()
    df["tipo"]           = df["tipo"].str.strip().str.capitalize()

    # Limpiar teléfonos — quitar espacios
    df["telefono"] = df["telefono"].astype(str).str.replace(" ", "", regex=False).str.strip()
    # Opcional: si hay valores como 'nan' después de la conversión, reemplazarlos
    df["telefono"] = df["telefono"].replace("nan", "")

    # Nulos en capacidad_flota_total: imputar con mediana
    mediana_cap = df["capacidad_flota_total"].median()
    nulos_cap = df["capacidad_flota_total"].isnull().sum()
    if nulos_cap > 0:
        df["capacidad_flota_total"] = df["capacidad_flota_total"].fillna(mediana_cap)
        print(f"  [INFO] {nulos_cap} nulos en capacidad_flota_total → imputados con mediana ({mediana_cap})")

    # Estandarizar fechas
    df["fecha_registro"] = pd.to_datetime(
        df["fecha_registro"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["id_empresa"].isnull().sum() == 0,
            "id_empresa no tiene nulos")
    validar(df["nombre_empresa"].isnull().sum() == 0,
            "nombre_empresa no tiene nulos")
    validar(df["estado"].isin(["Activa", "Suspendida", "Inhabilitada"]).all(),
            "estado solo contiene valores válidos")
    validar(len(df) == registros_originales,
            f"Registros intactos ({registros_originales})")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


def limpiar_rutas(df_raw):
    separador("Limpiando: rutas")
    df = df_raw.copy()
    registros_originales = len(df)

    # Normalizar tipo_corredor
    df["tipo_corredor"] = df["tipo_corredor"].str.strip().str.title()

    # Validar que distancia y duración sean positivas
    df = df[df["distancia_km"] > 0]
    df = df[df["duracion_horas"] > 0]
    df = df[df["tarifa_base"] > 0]

    eliminados = registros_originales - len(df)
    if eliminados > 0:
        print(f"  [INFO] {eliminados} rutas con valores inválidos eliminadas")

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["id_ruta"].isnull().sum() == 0,
            "id_ruta no tiene nulos")
    validar((df["distancia_km"] > 0).all(),
            "distancia_km siempre positiva")
    validar((df["tarifa_base"] > 0).all(),
            "tarifa_base siempre positiva")
    validar(len(df) >= registros_originales * 0.90,
            "No se eliminó más del 10% de rutas")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


def limpiar_pasajeros(df_raw):
    separador("Limpiando: pasajeros")
    df = df_raw.copy()
    registros_originales = len(df)

    # Normalizar nombres — title case y strip
    df["nombre"]   = df["nombre"].str.strip().str.title()
    df["apellido"] = df["apellido"].str.strip().str.title()

    # Email: lower case y strip
    df["email"] = df["email"].str.strip().str.lower()

    # Nacionalidad: capitalizar
    df["nacionalidad"] = df["nacionalidad"].str.strip().str.capitalize()

    # Nulos en email y telefono — son opcionales, dejar como vacío
    df["email"]    = df["email"].fillna("")
    df["telefono"] = df["telefono"].fillna("")

    # Estandarizar fechas
    df["fecha_nacimiento"] = pd.to_datetime(
        df["fecha_nacimiento"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["id_pasajero"].isnull().sum() == 0,
            "id_pasajero no tiene nulos")
    validar(df["nombre"].isnull().sum() == 0,
            "nombre no tiene nulos")
    validar(df["apellido"].isnull().sum() == 0,
            "apellido no tiene nulos")
    validar(df["ci"].isnull().sum() == 0,
            "ci no tiene nulos")
    validar(len(df) == registros_originales,
            f"Registros intactos ({registros_originales})")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


def limpiar_unidades(df_raw):
    separador("Limpiando: unidades_flota")
    df = df_raw.copy()
    registros_originales = len(df)

    # Normalizar textos
    df["marca"]         = df["marca"].str.strip().str.title()
    df["modelo"]        = df["modelo"].str.strip().str.upper()
    df["tipo_servicio"] = df["tipo_servicio"].str.strip().str.title()
    df["estado_unidad"] = df["estado_unidad"].str.strip().str.title()
    df["placa"]         = df["placa"].str.strip().str.upper()

    # Validar año de fabricación razonable
    df = df[(df["anio_fabricacion"] >= 1990) & (df["anio_fabricacion"] <= 2025)]

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["id_unidad"].isnull().sum() == 0,
            "id_unidad no tiene nulos")
    validar(df["placa"].isnull().sum() == 0,
            "placa no tiene nulos")
    validar((df["capacidad_asientos"] > 0).all(),
            "capacidad_asientos siempre positiva")
    validar(len(df) >= registros_originales * 0.90,
            "No se eliminó más del 10% de unidades")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


def limpiar_ventas(df_raw):
    separador("Limpiando: ventas_pasajes (tabla principal)")
    df = df_raw.copy()
    registros_originales = len(df)

    print(f"  Registros originales: {registros_originales}")

    # ── 1. ESTANDARIZAR FECHAS ───────────────────────────────
    df["fecha_venta"]  = pd.to_datetime(df["fecha_venta"],  errors="coerce").dt.strftime("%Y-%m-%d")
    df["fecha_viaje"]  = pd.to_datetime(df["fecha_viaje"],  errors="coerce").dt.strftime("%Y-%m-%d")

    fechas_nulas = df["fecha_venta"].isnull().sum() + df["fecha_viaje"].isnull().sum()
    if fechas_nulas > 0:
        print(f"  [INFO] {fechas_nulas} fechas inválidas → eliminando filas")
        df = df.dropna(subset=["fecha_venta", "fecha_viaje"])

    # ── 2. LIMPIAR MONTOS ────────────────────────────────────
    montos_invalidos = (df["monto_pagado"] <= 0).sum()
    if montos_invalidos > 0:
        print(f"  [INFO] {montos_invalidos} montos <= 0 → eliminando filas")
        df = df[df["monto_pagado"] > 0]

    # Redondear monto a 2 decimales
    df["monto_pagado"] = df["monto_pagado"].round(2)

    # ── 3. NORMALIZAR TEXTOS (DATO SUCIO INTENCIONAL) ────────
    # nombre_pasajero_registrado: NULL, espacios extra, mayúsculas mixtas
    df["nombre_pasajero_registrado"] = (
        df["nombre_pasajero_registrado"]
        .fillna("SIN REGISTRO")           # Reemplazar NULL
        .astype(str)
        .str.strip()                       # Quitar espacios inicio/fin
        .str.replace(r"\s+", " ", regex=True)  # Colapsar espacios internos
        .str.upper()                       # Estandarizar a mayúsculas
    )

    # canal_venta y estado_pasaje: normalizar capitalización
    df["canal_venta"]   = df["canal_venta"].str.strip().str.capitalize()
    df["estado_pasaje"] = df["estado_pasaje"].str.strip().str.capitalize()

    # observaciones: rellenar nulos
    df["observaciones"] = df["observaciones"].fillna("Sin observaciones")

    # ── 4. VALIDAR DESCUENTO ─────────────────────────────────
    descuentos_invalidos = (~df["descuento_aplicado"].between(0, 100)).sum()
    if descuentos_invalidos > 0:
        print(f"  [INFO] {descuentos_invalidos} descuentos fuera de rango → corrigiendo a 0")
        df.loc[~df["descuento_aplicado"].between(0, 100), "descuento_aplicado"] = 0

    # ── 5. AGREGAR COLUMNAS CALCULADAS ───────────────────────
    # Año y mes de venta (útil para el esquema estrella)
    df["anio_venta"] = pd.to_datetime(df["fecha_venta"], errors="coerce").dt.year
    df["mes_venta"]  = pd.to_datetime(df["fecha_venta"], errors="coerce").dt.month

    registros_finales = len(df)
    eliminados = registros_originales - registros_finales
    print(f"\n  Registros eliminados : {eliminados}")
    print(f"  Registros finales    : {registros_finales}")

    # ── VALIDACIONES CRÍTICAS ────────────────────────────────
    separador("Validaciones — ventas_pasajes")

    validar(df["id_venta"].isnull().sum() == 0,
            "id_venta no tiene nulos")
    validar(df["id_empresa"].isnull().sum() == 0,
            "id_empresa no tiene nulos")
    validar(df["id_ruta"].isnull().sum() == 0,
            "id_ruta no tiene nulos")
    validar(df["fecha_venta"].isnull().sum() == 0,
            "fecha_venta no tiene nulos")
    validar(df["fecha_viaje"].isnull().sum() == 0,
            "fecha_viaje no tiene nulos")
    validar((df["monto_pagado"] > 0).all(),
            "monto_pagado siempre positivo")
    validar(df["canal_venta"].isin(
                ["Ventanilla", "Telefono", "Web", "Agente"]).all(),
            "canal_venta solo contiene valores válidos")
    validar(df["monto_pagado"].between(5, 500).all(),
            "monto_pagado dentro del rango esperado (Bs. 5 — 500)")
    validar(registros_finales >= registros_originales * 0.90,
            f"No se eliminó más del 10% de registros "
            f"({eliminados}/{registros_originales} = "
            f"{eliminados/registros_originales*100:.1f}%)")

    print(f"\n  Resultado: {registros_finales} registros limpios")
    return df


# ══════════════════════════════════════════════════════════════
# LIMPIEZA CEPALSTAT
# ══════════════════════════════════════════════════════════════

def limpiar_cepal(df_raw):
    separador("Limpiando: cepal_transporte_pasajeros")
    df = df_raw.copy()

    # Convertir valor a numérico
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # Eliminar filas sin valor
    nulos_valor = df["valor"].isnull().sum()
    if nulos_valor > 0:
        print(f"  [INFO] {nulos_valor} filas sin valor → eliminando")
        df = df.dropna(subset=["valor"])

    # Normalizar textos
    df["pais_nombre"]    = df["pais_nombre"].str.strip().str.title()
    df["modo_transporte"]= df["modo_transporte"].str.strip().str.title()

    # Asegurar tipo de anio como int
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")

    # ── VALIDACIONES ────────────────────────────────────────
    print("\n  Validaciones:")
    validar(df["valor"].isnull().sum() == 0,
            "valor no tiene nulos")
    validar((df["valor"] >= 0).all(),
            "valor siempre no negativo")
    validar(df["pais_nombre"].isnull().sum() == 0,
            "pais_nombre no tiene nulos")
    validar(df["anio"].isnull().sum() == 0,
            "anio no tiene nulos")

    print(f"\n  Resultado: {len(df)} registros limpios")
    return df


# ══════════════════════════════════════════════════════════════
# JOIN: VENTAS + CEPALSTAT
# ══════════════════════════════════════════════════════════════

def hacer_join(df_ventas, df_cepal):
    separador("JOIN: ventas_pasajes + CEPALSTAT")

    # Filtrar solo Bolivia y transporte aéreo (lo que tiene datos)
    df_cepal_bo = df_cepal[
        df_cepal["iso3"] == "BOL"
    ][["anio", "modo_transporte", "valor", "unidad"]].copy()

    df_cepal_bo = df_cepal_bo.rename(columns={
        "valor": "cepal_volumen_pasajeros_km",
        "unidad": "cepal_unidad",
        "modo_transporte": "cepal_modo_transporte",
    })

    print(f"  CEPALSTAT Bolivia: {len(df_cepal_bo)} registros (años disponibles)")
    print(f"  Ventas internas  : {len(df_ventas)} registros")

    # Join por año de venta
    df_join = df_ventas.merge(
        df_cepal_bo,
        left_on="anio_venta",
        right_on="anio",
        how="left"    # left join: conservar todos los registros de ventas
    )

    # Cuántos quedaron sin match (años fuera del rango CEPALSTAT)
    sin_match = df_join["cepal_volumen_pasajeros_km"].isnull().sum()
    pct_match = ((len(df_join) - sin_match) / len(df_join)) * 100

    print(f"\n  Registros con contexto CEPALSTAT : {len(df_join) - sin_match} ({pct_match:.1f}%)")
    print(f"  Registros sin match CEPALSTAT    : {sin_match}")
    print(f"  Total registros integrados       : {len(df_join)}")

    # Rellenar nulos del join con 0 (años sin dato CEPALSTAT)
    df_join["cepal_volumen_pasajeros_km"] = df_join["cepal_volumen_pasajeros_km"].fillna(0)
    df_join["cepal_unidad"]               = df_join["cepal_unidad"].fillna("Sin dato CEPALSTAT")
    df_join["cepal_modo_transporte"]      = df_join["cepal_modo_transporte"].fillna("Sin dato")

    # Eliminar columna duplicada 'anio' del merge
    if "anio" in df_join.columns:
        df_join = df_join.drop(columns=["anio"])

    # ── VALIDACIÓN FINAL DEL JOIN ────────────────────────────
    print("\n  Validaciones del dataset integrado:")
    validar(len(df_join) == len(df_ventas),
            f"El join no perdió registros (total={len(df_join)})")
    validar(df_join["id_venta"].isnull().sum() == 0,
            "id_venta no tiene nulos post-join")
    validar(df_join["monto_pagado"].isnull().sum() == 0,
            "monto_pagado no tiene nulos post-join")

    print(f"\n  Columnas del dataset integrado: {list(df_join.columns)}")
    return df_join


# ══════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════

def run_transform():
    inicio = datetime.now()

    print("\n" + "═" * 55)
    print("  PIPELINE SILVER — LIMPIEZA Y TRANSFORMACIÓN")
    print(f"  Proyecto BI — Terminal de Buses de Tarija")
    print(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 55)

    # ── CARGAR DATOS BRONZE ──────────────────────────────────
    separador("Cargando datos Bronze")
    df_geo      = cargar_csv(BRONZE_SQL_DIR, "geografia_*.csv")
    df_emp      = cargar_csv(BRONZE_SQL_DIR, "empresas_*.csv")
    df_rutas    = cargar_csv(BRONZE_SQL_DIR, "rutas_*.csv")
    df_pasajeros= cargar_csv(BRONZE_SQL_DIR, "pasajeros_*.csv")
    df_unidades = cargar_csv(BRONZE_SQL_DIR, "unidades_flota_*.csv")
    df_itin     = cargar_csv(BRONZE_SQL_DIR, "itinerarios_*.csv")
    df_ventas   = cargar_csv(BRONZE_SQL_DIR, "ventas_pasajes_*.csv")
    df_cepal    = cargar_csv(BRONZE_CEP_DIR, "cepal_transporte_pasajeros_*.csv")

    # ── LIMPIAR CADA TABLA ───────────────────────────────────
    df_geo_clean      = limpiar_geografia(df_geo)
    df_emp_clean      = limpiar_empresas(df_emp)
    df_rutas_clean    = limpiar_rutas(df_rutas)
    df_pas_clean      = limpiar_pasajeros(df_pasajeros)
    df_uni_clean      = limpiar_unidades(df_unidades)
    df_itin_clean     = df_itin.copy()  # itinerarios no necesita limpieza adicional
    df_ventas_clean   = limpiar_ventas(df_ventas)
    df_cepal_clean    = limpiar_cepal(df_cepal)

    # ── JOIN VENTAS + CEPALSTAT ──────────────────────────────
    df_integrado = hacer_join(df_ventas_clean, df_cepal_clean)

    # ── GUARDAR TODOS LOS SILVER ─────────────────────────────
    separador("Guardando archivos Silver")
    guardar_silver(df_geo_clean,    "geografia_silver")
    guardar_silver(df_emp_clean,    "empresas_silver")
    guardar_silver(df_rutas_clean,  "rutas_silver")
    guardar_silver(df_pas_clean,    "pasajeros_silver")
    guardar_silver(df_uni_clean,    "unidades_flota_silver")
    guardar_silver(df_itin_clean,   "itinerarios_silver")
    guardar_silver(df_ventas_clean, "ventas_pasajes_silver")
    guardar_silver(df_cepal_clean,  "cepal_transporte_silver")
    guardar_silver(df_integrado,    "dataset_integrado_silver")

    # ── RESUMEN FINAL ────────────────────────────────────────
    fin = datetime.now()
    duracion = (fin - inicio).seconds

    print("\n" + "═" * 55)
    print("  PIPELINE SILVER COMPLETADO ✓")
    print(f"  Duración: {duracion} segundos")
    print(f"\n  Archivos generados en: {SILVER_DIR}/")
    print("  → geografia_silver")
    print("  → empresas_silver")
    print("  → rutas_silver")
    print("  → pasajeros_silver")
    print("  → unidades_flota_silver")
    print("  → itinerarios_silver")
    print("  → ventas_pasajes_silver  ← tabla principal limpia")
    print("  → cepal_transporte_silver")
    print("  → dataset_integrado_silver  ← ventas + CEPALSTAT")
    print("═" * 55 + "\n")

    return df_integrado


if __name__ == "__main__":
    run_transform()