"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: SILVER - Exploración de datos Bronze
ARCHIVO: silver/explore_bronze.py
DESCRIPCIÓN: Analiza los CSVs de Bronze para documentar
             nulos, formatos incorrectos y datos sucios.
             Genera un reporte TXT como evidencia para el PDF.
             Uso: python silver/explore_bronze.py
============================================================
"""

import pandas as pd
import os
import glob
from datetime import datetime

BRONZE_SQL_DIR  = os.path.join("data", "bronze", "sql")
BRONZE_CEP_DIR  = os.path.join("data", "bronze", "cepal")
REPORT_DIR      = os.path.join("data", "silver")

os.makedirs(REPORT_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────

def separador(titulo=""):
    linea = "=" * 60
    if titulo:
        print(f"\n{linea}")
        print(f"  {titulo}")
        print(linea)
    else:
        print(linea)


def cargar_csv_mas_reciente(directorio, patron):
    """Carga el CSV más reciente que coincida con el patrón."""
    archivos = glob.glob(os.path.join(directorio, patron))
    if not archivos:
        raise FileNotFoundError(
            f"No se encontró archivo con patrón '{patron}' en {directorio}\n"
            f"Asegúrate de haber ejecutado run_bronze.py primero."
        )
    archivo = sorted(archivos)[-1]  # el más reciente por nombre
    print(f"  Cargando: {os.path.basename(archivo)}")
    return pd.read_csv(archivo, encoding="utf-8-sig"), archivo


def analizar_tabla(df, nombre_tabla, reporte_lines):
    """Analiza una tabla y agrega los hallazgos al reporte."""
    separador(f"TABLA: {nombre_tabla.upper()}")
    lineas = []

    # ── Forma ────────────────────────────────────────────────
    filas, cols = df.shape
    info = f"  Registros : {filas} | Columnas: {cols}"
    print(info)
    lineas.append(f"\n{'='*60}")
    lineas.append(f"  TABLA: {nombre_tabla.upper()}")
    lineas.append(f"{'='*60}")
    lineas.append(info)

    # ── Nulos ────────────────────────────────────────────────
    nulos = df.isnull().sum()
    nulos_reales = nulos[nulos > 0]
    print(f"\n  Columnas con nulos ({len(nulos_reales)} de {cols}):")
    lineas.append(f"\n  NULOS:")
    if nulos_reales.empty:
        msg = "    → Ninguna columna tiene nulos"
        print(msg); lineas.append(msg)
    else:
        for col, n in nulos_reales.items():
            pct = (n / filas) * 100
            msg = f"    → {col:<35} {n:>5} nulos ({pct:.1f}%)"
            print(msg); lineas.append(msg)

    # ── Textos con espacios extra ─────────────────────────────
    cols_texto = df.select_dtypes(include="object").columns.tolist()
    print(f"\n  Columnas de texto analizadas: {len(cols_texto)}")
    lineas.append(f"\n  PROBLEMAS DE FORMATO EN TEXTOS:")

    problemas_texto = False
    for col in cols_texto:
        serie = df[col].dropna().astype(str)
        # Espacios extra al inicio/fin
        con_espacios = (serie != serie.str.strip()).sum()
        # Mayúsculas mezcladas (ni todo upper ni todo lower ni title)
        con_mixed = serie.apply(
            lambda x: x != x.upper() and x != x.lower() and x != x.title()
        ).sum()

        if con_espacios > 0 or con_mixed > 0:
            problemas_texto = True
            msg = (f"    → {col:<35} "
                   f"espacios_extra={con_espacios:>4}  "
                   f"mayusculas_mixtas={con_mixed:>4}")
            print(msg); lineas.append(msg)

    if not problemas_texto:
        msg = "    → No se detectaron problemas en columnas de texto"
        print(msg); lineas.append(msg)

    # ── Tipos de datos ────────────────────────────────────────
    print(f"\n  Tipos de datos:")
    lineas.append(f"\n  TIPOS DE DATOS:")
    for col, dtype in df.dtypes.items():
        msg = f"    → {col:<35} {str(dtype)}"
        print(msg); lineas.append(msg)

    # ── Estadísticas numéricas básicas ───────────────────────
    cols_num = df.select_dtypes(include="number").columns.tolist()
    if cols_num:
        print(f"\n  Estadísticas numéricas:")
        lineas.append(f"\n  ESTADÍSTICAS NUMÉRICAS:")
        desc = df[cols_num].describe().round(2)
        for col in cols_num:
            if col in desc.columns:
                mn  = desc[col].get("min", "N/A")
                mx  = desc[col].get("max", "N/A")
                med = desc[col].get("mean", "N/A")
                msg = f"    → {col:<30} min={mn}  max={mx}  media={med}"
                print(msg); lineas.append(msg)

    reporte_lines.extend(lineas)
    return df


def analizar_ventas_detalle(df, reporte_lines):
    """Análisis específico de la tabla ventas_pasajes."""
    separador("ANÁLISIS DETALLADO: ventas_pasajes")
    lineas = ["\n  ANÁLISIS DETALLADO ventas_pasajes:"]

    # Valores negativos o cero en monto_pagado
    if "monto_pagado" in df.columns:
        invalidos = df[df["monto_pagado"] <= 0]
        msg = f"  → Montos <= 0          : {len(invalidos)} registros"
        print(msg); lineas.append(msg)

        rango = f"  → Rango montos         : Bs. {df['monto_pagado'].min():.2f} — Bs. {df['monto_pagado'].max():.2f}"
        print(rango); lineas.append(rango)

    # Nombre_pasajero_registrado: dato sucio intencional
    if "nombre_pasajero_registrado" in df.columns:
        nulos_nombre = df["nombre_pasajero_registrado"].isnull().sum()
        serie = df["nombre_pasajero_registrado"].dropna().astype(str)
        con_doble_espacio = serie.str.contains(r"  +").sum()
        mixed_case = serie.apply(
            lambda x: x != x.upper() and x != x.lower()
        ).sum()

        msg1 = f"  → nombre NULL          : {nulos_nombre} registros"
        msg2 = f"  → doble espacio        : {con_doble_espacio} registros"
        msg3 = f"  → mayúsculas mixtas    : {mixed_case} registros"
        for m in [msg1, msg2, msg3]:
            print(m); lineas.append(m)

        total_sucios = nulos_nombre + con_doble_espacio + mixed_case
        pct = (total_sucios / len(df)) * 100
        resumen = f"  → TOTAL datos sucios   : ~{total_sucios} ({pct:.1f}%) ← evidencia capa Silver"
        print(resumen); lineas.append(resumen)

    # Estados de pasaje
    if "estado_pasaje" in df.columns:
        print(f"\n  Distribución estado_pasaje:")
        lineas.append(f"\n  Distribución estado_pasaje:")
        for estado, cnt in df["estado_pasaje"].value_counts().items():
            pct = (cnt / len(df)) * 100
            msg = f"    → {estado:<15} {cnt:>5} ({pct:.1f}%)"
            print(msg); lineas.append(msg)

    # Canal de venta
    if "canal_venta" in df.columns:
        print(f"\n  Distribución canal_venta:")
        lineas.append(f"\n  Distribución canal_venta:")
        for canal, cnt in df["canal_venta"].value_counts().items():
            pct = (cnt / len(df)) * 100
            msg = f"    → {canal:<15} {cnt:>5} ({pct:.1f}%)"
            print(msg); lineas.append(msg)

    reporte_lines.extend(lineas)


def run_explore():
    inicio = datetime.now()

    separador("EXPLORACIÓN BRONZE — PROYECTO BI TERMINAL TARIJA")
    print(f"  Fecha: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

    reporte_lines = [
        "=" * 60,
        "REPORTE DE EXPLORACIÓN — CAPA BRONZE",
        f"Fecha: {inicio.strftime('%Y-%m-%d %H:%M:%S')}",
        "Proyecto BI — Terminal de Buses de Tarija",
        "=" * 60,
    ]

    # ── TABLAS SQL ───────────────────────────────────────────
    separador("FUENTE 1: SQL SERVER — TerminalTarijaDB")
    reporte_lines.append("\n\nFUENTE 1: SQL SERVER — TerminalTarijaDB")

    tablas_sql = [
        ("geografia",      "geografia_*.csv"),
        ("empresas",       "empresas_*.csv"),
        ("rutas",          "rutas_*.csv"),
        ("pasajeros",      "pasajeros_*.csv"),
        ("unidades_flota", "unidades_flota_*.csv"),
        ("itinerarios",    "itinerarios_*.csv"),
        ("ventas_pasajes", "ventas_pasajes_*.csv"),
    ]

    dfs = {}
    for nombre, patron in tablas_sql:
        try:
            df, _ = cargar_csv_mas_reciente(BRONZE_SQL_DIR, patron)
            dfs[nombre] = df
            analizar_tabla(df, nombre, reporte_lines)
        except FileNotFoundError as e:
            print(f"  [WARN] {e}")

    # Análisis detallado de ventas
    if "ventas_pasajes" in dfs:
        analizar_ventas_detalle(dfs["ventas_pasajes"], reporte_lines)

    # ── CEPALSTAT ────────────────────────────────────────────
    separador("FUENTE 2: CEPALSTAT — Indicador 3906")
    reporte_lines.append("\n\nFUENTE 2: CEPALSTAT — Indicador 3906")

    try:
        df_cepal, _ = cargar_csv_mas_reciente(
            BRONZE_CEP_DIR, "cepal_transporte_pasajeros_*.csv"
        )
        analizar_tabla(df_cepal, "cepal_transporte_pasajeros", reporte_lines)
        dfs["cepal"] = df_cepal
    except FileNotFoundError as e:
        print(f"  [WARN] {e}")

    # ── GUARDAR REPORTE ──────────────────────────────────────
    separador("GUARDANDO REPORTE")
    ruta_reporte = os.path.join(
        REPORT_DIR,
        f"reporte_exploracion_{inicio.strftime('%Y%m%d_%H%M%S')}.txt"
    )
    with open(ruta_reporte, "w", encoding="utf-8") as f:
        f.write("\n".join(reporte_lines))

    print(f"  [OK] Reporte guardado en: {ruta_reporte}")

    return dfs


if __name__ == "__main__":
    run_explore()