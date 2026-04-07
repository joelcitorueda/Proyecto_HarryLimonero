"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: BRONZE - Extracción desde SQL Server
ARCHIVO: bronze/extract_sql.py
DESCRIPCIÓN: Extrae todas las tablas de TerminalTarijaDB
             y las guarda como CSV en data/bronze/sql/
============================================================
"""

import pyodbc
import pandas as pd
import os
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURACIÓN DE CONEXIÓN
# ──────────────────────────────────────────────
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=TerminalTarijaDB;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Tablas a extraer (en orden para respetar dependencias)
TABLAS = [
    "geografia",
    "empresas",
    "rutas",
    "pasajeros",
    "unidades_flota",
    "itinerarios",
    "ventas_pasajes",
]

# Carpeta de destino Bronze
OUTPUT_DIR = os.path.join("data", "bronze", "sql")


def crear_carpeta_salida():
    """Crea la carpeta de destino si no existe."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[OK] Carpeta de salida: {OUTPUT_DIR}")


def conectar_sql_server():
    """Establece conexión con SQL Server y la retorna."""
    print("[...] Conectando a SQL Server...")
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        print("[OK] Conexión exitosa a TerminalTarijaDB")
        return conn
    except pyodbc.Error as e:
        print(f"[ERROR] No se pudo conectar a SQL Server: {e}")
        raise


def extraer_tabla(conn, nombre_tabla):
    """
    Extrae una tabla completa como DataFrame y la guarda en CSV.
    Retorna el DataFrame extraído.
    """
    print(f"  [→] Extrayendo tabla: {nombre_tabla}...")
    try:
        query = f"SELECT * FROM {nombre_tabla}"
        df = pd.read_sql(query, conn)

        # Guardar CSV crudo (sin modificaciones — ingesta fiel)
        timestamp = datetime.now().strftime("%Y%m%d")
        nombre_archivo = f"{nombre_tabla}_{timestamp}.csv"
        ruta_archivo = os.path.join(OUTPUT_DIR, nombre_archivo)

        df.to_csv(ruta_archivo, index=False, encoding="utf-8-sig")

        print(f"  [OK] {nombre_tabla}: {len(df)} registros → {nombre_archivo}")
        return df

    except Exception as e:
        print(f"  [ERROR] Fallo al extraer {nombre_tabla}: {e}")
        raise


def guardar_resumen(resumen):
    """Guarda un archivo de resumen de la extracción."""
    ruta = os.path.join(OUTPUT_DIR, "resumen_extraccion.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(ruta, "w", encoding="utf-8") as f:
        f.write("=" * 50 + "\n")
        f.write("RESUMEN DE EXTRACCIÓN BRONZE - SQL SERVER\n")
        f.write(f"Fecha y hora: {timestamp}\n")
        f.write(f"Base de datos: TerminalTarijaDB\n")
        f.write("=" * 50 + "\n\n")
        for tabla, registros in resumen.items():
            f.write(f"  {tabla:<20} → {registros:>6} registros\n")
        f.write(f"\n  TOTAL TABLAS: {len(resumen)}\n")
        f.write(f"  TOTAL REGISTROS: {sum(resumen.values())}\n")

    print(f"\n[OK] Resumen guardado en: {ruta}")


def run_extract_sql():
    """Función principal — orquesta toda la extracción."""
    print("\n" + "=" * 50)
    print("BRONZE - EXTRACCIÓN SQL SERVER")
    print("Proyecto BI Terminal de Buses Tarija")
    print("=" * 50 + "\n")

    crear_carpeta_salida()

    conn = conectar_sql_server()
    resumen = {}

    try:
        print("\n[→] Iniciando extracción de tablas...\n")
        for tabla in TABLAS:
            df = extraer_tabla(conn, tabla)
            resumen[tabla] = len(df)

    finally:
        conn.close()
        print("\n[OK] Conexión cerrada.")

    guardar_resumen(resumen)

    print("\n" + "=" * 50)
    print("EXTRACCIÓN SQL COMPLETADA")
    print(f"Tablas extraídas : {len(resumen)}")
    print(f"Total registros  : {sum(resumen.values())}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    run_extract_sql()