"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: BRONZE - Orquestador principal
ARCHIVO: bronze/run_bronze.py
DESCRIPCIÓN: Ejecuta en orden la extracción completa
             de SQL Server y CEPALSTAT.
             Uso: python bronze/run_bronze.py
============================================================
"""

import sys
import os
from datetime import datetime

# Agregar el directorio raíz al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze.extract_sql   import run_extract_sql
from bronze.extract_cepal import run_extract_cepal


def run_bronze():
    inicio = datetime.now()

    print("\n" + "█" * 50)
    print("  PIPELINE BRONZE — INICIO")
    print(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("█" * 50)

    errores = []

    # ── PASO 1: SQL Server ───────────────────────────────────
    print("\n[PASO 1/2] Extracción desde SQL Server")
    print("-" * 40)
    try:
        run_extract_sql()
        print("[PASO 1] ✓ Completado")
    except Exception as e:
        msg = f"[PASO 1] ✗ FALLO SQL Server: {e}"
        print(msg)
        errores.append(msg)

    # ── PASO 2: CEPALSTAT ────────────────────────────────────
    print("\n[PASO 2/2] Extracción desde API CEPALSTAT")
    print("-" * 40)
    try:
        run_extract_cepal()
        print("[PASO 2] ✓ Completado")
    except Exception as e:
        msg = f"[PASO 2] ✗ FALLO CEPALSTAT: {e}"
        print(msg)
        errores.append(msg)

    # ── RESULTADO FINAL ──────────────────────────────────────
    fin = datetime.now()
    duracion = (fin - inicio).seconds

    print("\n" + "█" * 50)
    print("  PIPELINE BRONZE — FIN")
    print(f"  Duración: {duracion} segundos")

    if errores:
        print(f"\n  ⚠ COMPLETADO CON {len(errores)} ERROR(ES):")
        for e in errores:
            print(f"    - {e}")
        print("\n  Los archivos que sí se extrajeron están en data/bronze/")
        sys.exit(1)
    else:
        print("\n  ✓ TODOS LOS PASOS COMPLETADOS SIN ERRORES")
        print("  Archivos disponibles en:")
        print("    → data/bronze/sql/   (tablas de TerminalTarijaDB)")
        print("    → data/bronze/cepal/ (indicadores CEPALSTAT)")

    print("█" * 50 + "\n")


if __name__ == "__main__":
    run_bronze()