"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: SILVER - Orquestador
ARCHIVO: silver/run_silver.py
DESCRIPCIÓN: Ejecuta exploración y limpieza en orden.
             Uso: python silver/run_silver.py
============================================================
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from silver.explore_bronze import run_explore
from silver.transform      import run_transform


def run_silver():
    inicio = datetime.now()

    print("\n" + "█" * 55)
    print("  PIPELINE SILVER — INICIO")
    print(f"  {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("█" * 55)

    errores = []

    # PASO 1: Exploración
    print("\n[PASO 1/2] Exploración de datos Bronze")
    print("-" * 40)
    try:
        run_explore()
        print("[PASO 1] ✓ Exploración completada")
    except Exception as e:
        msg = f"[PASO 1] ✗ FALLO exploración: {e}"
        print(msg)
        errores.append(msg)

    # PASO 2: Limpieza + Join
    print("\n[PASO 2/2] Limpieza, validaciones y Join")
    print("-" * 40)
    try:
        run_transform()
        print("[PASO 2] ✓ Transformación completada")
    except AssertionError as e:
        msg = f"[PASO 2] ✗ VALIDACIÓN FALLIDA: {e}"
        print(msg)
        errores.append(msg)
    except Exception as e:
        msg = f"[PASO 2] ✗ FALLO transformación: {e}"
        print(msg)
        errores.append(msg)

    # Resultado
    fin = datetime.now()
    duracion = (fin - inicio).seconds

    print("\n" + "█" * 55)
    print("  PIPELINE SILVER — FIN")
    print(f"  Duración: {duracion} segundos")

    if errores:
        print(f"\n  ⚠ COMPLETADO CON {len(errores)} ERROR(ES):")
        for e in errores:
            print(f"    - {e}")
        sys.exit(1)
    else:
        print("\n  ✓ TODOS LOS PASOS COMPLETADOS SIN ERRORES")
        print("  Archivos listos en: data/silver/")

    print("█" * 55 + "\n")


if __name__ == "__main__":
    run_silver()