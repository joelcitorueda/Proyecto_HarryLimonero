"""
============================================================
PROYECTO BI - Terminal de Buses de Tarija
CAPA: BRONZE - Extracción desde API CEPALSTAT
ARCHIVO: bronze/extract_cepal.py
DESCRIPCIÓN: Consume el indicador 3906 (Volumen de transporte
             de pasajeros por modo) de CEPALSTAT para Bolivia
             y países de referencia regional. Guarda JSON y CSV
             crudos en data/bronze/cepal/
============================================================
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURACIÓN DE LA API CEPALSTAT
# ──────────────────────────────────────────────

# IDs confirmados desde el JSON de dimensiones
# Indicador 3906: Volumen de transporte de pasajeros (pasajeros-km)

# IDs de países
PAISES = {
    "Bolivia (Estado Plurinacional de)": 221,
    "Argentina":                          216,
    "Peru":                               244,
    "Chile":                              224,
    "Brasil":                             222,
    "Paraguay":                           242,
}

# IDs de años disponibles (2015-2022, sin 2016)
ANIOS = {
    "2015": 29185,
    "2017": 29187,
    "2018": 29188,
    "2019": 29189,
    "2020": 29190,
    "2021": 29191,
    "2022": 29192,
}

# IDs de modos de transporte
MODOS = {
    "Transporte aereo":        74755,
    "Transporte ferroviario":  74756,
    "Transporte vial":         74757,
}

# ID de tipo de informe (Global)
TIPO_INFORME = 74391

# Base URL de la API
BASE_URL = "https://api-cepalstat.cepal.org/cepalstat/api/v1"
INDICATOR_ID = 3906

# Carpeta de destino Bronze CEPAL
OUTPUT_DIR = os.path.join("data", "bronze", "cepal")


def crear_carpeta_salida():
    """Crea la carpeta de destino si no existe."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[OK] Carpeta de salida: {OUTPUT_DIR}")


def construir_members():
    """
    Construye el string de members para la API combinando
    todos los países, años, modos y tipo de informe.
    """
    ids = []
    ids += list(PAISES.values())          # países
    ids += list(ANIOS.values())           # años
    ids += [TIPO_INFORME]                 # tipo informe global
    ids += list(MODOS.values())           # todos los modos disponibles
    return ",".join(str(i) for i in ids)


def llamar_api(endpoint, params, descripcion):
    """
    Realiza una llamada GET a la API de CEPALSTAT.
    Incluye manejo de errores y assert de validación.
    Retorna el JSON de respuesta.
    """
    url = f"{BASE_URL}/{endpoint}"
    print(f"  [→] Consultando: {descripcion}")
    print(f"      URL: {url}")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # ── VALIDACIÓN CRÍTICA (assert) ──────────────────────
        assert data["header"]["success"] is True, \
            f"API retornó success=False: {data['header'].get('message', 'Sin mensaje')}"

        assert data["header"]["code"] == 200, \
            f"API retornó código {data['header']['code']}"

        records = data["footer"]["records"]
        print(f"  [OK] Respuesta exitosa — {records} registros recibidos")

        # Advertencia si no hay datos (no es error fatal)
        if records == 0:
            print(f"  [WARN] La API respondió pero sin datos (records=0)")
            print(f"         Esto puede significar que el país no reporta ese indicador")

        return data

    except requests.exceptions.Timeout:
        print(f"  [ERROR] Timeout — la API no respondió en 30 segundos")
        raise
    except requests.exceptions.ConnectionError as e:
        print(f"  [ERROR] No se pudo conectar a CEPALSTAT: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"  [ERROR] HTTP Error: {e}")
        raise
    except AssertionError as e:
        print(f"  [ERROR] Validación fallida: {e}")
        raise
    except Exception as e:
        print(f"  [ERROR] Error inesperado: {e}")
        raise


def guardar_json_crudo(data, nombre_archivo):
    """Guarda la respuesta JSON cruda de la API (ingesta fiel)."""
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [OK] JSON crudo guardado: {nombre_archivo}")
    return ruta


def json_a_dataframe(data, nombre_indicador):
    """
    Convierte el JSON de la API a un DataFrame plano y legible.
    Mapea los IDs de dimensiones a nombres descriptivos.
    """
    registros = data["body"]["data"]

    if not registros:
        print(f"  [WARN] No hay registros para convertir a DataFrame")
        return pd.DataFrame()

    # Mapas inversos ID → nombre para decodificar las dimensiones
    mapa_paises = {v: k for k, v in PAISES.items()}
    mapa_anios  = {v: k for k, v in ANIOS.items()}
    mapa_modos  = {v: k for k, v in MODOS.items()}

    filas = []
    for reg in registros:
        fila = {
            "indicador_id":    INDICATOR_ID,
            "indicador_nombre": nombre_indicador,
            "pais_id":         reg.get("dim_208"),
            "pais_nombre":     mapa_paises.get(reg.get("dim_208"), "Desconocido"),
            "iso3":            reg.get("iso3", ""),
            "anio_id":         reg.get("dim_29117"),
            "anio":            mapa_anios.get(reg.get("dim_29117"), "Desconocido"),
            "modo_id":         reg.get("dim_74754"),
            "modo_transporte": mapa_modos.get(reg.get("dim_74754"), "Desconocido"),
            "valor":           reg.get("value"),
            "unidad":          data["body"]["metadata"].get("unit", ""),
            "fuente_id":       reg.get("source_id"),
            "fecha_extraccion": datetime.now().strftime("%Y-%m-%d"),
        }
        filas.append(fila)

    df = pd.DataFrame(filas)

    # Convertir valor a numérico (viene como string desde la API)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    return df


def run_extract_cepal():
    """Función principal — orquesta toda la extracción CEPALSTAT."""
    print("\n" + "=" * 50)
    print("BRONZE - EXTRACCIÓN API CEPALSTAT")
    print("Proyecto BI Terminal de Buses Tarija")
    print("=" * 50 + "\n")

    crear_carpeta_salida()

    timestamp = datetime.now().strftime("%Y%m%d")
    members_str = construir_members()

    # ── EXTRACCIÓN 1: Metadata del indicador ────────────────
    print("[1/3] Obteniendo metadata del indicador 3906...")
    params_meta = {"lang": "es", "format": "json"}
    data_meta = llamar_api(
        f"indicator/{INDICATOR_ID}/metadata",
        params_meta,
        "Metadata indicador 3906"
    )
    guardar_json_crudo(data_meta, f"cepal_metadata_{timestamp}.json")

    # ── EXTRACCIÓN 2: Datos principales (todos los países y modos) ──
    print("\n[2/3] Obteniendo datos de transporte de pasajeros...")
    params_data = {
        "lang":    "es",
        "format":  "json",
        "members": members_str,
        "in":      "1",
        "path":    "0",
    }
    data_principal = llamar_api(
        f"indicator/{INDICATOR_ID}/data",
        params_data,
        "Datos transporte pasajeros - Bolivia y región"
    )
    guardar_json_crudo(data_principal, f"cepal_transporte_pasajeros_{timestamp}.json")

    # Convertir a DataFrame y guardar CSV
    nombre_ind = data_principal["body"]["metadata"].get(
        "indicator_name", "Volumen transporte pasajeros"
    )
    df_principal = json_a_dataframe(data_principal, nombre_ind)

    if not df_principal.empty:
        csv_path = os.path.join(OUTPUT_DIR, f"cepal_transporte_pasajeros_{timestamp}.csv")
        df_principal.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"  [OK] CSV guardado: cepal_transporte_pasajeros_{timestamp}.csv")
        print(f"\n  Vista previa de los datos:")
        print(df_principal[["pais_nombre", "anio", "modo_transporte", "valor", "unidad"]].to_string(index=False))
    else:
        print("  [WARN] No se generó CSV — DataFrame vacío")

    # ── EXTRACCIÓN 3: Dimensiones (para documentación) ──────
    print("\n[3/3] Obteniendo dimensiones del indicador...")
    params_dims = {"lang": "es", "format": "json", "in": "1", "path": "0"}
    data_dims = llamar_api(
        f"indicator/{INDICATOR_ID}/dimensions",
        params_dims,
        "Dimensiones indicador 3906"
    )
    guardar_json_crudo(data_dims, f"cepal_dimensiones_{timestamp}.json")

    # ── RESUMEN FINAL ────────────────────────────────────────
    total_registros = len(df_principal) if not df_principal.empty else 0

    print("\n" + "=" * 50)
    print("EXTRACCIÓN CEPALSTAT COMPLETADA")
    print(f"Indicador       : {INDICATOR_ID} — {nombre_ind[:50]}...")
    print(f"Registros       : {total_registros}")
    print(f"Países incluidos: {len(PAISES)}")
    print(f"Años disponibles: {list(ANIOS.keys())}")
    print(f"Archivos en     : {OUTPUT_DIR}/")
    print("=" * 50 + "\n")

    return df_principal


if __name__ == "__main__":
    run_extract_cepal()