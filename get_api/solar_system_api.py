import os, traceback
from decimal import Decimal
from typing import Any, Dict, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

import psycopg
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

app = FastAPI(title="Solar System", version="1.0.0")

# Variables a consultar (incluye temperaturas)
VARIABLES: List[str] = [
    "PM01_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM02_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "PM03_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM04_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "PM05_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM06_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "PM07_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM08_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "PM09_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM10_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "PM11_ACTIVE_ENERGY_SUPPLIED_(kWh)", "PM12_ACTIVE_ENERGY_SUPPLIED_(kWh)",
    "IRRADIANCE_PLC1(W/m^2)", "IRRADIANCE_PLC2(W/m^2)", "solar_rad",
    "temp_in", "temp_out"
]

# --- TZ helpers ---
BOGOTA_TZ = ZoneInfo("America/Bogota")

def to_bogota_iso(ts_any) -> str:
    """
    Convierte 'ts_any' (datetime o string ISO) a America/Bogota y devuelve ISO8601 con offset -05:00.
    - Si llega naive => se asume UTC.
    - Si llega string => se parsea, asumiendo UTC si termina en 'Z' o es naive.
    """
    # Si es string, intentamos parsear
    if isinstance(ts_any, str):
        # datetime.fromisoformat no acepta 'Z', lo reemplazamos por '+00:00'
        ts_str = ts_any.replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            # último recurso: tratamos como naive y asumimos UTC
            ts = datetime.fromisoformat(ts_str.split("+")[0]).replace(tzinfo=timezone.utc)
    else:
        ts = ts_any

    # Asegurar que tenga tzinfo
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.replace(tzinfo=timezone.utc)

    # Normalizar a UTC y luego a Bogotá
    ts_bogota = ts.astimezone(timezone.utc).astimezone(BOGOTA_TZ)

    # Puedes ajustar precisión: seconds, milliseconds, microseconds
    return ts_bogota.isoformat()  # ejemplo: '2025-09-12T12:50:21.998-05:00'

SQL_LAST_TWO_HOURS = """
SELECT
    vt.tag        AS variable_tag,
    ts.timestamp  AS ts,
    vl.value      AS value
FROM public.backend_variablelog vl
JOIN public.backend_variabletype vt ON vl.var_type_id  = vt.id
JOIN public.backend_timestamps ts   ON vl.timestamp_id = ts.id
WHERE ts.timestamp >= date_trunc('hour', now()) - interval '1 hour'
  AND ts.timestamp <  date_trunc('hour', now()) + interval '1 hour'
  AND vt.tag = ANY(%s::text[])
ORDER BY variable_tag, ts ASC;   -- todas las mediciones de la hora actual y la anterior
"""



@app.get("/solar-system/av-pr")
def get_latest_5() -> Dict[str, List[Dict[str, Any]]]:
    """
    Devuelve SIEMPRE los últimos 5 registros de cada variable en VARIABLES,
    con el timestamp convertido a America/Bogota (offset -05:00).
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL no está configurada")

    result: Dict[str, List[Dict[str, Any]]] = {k: [] for k in VARIABLES}

    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_LAST_TWO_HOURS, (VARIABLES,))
                rows = cur.fetchall()
                print(f"[DEBUG] Filas devueltas: {len(rows)} (vars={len(VARIABLES)} x 5)")

                for variable_tag, ts, value in rows:
                    # Convertimos a America/Bogota y serializamos sin 'Z'
                    ts_out = to_bogota_iso(ts)
                    val = float(value) if isinstance(value, (Decimal, float, int)) else None
                    result[variable_tag].append({"ts": ts_out, "value": val})

        # Sanity check: exactamente 5 por variable
        for tag, values in result.items():
            if len(values) != 5:
                print(f"[WARN] {tag} tiene {len(values)} registros en vez de 5")

        return result

    except Exception as e:
        print("[ERROR]\n" + traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

