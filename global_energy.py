#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kpi_Energy15_total_min.py

Cada 15 minutos:
- Consulta el origen (ENERGY_SRC_URL).
- Calcula ΔE = E(última) - E(penúltima) para cada inversor (PMxx...).
- Suma todos los ΔE -> E_total.
- Muestra en consola un JSON mínimo:

{
  "ts": "<último_ts>",
  "inc_data": {
    "E_total": <suma_ΔE>
  }
}
"""

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# =========================
# CONFIGURACIÓN
# =========================

ZONA_BOGOTA = ZoneInfo("America/Bogota")
URL_API_GET  = os.getenv("ENERGY_SRC_URL",  "http://192.168.40.224:9010/solar-system/av-pr")

PREFIJO_INVER = "PM"
CLAVE_ENERGIA = "_ACTIVE_ENERGY_SUPPLIED_(kWh)"

# =========================
# UTILIDADES
# =========================

def parse_ts(ts_iso: str) -> datetime:
    dt = datetime.fromisoformat(ts_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZONA_BOGOTA)
    return dt

def consultar_api() -> Dict[str, List[Dict]]:
    r = requests.get(URL_API_GET, timeout=30)
    r.raise_for_status()
    return r.json()

def extraer_puntos_energia(serie: List[Dict]) -> List[Tuple[datetime, float, str]]:
    pts = []
    for p in (serie or []):
        v = p.get("value")
        ts = p.get("ts")
        if isinstance(v, (int, float)) and isinstance(ts, str):
            pts.append((parse_ts(ts), float(v), ts))
    pts.sort(key=lambda x: x[0])
    return pts

def tags_inversores(datos: Dict[str, List[Dict]]) -> List[str]:
    return [t for t in (datos.keys() if isinstance(datos, dict) else [])
            if isinstance(t, str) and t.startswith(PREFIJO_INVER) and CLAVE_ENERGIA in t]

def delta_ultimas_dos(pts: List[Tuple[datetime, float, str]]) -> Optional[Tuple[str, float]]:
    """Devuelve (ts_último, delta) o None si no hay al menos 2 puntos."""
    if len(pts) < 2:
        return None
    (_, v_prev, _), (_, v_curr, ts_curr) = pts[-2], pts[-1]
    return ts_curr, round(v_curr - v_prev, 6)

# =========================
# TAREA PRINCIPAL
# =========================

def tarea_energy15_total_min():
    try:
        datos = consultar_api()
    except Exception as e:
        print(json.dumps({"error": f"Fallo consultando API: {e}"}))
        return

    suma_total = 0.0
    ts_publicado: Optional[str] = None
    hubo_algo = False

    for tag in tags_inversores(datos):
        pts = extraer_puntos_energia(datos.get(tag, []))
        res = delta_ultimas_dos(pts)
        if res is None:
            continue

        ts_ultimo, delta = res
        suma_total += delta
        hubo_algo = True

        if (ts_publicado is None) or (parse_ts(ts_ultimo) > parse_ts(ts_publicado)):
            ts_publicado = ts_ultimo

    if not hubo_algo or ts_publicado is None:
        print(json.dumps({"error": "Sin suficientes datos"}))
        return

    payload = {
        "ts": ts_publicado,
        "inc_data": {
            "E_total": round(suma_total, 6)
        }
    }

    print(json.dumps(payload, ensure_ascii=False))

# =========================
# SCHEDULER: cada 15 minutos (00,15,30,45)
# =========================

def main():
    sched = BlockingScheduler(timezone=str(ZONA_BOGOTA))
    sched.add_job(
        tarea_energy15_total_min,
        CronTrigger(minute="*/15", second=0, timezone=str(ZONA_BOGOTA)),
        name="energy15_total_min_job"
    )
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    # No ejecuta la tarea inmediatamente; solo arranca el scheduler.
    main()
