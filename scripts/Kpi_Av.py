import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional
from math import isclose
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from logging.handlers import TimedRotatingFileHandler
import os

# CONFIGURACION API 

URL_API = "http://192.168.40.224:9010/solar-system/av-pr"
ZONA_BOGOTA = ZoneInfo("America/Bogota")

verbose = os.getenv("VERBOSE", "true").lower() == "true"
if not os.path.exists("logs"):
    os.makedirs("logs")

log_handler = TimedRotatingFileHandler(
    filename="logs/av.log",
    when="midnight",
    interval=1,
    backupCount=15,
    encoding='utf-8'
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if verbose else logging.INFO)
logger.addHandler(log_handler)

# CLAVES

PREFIJO_INVER = "PM"
CLAVE_ENERGIA = "_ACTIVE_ENERGY_SUPPLIED_(kWh)"
TAGS_IRRADIANCIA = ["IRRADIANCE_PLC1(W/m^2)", "IRRADIANCE_PLC2(W/m^2)", "solar_rad"]

# PARÁMETROS

UMBRAL_IRRADIANCIA = 50.0  # W/m²
EPSILON_CERO = 1e-9
RUN_ON_START = False

# POTENCIA NOMINAL
 
POTENCIAS_NOM_KW: Dict[str, float] = {
    "PM01": 36.0,
    "PM02": 60.0,
    "PM03": 24.0,
    "PM05": 10.0,
    "PM06": 27.6,
}

# FUNCIONES 

def post_api(url, data):
    try:
        response = requests.post(url, json=data)
        if response.status_code == 201:
            logger.info("Data posted successfully.")
            print("Data posted successfully.")
        else:
            logger.error(f"Failed to post data: {response.status_code} - {response.text}")
    except Exception as e:
        logger.exception(f"Exception during API post: {str(e)}")

# Convierte un timestamp ISO-8601 a datetime y le agrega zona Bogotá si no trae tz
def parsear_ts(ts_iso: str) -> datetime:
    dt = datetime.fromisoformat(ts_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZONA_BOGOTA)
    return dt

# Llama la API, valida 200 OK y devuelve el JSON como dict
def consultar_api() -> Dict[str, List[Dict]]:
    resp = requests.get(URL_API, timeout=30)
    resp.raise_for_status()
    return resp.json()

# Retorna el promedio de la lista o 0.0 si está vacía
def promedio(valores: List[float]) -> float:
    return sum(valores) / len(valores) if valores else 0.0

# Calcula la irradiancia promedio combinando los promedios de cada sensor definido
def calcular_irradiancia_prom(datos: Dict[str, List[Dict]]) -> float:
    promedios_sensores = []
    for tag in TAGS_IRRADIANCIA:
        if tag in datos:
            vals = [float(p.get("value")) for p in datos[tag]
                    if isinstance(p.get("value"), (int, float))]
            if vals:
                promedios_sensores.append(promedio(vals))
    return promedio(promedios_sensores) if promedios_sensores else 0.0

# Devuelve la serie como tuplas (datetime, valor, ts_str) ordenadas por tiempo
def extraer_puntos_energia(serie: List[Dict]) -> List[Tuple[datetime, float, str]]:
    puntos = []
    for p in serie:
        v = p.get("value")
        ts = p.get("ts")
        if isinstance(v, (int, float)) and isinstance(ts, str):
            try:
                puntos.append((parsear_ts(ts), float(v), ts))
            except Exception:
                pass
    puntos.sort(key=lambda x: x[0])
    return puntos

# Para cada inversor calcula horas del período (hper_h) y energía neta (e_neta)
def calcular_hper_y_energia_neta(datos: Dict[str, List[Dict]]) -> Dict[str, Dict[str, float]]:
    resultados: Dict[str, Dict[str, float]] = {}
    for tag, serie in datos.items():
        if not (tag.startswith(PREFIJO_INVER) and CLAVE_ENERGIA in tag):
            continue
        pts = extraer_puntos_energia(serie)
        if len(pts) < 2:
            continue
        t_primera, v_primera, _ = pts[0]
        t_ultima,  v_ultima,  _ = pts[-1]
        hper_h = (t_ultima - t_primera).total_seconds() / 3600.0
        e_neta = v_ultima - v_primera
        inv = tag.split("_", 1)[0]
        resultados[inv] = {"hper_h": hper_h, "e_neta": e_neta}
    return resultados

# Promedia las e_neta de todos los inversores excepto el objetivo
def promedio_otros(inversor_objetivo: str, energias_por_inv: Dict[str, float]) -> float:
    otros = [e for inv, e in energias_por_inv.items() if inv != inversor_objetivo]
    return promedio(otros)

# Calcula, por inversor, huna_h (horas no aprovechadas) usando irradiancia y comparación con “otros”
def calcular_huna(datos: Dict[str, List[Dict]]) -> Dict[str, Dict[str, float]]:
    irr_prom = calcular_irradiancia_prom(datos)
    mediciones = calcular_hper_y_energia_neta(datos)
    if not mediciones:
        return {}
    if irr_prom <= UMBRAL_IRRADIANCIA:
        salida = {}
        for inv, vals in mediciones.items():
            salida[inv] = {"hper_h": 0.0, "e_neta": vals["e_neta"], "huna_h": 0.0} 
        return salida
    energias_por_inv = {inv: vals["e_neta"] for inv, vals in mediciones.items()}
    salida: Dict[str, Dict[str, float]] = {}
    for inv, vals in mediciones.items():
        hper_h = vals["hper_h"]
        e_neta = vals["e_neta"]
        if abs(e_neta) < EPSILON_CERO:
            huna = hper_h
        else:
            prom_ot = promedio_otros(inv, energias_por_inv)
            if abs(prom_ot) > EPSILON_CERO:
                ratio = e_neta / prom_ot
                huna = (1.0 - ratio) * hper_h if ratio < 0.9 else 0.0
            else:
                huna = 0.0
        salida[inv] = {"hper_h": hper_h, "e_neta": e_neta, "huna_h": huna}
    return salida

# AINVi

# Convierte huna/hper en disponibilidad individual ainv ∈ [0,1] por inversor
def calcular_ainv_por_inversor(resultados: Dict[str, Dict[str, float]]) -> Dict[str, float]:

    ainv_por_inv: Dict[str, float] = {}
    for inv, vals in resultados.items():
        hper = vals.get("hper_h", 0.0)
        huna = vals.get("huna_h", 0.0)
        if hper <= EPSILON_CERO:
            ainv = 0.0
        else:
            ainv = max(0.0, min(1.0, 1.0 - (huna / hper)))
        ainv_por_inv[inv] = ainv
    return ainv_por_inv

# ASYS

# Calcula la disponibilidad del parque (ASYS) ponderando ainv por potencia nominal
def calcular_asys_desde_ainv(ainv_por_inv: Dict[str, float]) -> Optional[float]:
    if not ainv_por_inv:
        return None
    p_sys = sum(POTENCIAS_NOM_KW.get(inv, 0.0) for inv in ainv_por_inv.keys())
    if isclose(p_sys, 0.0, abs_tol=EPSILON_CERO):
        return None
    numerador = 0.0
    for inv, ainv in ainv_por_inv.items():
        p_nom = POTENCIAS_NOM_KW.get(inv, 0.0)
        numerador += p_nom * ainv
    return numerador / p_sys

# Encuentra el timestamp más reciente entre todas las series de energía de inversores
def ts_ultima_muestra(datos: Dict[str, List[Dict]]) -> Optional[str]:
    ultimo_dt: Optional[datetime] = None
    ultimo_ts_str: Optional[str] = None

    for tag, serie in datos.items():
        if not (tag.startswith(PREFIJO_INVER) and CLAVE_ENERGIA in tag):
            continue
        if not serie:
            continue
        pts = extraer_puntos_energia(serie)
        if not pts:
            continue
        dt, _, ts_str = pts[-1]
        if (ultimo_dt is None) or (dt > ultimo_dt):
            ultimo_dt = dt
            ultimo_ts_str = ts_str

    return ultimo_ts_str

# TAREA PROGRAMADA

# Consulta datos, calcula HUNA/AINV/ASYS, obtiene ts final y imprime el JSON de salida
def tarea_programada():
    try:
        datos = consultar_api()
    except Exception as e:
        print(json.dumps({"error": f"Fallo consultando API: {e}"}))
        return
    
    resultados = calcular_huna(datos)

    ainv_por_inv = calcular_ainv_por_inversor(resultados)

    asys = calcular_asys_desde_ainv(ainv_por_inv)

    ts_final = ts_ultima_muestra(datos)

    if (asys is None) or (ts_final is None):
        print(json.dumps({"error": "No fue posible calcular ASYS o determinar ts final"}))
        return

    ainv_pct = {f"Ainv{inv}": round(ainv * 100.0, 2)
            for inv, ainv in sorted(ainv_por_inv.items())}
    asys_pct = round(asys * 100.0, 2)
    

    salida = {
        "ts": ts_final,
        "inc_data": {**ainv_pct, "av": asys_pct}
    }
    post_api(url="http://192.168.40.224/api/staging-area/", data=salida)

    
# Ejecuta cada hora al minuto 14:59 (America/Bogota)
if __name__ == "__main__":
    planificador = BlockingScheduler(timezone=str(ZONA_BOGOTA))

    planificador.add_job(
        tarea_programada,
        CronTrigger(minute=14, second=59, timezone=str(ZONA_BOGOTA))
    )
    logger.info("Planificador activo. Se ejecutará cada hora a hh:14:59 (America/Bogota).")
    #print("Planificador activo. Se ejecutará cada hora a hh:14:59 (America/Bogota).")
    try:
        planificador.start()
    except (KeyboardInterrupt, SystemExit):
        print("Planificador detenido.")