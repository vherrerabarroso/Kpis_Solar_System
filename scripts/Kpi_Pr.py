import json, requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import logging
from logging.handlers import TimedRotatingFileHandler
import os

# CONFIGURACION API 

URL_API = "http://192.168.40.224:9010/solar-system/av-pr"
ZONA_BOGOTA = ZoneInfo("America/Bogota")
EJECUTAR_AL_INICIO = True

verbose = os.getenv("VERBOSE", "true").lower() == "true"
if not os.path.exists("logs"):
    os.makedirs("logs")

log_handler = TimedRotatingFileHandler(
    filename="logs/pr.log",
    when="midnight",
    interval=1,
    backupCount=15,
    encoding='utf-8'
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if verbose else logging.INFO)
logger.addHandler(log_handler)

# POTENCIA NOMINAL

POTENCIAS_NOM_KW: Dict[str, float] = {
    "PM01": 36.0,
    "PM02": 60.0,
    "PM03": 24.0,
    "PM05": 10.0,
    "PM06": 27.6,
}

# CLAVES

IRR_KEYS = ["IRRADIANCE_PLC1(W/m^2)", "IRRADIANCE_PLC2(W/m^2)", "solar_rad"]
TIN_KEY, TOUT_KEY = "temp_in", "temp_out"
PREFIJO_INVER = "PM"
CLAVE_ENERGIA = "_ACTIVE_ENERGY_SUPPLIED_(kWh)"

# PARAMETROS

BETA_MONO = -0.0036 # 1/°C

TMOD_LIST = [29.4, 29.2, 29.2, 29.3, 29.9, 30.3,
         30.9, 30.5, 28.9, 27.7, 27.5, 28.5]


IRR_UMBRAL = 30.0 # W/m² 

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


# Convierte una cadena de texto de fecha y hora en un objeto 
def parse_dt(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=ZONA_BOGOTA)

# Obtiene la temperatura del módulo fotovoltaico de referencia para el mes de una fecha dada
def month_tmod(dt: datetime) -> float:
    m = dt.astimezone(ZONA_BOGOTA).month
    return float(TMOD_LIST[m-1])

# Identifica y mapea las claves de datos de energía de cada inversor presente en el diccionario de datos
def energy_series_keys(blob: Dict[str, List[Dict]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in blob.items():
        if not (k.startswith(PREFIJO_INVER) and CLAVE_ENERGIA in k):
            continue
        if not isinstance(v, list) or len(v) < 2:
            continue
        inv = k.split("_", 1)[0]
        out[inv] = k
    return out

# Extrae y convierte las marcas de tiempo de las series de datos en una lista de objetos datetime
def time_grid(blob: Dict[str, List[Dict]], ref_key: Optional[str]=None) -> List[datetime]:
    kmap = energy_series_keys(blob)
    if not kmap:
        return []
    key = ref_key or list(kmap.values())[0]
    return [parse_dt(p["ts"]) for p in blob[key]]

# Calcula el promedio de una lista de valores numéricos, ignorando los no numéricos
def mean(values: List[float]) -> Optional[float]:
    vals = [float(v) for v in values if isinstance(v, (int, float))]
    return (sum(vals)/len(vals)) if vals else None

# Es la función principal que calcula el Performance Ratio (PR) para cada inversor y para todo el sistema, a partir de los datos de la API
def calcular_pr(blob: Dict[str, List[Dict]]) -> Dict:
    tg = time_grid(blob)
    if len(tg) < 2:
        return {"ts": None, "pri": {}, "pr": None}

    t_ini, t_fin = tg[0], tg[-1]
    dt_h_total = (t_fin - t_ini).total_seconds()/3600.0
    if dt_h_total <= 0:
        return {"ts": t_fin.isoformat(), "pri": {}, "pr": None}

    per_sensor_means: List[Optional[float]] = []
    for k in IRR_KEYS:
        serie = blob.get(k, [])
        per_sensor_means.append(mean([p.get("value") for p in serie]))
    irr_avg_wm2 = mean([m for m in per_sensor_means if m is not None])

    if (irr_avg_wm2 is None) or (irr_avg_wm2 <= IRR_UMBRAL):
        kmap = energy_series_keys(blob)  
        #pri_ceros = {inv: 0.0 for inv in kmap.keys() if inv in POTENCIAS_NOM_KW} 
        return {"ts": t_fin.isoformat(),"inc_data": {'PrPM01': 0.0, 'PrPM02': 0.0, 'PrPM03': 0.0, 'PrPM05': 0.0, 'PrPM06': 0.0, 'pr': 0.0}}

    
    prom_in  = mean([p.get("value") for p in blob.get(TIN_KEY, [])])
    prom_out = mean([p.get("value") for p in blob.get(TOUT_KEY, [])])
    Tmeas = (prom_in + prom_out) / 2.0 if (prom_in is not None and prom_out is not None) else (prom_in if prom_in is not None else prom_out)

    Tmod = month_tmod(t_fin)
    Lt = (BETA_MONO * (Tmod - Tmeas)) if Tmeas is not None else None

    
    G_dgi = irr_avg_wm2 * dt_h_total / 1000.0

    
    kmap = energy_series_keys(blob)
    Enet_by_inv: Dict[str, float] = {}
    for inv, key in kmap.items():
        serie = blob[key]
        e0, e1 = serie[0].get("value"), serie[-1].get("value")
        if isinstance(e0, (int, float)) and isinstance(e1, (int, float)):
            d = float(e1) - float(e0)
            if d >= 0:
                Enet_by_inv[inv] = d

    
    pri: Dict[str, float] = {}
    for inv, Enet in Enet_by_inv.items():
        Pnom = POTENCIAS_NOM_KW.get(inv)
        if Pnom is None or None in (G_dgi, Lt):
            continue
        denom = Pnom * (1.0 - Lt) * G_dgi
        if denom > 0:
            pri[inv] = round(max(0.0, min(100.0, (Enet/denom)*100.0)), 2)

    
    E_total = sum(Enet_by_inv[inv] for inv in Enet_by_inv if inv in POTENCIAS_NOM_KW)
    P_total = sum(POTENCIAS_NOM_KW[inv] for inv in Enet_by_inv if inv in POTENCIAS_NOM_KW)
    pr_total = None
    if None not in (G_dgi, Lt) and P_total > 0:
        denom_tot = P_total * (1.0 - Lt) * G_dgi
        if denom_tot > 0:
            pr_total = round(max(0.0, min(100.0, (E_total/denom_tot)*100.0)), 2
                             )
    pri_pref = {f"Pr{inv}": val for inv, val in pri.items()}

    return {
    "ts": t_fin.isoformat(),
    "inc_data": {**pri_pref, "pr": pr_total}
}
    
# Realiza una solicitud HTTP a la API y devuelve los datos en formato JSON
def consultar_api() -> Dict[str, List[Dict]]:
    r = requests.get(URL_API, timeout=20)
    r.raise_for_status()
    return r.json()

# Orquesta el flujo de trabajo: consulta la API, calcula el PR y muestra el resultado en la consola
def tarea():
    try:
        datos = consultar_api()
        salida = calcular_pr(datos)
        print(salida)
        post_api(url="http://192.168.40.224/api/staging-area/", data=salida)
    except Exception as e:
        print(json.dumps({"ts": None, "pri": {}, "pr": None, "error": str(e)}, ensure_ascii=False))

# Configura y ejecuta un planificador que llama a la función tarea periódicamente
if __name__ == "__main__":
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    planificador = BlockingScheduler(timezone=str(ZONA_BOGOTA))
    planificador.add_job(
        tarea,
        CronTrigger(minute=14, second=59, timezone=str(ZONA_BOGOTA))
    )
    logger.info("Planificador activo. Se ejecutará cada hora a hh:14:59 (America/Bogota).")
    try:
        planificador.start()
    except (KeyboardInterrupt, SystemExit):
        print("Planificador detenido.")
