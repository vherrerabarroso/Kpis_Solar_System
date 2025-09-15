# --------------------- Importes mínimos ---------------------
import os, json, re, requests
import pandas as pd, numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import DATETIME as MYSQL_DATETIME
from dotenv import load_dotenv
from datetime import datetime, timezone
from pytz import timezone as tz

# --------------------- Config/API ---------------------------
load_dotenv()
API_KEY, API_SECRET = os.getenv("WL_API_KEY"), os.getenv("WL_API_SECRET")
STATION_ID = os.getenv("STATION_ID", "85133")
BOGOTA = tz("America/Bogota")
URL_API = f"https://api.weatherlink.com/v2/current/{STATION_ID}?api-key={API_KEY}"
HEADERS = {"X-Api-Secret": API_SECRET}

def f_to_c(f): return round((f - 32) * 5/9, 2)
def to_slot(dt): return dt.replace(minute=dt.minute - dt.minute % 15, second=0, microsecond=0)
def fmt_ms(dt): return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def fetch_temp_slot_c():
    r = requests.get(URL_API, headers=HEADERS, timeout=15); r.raise_for_status()
    j = r.json()
    for s in j.get("sensors", []):
        for rec in s.get("data", []):
            for k in ("temp", "temp_out", "temp_in"):
                v = rec.get(k)
                if v is not None:
                    ts_loc = datetime.fromtimestamp(rec["ts"], tz=timezone.utc).astimezone(BOGOTA)
                    return to_slot(ts_loc).replace(tzinfo=None), f_to_c(v)
    return None, None

# --------------------- MySQL -------------------------------
USER, PASSWORD, HOST, PORT, DB = "solar", "solar123", "127.0.0.1", 3306, "solar_data"
ENGINE = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?charset=utf8mb4", pool_pre_ping=True)
TABLE_IN = "Datos_15M2"
TABLE_OUT_MAIN = "Kpi_PR"

with ENGINE.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS temp_quarterly (
          ts DATETIME(3) NOT NULL,
          station_id VARCHAR(32) NOT NULL,
          temp_c DECIMAL(5,2) NOT NULL,
          PRIMARY KEY (ts, station_id)
        ) ENGINE=InnoDB
    """))

def upsert_temp(slot_dt, temp_c):
    with ENGINE.begin() as conn:
        conn.execute(
            text("""INSERT INTO temp_quarterly (ts, station_id, temp_c)
                    VALUES (:ts, :sid, :t)
                    ON DUPLICATE KEY UPDATE temp_c = VALUES(temp_c)"""),
            {"ts": fmt_ms(slot_dt), "sid": STATION_ID, "t": float(temp_c)}
        )

# --------------------- Parámetros PR -----------------------
P_STC = 261.32          # kWp
IRR_THR = 30.0          # W/m2
STEP_MIN = 15.0         # min
BETA_MONO, BETA_POLY = -0.0036, -0.004
COUNT_MONO, WP_MONO = 624, 395.0
COUNT_POLY, WP_POLY = 53, 280.0
TMOD_LIST = [36.9, 36.5, 36.4, 36.9, 38.1, 38.6, 24.4, 23.3, 36.5, 34.5, 34.4, 35.7]

# --------------------- Utilidades --------------------------
def normalize_ts(s: pd.Series, target_tz: str | None = None) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    if getattr(s.dtype, "tz", None) is not None:
        if target_tz: s = s.dt.tz_convert(target_tz)
        s = s.dt.tz_localize(None)
    return s

def get_num(d, key):
    try:
        v = d.get(key, None)
        if v is None: return np.nan
        if isinstance(v, (int, float)): return float(v)
        if isinstance(v, str):
            v = v.strip().replace(",", ".")
            return float(v) if v not in ("", "null", "None") else np.nan
        return float(v)
    except: return np.nan

# -------- Paso 1: guardar temp (opcional) ------------------
slot_api, temp_c = fetch_temp_slot_c()
if slot_api and temp_c is not None:
    upsert_temp(slot_api, temp_c)

# -------- Paso 2: cargar base y temperaturas ---------------
raw = pd.read_sql(text(f"SELECT id, ts, inc_data FROM {TABLE_IN} ORDER BY ts"), ENGINE)
if raw.empty:
    raise SystemExit("Datos_15M2 sin filas.")

raw["inc_dict"] = raw["inc_data"].apply(lambda x: json.loads(x) if isinstance(x, str) else (x or {}))
raw["ts"] = normalize_ts(raw["ts"], target_tz="America/Bogota")

temps = pd.read_sql(text("SELECT ts, temp_c FROM temp_quarterly WHERE station_id=:sid ORDER BY ts"),
                    ENGINE, params={"sid": STATION_ID})
temps["ts"] = pd.to_datetime(temps["ts"], errors="coerce")

raw = raw.sort_values("ts").copy()
temps = temps.rename(columns={"ts":"ts_temp","temp_c":"Tamb_c"}).sort_values("ts_temp").copy()
df = pd.merge_asof(raw, temps, left_on="ts", right_on="ts_temp",
                   direction="nearest", tolerance=pd.Timedelta("8min"))
df["Tmeas_i"] = df["Tamb_c"].fillna(0.0).astype(float)

# -------- Paso 3: irradiancia (W/m2) -----------------------
irr1 = df["inc_dict"].apply(lambda d: get_num(d, "IRRADIANCE_PLC1(W/m^2)"))
irr2 = df["inc_dict"].apply(lambda d: get_num(d, "IRRADIANCE_PLC2(W/m^2)"))
irr_avg = (irr1 + irr2) / 2.0
diffPct = ((irr1 - irr2).abs() / irr_avg.replace(0, np.nan) * 100.0).where(irr_avg > 0, 0.0)
irr_used = irr_avg.where(diffPct < 5.0, pd.concat([irr1, irr2], axis=1).max(axis=1))
valid_irr = irr_used > IRR_THR

# -------- Paso 4: energía desde contadores -----------------
pat = re.compile(r"^PM\d{2}_ACTIVE_ENERGY_SUPPLIED", re.I)
all_keys = set().union(*[d.keys() for d in df["inc_dict"]])
energy_keys = sorted([k for k in all_keys if pat.search(k)])
if not energy_keys:
    raise SystemExit("Sin claves de energía en inc_data.")

acc = pd.DataFrame({k: df["inc_dict"].apply(lambda d, kk=k: get_num(d, kk)) for k in energy_keys})
# Diferencias entre intervalos, saturando negativos a 0
acc_diff = acc.apply(pd.to_numeric, errors="coerce").diff().clip(lower=0).fillna(0.0)
Eneta_raw = acc_diff.sum(axis=1).fillna(0.0)  # kWh (sin enmascarar)

# -------- Paso 5: coeficientes térmicos --------------------
P_mono = COUNT_MONO * WP_MONO / 1000.0
P_poly = COUNT_POLY * WP_POLY / 1000.0
P_tot  = max(P_mono + P_poly, 1e-9)
Beta   = (P_mono / P_tot) * BETA_MONO + (P_poly / P_tot) * BETA_POLY

mes_num = df["ts"].dt.month
Tmod = mes_num.apply(lambda m: TMOD_LIST[m-1] if pd.notna(m) else np.nan).astype(float)
Lt_i = Beta * (Tmod - df["Tmeas_i"])

# -------- Paso 6: Gdgi y PR (aplicando umbral) -------------
step_h = STEP_MIN / 60.0
Gdgi_raw = (irr_used * step_h) / 1000.0                 # kWh/m2

Den_i = P_STC * Gdgi_raw * (1 - Lt_i)                   # kWh (kWp * kWh/m2 * adim)
PR_raw = np.where(Den_i > 0, (Eneta_raw / Den_i).clip(0, 1) * 100.0, np.nan)

# Forzar a 0 SOLO si no cumple irradiancia
Eneta_kWh     = np.where(valid_irr, Eneta_raw, 0.0)
Gdgi_i_kWh_m2 = np.where(valid_irr, Gdgi_raw, 0.0)
PR_pct        = np.where(valid_irr, PR_raw, 0.0)

# -------- Paso 7: salida final --------------------------------
df_out = pd.DataFrame({
    "id": df["id"].values,
    "ts": df["ts"].values,
    "Eneta_kWh": Eneta_kWh,                 # kWh
    "Pstc_kWp": P_STC,                      # kWp
    "Lt_i_pu": Lt_i.values,                 # p.u.
    "Beta_%/°C": Beta,                      # %/°C
    "Tmod_°C": Tmod.values,                 # °C
    "Tmeas_i_°C": df["Tmeas_i"].values,     # °C
    "Gdgi_i_kWh_m2": Gdgi_i_kWh_m2,         # kWh/m2
    "Irradiance_W_m2": irr_used.values,     # W/m2
    "PR_%": PR_pct                          # %
})

df_out = df_out[df["ts"].notna()].reset_index(drop=True)

# -------- Guardar en MySQL (ts como DATETIME(3)) ----------
with ENGINE.begin() as conn:
    df_out.to_sql(TABLE_OUT_MAIN, conn, if_exists="replace", index=False,
                  dtype={"ts": MYSQL_DATETIME(fsp=3)})

print(f"Tabla '{TABLE_OUT_MAIN}' insertada correctamente en la base de datos '{DB}'.")
