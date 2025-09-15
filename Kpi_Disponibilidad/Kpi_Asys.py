# ----------------------------------------- Librerias --------------------------------------------
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine

# -------------------------------- Conexión con la base de datos --------------------------------
MYSQL_USER = "solar"; MYSQL_PASS = "solar123"
MYSQL_HOST = "127.0.0.1"; MYSQL_PORT = 3306
MYSQL_DB   = "solar_data"
ENGINE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
engine = create_engine(ENGINE_URL, pool_pre_ping=True)

SRC_TABLE = "Datos"  # columnas: id, ts, inc_data(JSON)

# ------------------------ Configuración inversores y constantes ---------------------------------
INVERSORES  = [f"PM{str(i).zfill(2)}" for i in range(1,13)]
UMBRAL_IRR  = 50.0        # W/m²
BLOQUE_H    = 1.0         # 1 hora

# Potencias nominales reales y base de operación al 60% (como en tu Excel)
PNOMINAL_KW_REAL = {
    "PM01": 36.0, "PM02": 60.0, "PM03": 24.0, "PM04": 24.0,
    "PM05": 10.0, "PM06": 27.6, "PM07":  1.5, "PM08":  7.2,
    "PM09":  7.2, "PM10":  3.0, "PM11":  5.0, "PM12":  5.0
}
PNOMINAL_KW = {k: v * 0.50 for k, v in PNOMINAL_KW_REAL.items()}

# ------------------------------------ Funciones auxiliares --------------------------------------
def ensure_dict(x):
    if isinstance(x, dict): return x
    try: return json.loads(x) if isinstance(x, str) else {}
    except: return {}

def get_num(d, key):
    try: return float(d[key])
    except: return float("nan")

def parse_mantenimiento(d):
    v = d.get("Mantenimiento", None)
    if v is None: return set()
    if isinstance(v, list):
        cand = {str(x).strip().upper() for x in v}
    elif isinstance(v, str):
        v = v.strip()
        if not v or v.upper() in {"NINGUNO","NONE","NO","NA"}: return set()
        cand = {p.strip().upper() for p in v.replace(";", ",").split(",") if p.strip()}
    else:
        return set()
    return {c for c in cand if c in set(INVERSORES)}

# ------------------------------------ Leer datos crudos -----------------------------------------
base = pd.read_sql(f"SELECT id, ts, inc_data FROM {SRC_TABLE}", engine)
base["ts"] = pd.to_datetime(base["ts"], errors="coerce")
base = base.sort_values("ts").reset_index(drop=True)
base["inc_dict"] = base["inc_data"].apply(ensure_dict)
base["mant_set"] = base["inc_dict"].apply(parse_mantenimiento)
base["ts_h"] = base["ts"].dt.floor("h")

# ------------------------------- IRRADIANCIA → HPER por hora -----------------------------------
irr1 = base["inc_dict"].apply(lambda d: get_num(d, "IRRADIANCE_PLC1(W/m^2)"))
irr2 = base["inc_dict"].apply(lambda d: get_num(d, "IRRADIANCE_PLC2(W/m^2)"))
prom    = (irr1 + irr2) / 2.0
dAbs    = (irr1 - irr2).abs()
diffPct = (dAbs / prom * 100.0).where(prom > 0, 0.0)
irr_used = prom.where(diffPct < 5.0, pd.concat([irr1, irr2], axis=1).max(axis=1)).clip(lower=0)

irr_df = base[["ts","ts_h"]].copy()
irr_df["irr_used"] = irr_used

hper_hour = irr_df.groupby("ts_h", as_index=False).agg(IrraActual=("irr_used","max"))
hper_hour = hper_hour.sort_values("ts_h")
hper_hour["IrraPrevia"] = hper_hour["IrraActual"].shift(1).fillna(0.0)
hper_hour["HEX_h"]  = np.where(hper_hour["IrraActual"].fillna(0) < UMBRAL_IRR, BLOQUE_H, 0.0)
hper_hour["HPER_h"] = (BLOQUE_H - hper_hour["HEX_h"]).round(2)

# --------------------------------- HAMA por hora (mantenimiento) --------------------------------
mant_rows = []
for inv in INVERSORES:
    m = base.assign(inversor=inv, en_mant=base["mant_set"].apply(lambda s: inv in s))
    h = m.groupby("ts_h", as_index=False)["en_mant"].max()
    h["HAMA_h"] = np.where(h["en_mant"], BLOQUE_H, 0.0)
    h["inversor"] = inv
    mant_rows.append(h[["ts_h","inversor","HAMA_h"]])
HAMA = pd.concat(mant_rows, ignore_index=True)

# ------------------------------ Energía neta horaria y HUNA ------------------------------------
ener_rows = []
for inv in INVERSORES:
    colE = f"{inv}_ACTIVE_ENERGY_SUPPLIED_(kWh)"
    sub = base[["ts","ts_h","inc_dict"]].copy()
    sub["inversor"] = inv
    sub["E_kWh"] = sub["inc_dict"].apply(lambda d, k=colE: get_num(d, k))

    lastE = (sub.sort_values("ts").groupby("ts_h", as_index=False)
                 .agg(inversor=("inversor","last"),
                      Eactu=("E_kWh","last")))
    lastE["inversor"] = inv
    lastE = lastE.sort_values("ts_h")
    lastE["Eprevi"] = lastE["Eactu"].shift(1).fillna(0.0)

    # Energía NETA por inversor-hora
    lastE["Eneta_kWh"] = (lastE["Eactu"] - lastE["Eprevi"]).clip(lower=0.0)

    # %oper previo (lo dejamos por compatibilidad con tu HUNA actual)
    pnom = PNOMINAL_KW[inv]
    esperado_kWh = max(pnom, 1e-9)
    lastE["porc_oper"] = (lastE["Eneta_kWh"] / esperado_kWh).clip(0.0, 1.0)

    lastE["HUNA1_h"] = np.where(lastE["Eneta_kWh"] > 0, 0.0, BLOQUE_H)
    lastE["HUNA2_h"] = (1.0 - lastE["porc_oper"]) * BLOQUE_H
    lastE.loc[lastE["HUNA1_h"] > 0, "HUNA2_h"] = 0.0

    ener_rows.append(lastE[[
        "ts_h","inversor","Eprevi","Eactu","Eneta_kWh","porc_oper","HUNA1_h","HUNA2_h"
    ]])

ENER = pd.concat(ener_rows, ignore_index=True)

# ------------------------------ Malla completa hora × inversor ---------------------------------
horas = hper_hour["ts_h"].drop_duplicates().sort_values()
grid = pd.MultiIndex.from_product([horas, INVERSORES], names=["ts_h","inversor"]).to_frame(index=False)

HUNA = ENER.copy()
HUNA["HUNA_h"] = HUNA["HUNA1_h"] + HUNA["HUNA2_h"]

all_df = (grid
          .merge(hper_hour[["ts_h","HPER_h","IrraActual","IrraPrevia"]], on="ts_h", how="left")
          .merge(HAMA[["ts_h","inversor","HAMA_h"]], on=["ts_h","inversor"], how="left")
          .merge(HUNA[["ts_h","inversor","HUNA_h","Eprevi","Eactu","Eneta_kWh","porc_oper"]],
                 on=["ts_h","inversor"], how="left"))

all_df[["HAMA_h","HUNA_h","porc_oper","Eprevi","Eactu","Eneta_kWh"]] = \
    all_df[["HAMA_h","HUNA_h","porc_oper","Eprevi","Eactu","Eneta_kWh"]].fillna(0.0)

# Enmascarar HAMA/HUNA dentro de HPER
all_df["hper"] = all_df["HPER_h"].fillna(0.0).round(2)
all_df["hama"] = np.minimum(all_df["HAMA_h"], all_df["hper"]).round(2)
all_df["huna"] = np.minimum(all_df["HUNA_h"], all_df["hper"]).round(2)

# Ainv (estilo Excel): 1 − HUNA  (en %)
all_df["Aninvi"] = ((1.0 - all_df["huna"]) * 100.0).clip(0,100).round(2)

# Pnominal (al 60%) y Psys
pnom_df = pd.DataFrame({"inversor": list(PNOMINAL_KW.keys()),
                        "Pnominal": list(PNOMINAL_KW.values())})
out = all_df.merge(pnom_df, on="inversor", how="left")

psys_hour = out.groupby("ts_h", as_index=False)["Pnominal"].sum().rename(columns={"Pnominal":"Psys"})
out = out.merge(psys_hour, on="ts_h", how="left")

# ------------------------------ ASYS por hora y Asys_inversor ----------------------------------
# Asys_inversor = (Pnominal * Aninvi/100) / Psys  (fracción por inversor y hora)
out["Asys_inversor"] = ((out["Pnominal"] * (out["Aninvi"] / 100.0)) / out["Psys"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

# ASYS = suma de Asys_inversor por hora * 100 (para dejarlo en %)
asys_df = out.groupby("ts_h", as_index=False)["Asys_inversor"].sum()
asys_df["ASYS"] = (asys_df["Asys_inversor"] * 100.0).clip(0,100).round(2)
asys_df = asys_df[["ts_h","ASYS"]]

# Unir ASYS recalculado
out = out.drop(columns=[c for c in ["ASYS"] if c in out.columns])
out = out.merge(asys_df, on="ts_h", how="left")

# id/ts representativos por hora
ids_ts = base.groupby("ts_h", as_index=False).agg(id=("id","first"), ts=("ts","first"))
out = out.merge(ids_ts, on="ts_h", how="left")

# Selección final
out = out.rename(columns={"IrraActual":"IrraActual","IrraPrevia":"IrraPrevia"})
out = out[[
    "id","ts","ts_h","inversor",
    "Eactu","Eprevi","Eneta_kWh",
    "IrraActual","IrraPrevia",
    "hper","hama","huna","porc_oper","Aninvi",
    "Asys_inversor","ASYS","Psys","Pnominal"
]].sort_values(["ts_h","inversor"])

# ------------------------------------ Guardar tabla única --------------------------------------
out.to_sql("Debug_Asys", engine, if_exists="replace", index=False)
print("Tabla única 'Debug_Asys' generada correctamente (incluye Asys_inversor y ASYS por suma).")
