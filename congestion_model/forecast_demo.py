"""Forward demo: project NBOHR (Bearkat) basis 7 days out from an Xweather forecast.

This is the payoff of the co-located Xweather pull -- the model running FORWARD on data
you actually have ahead of time. Forecast-mode features only (no actual generation):
  * XW_WIND / XW_GUST / XW_GHI  -> Xweather 7-day hourly forecast at the node (1 API call)
  * ERCOT_LOAD / WEST_LOAD      -> Yes Energy LOAD_FORECASTS (ERCOT MTLF, hourly ~8d fwd;
                                   falls back to month x hour climatology past the horizon)
  * N_OUTAGE                    -> scheduled forward outages (planned windows are known ahead;
                                   falls back to the recent level)
  * HOD / MONTH / DOW           -> calendar

Outputs a daily basis band + per-hour blowout probability + the largest drivers, and writes
forecast_demo.json for the visual. Prints a readable summary.

With --log, also writes the run to Snowflake (SKYVEST.DBO.CM_CONGEST_FORECAST + _DRIVERS) so
the dashboard can track forecast-vs-realized over time and accumulate a track record. This
is the ONLY place the model touches Snowflake-write; the Flask app stays read-only.
"""
import json
import os
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

import xweather_features as xw   # same dir; run from congestion_model/

NODE = "NBOHR_RN"

HOURLY = os.path.join(os.path.dirname(__file__), "nbohr_hourly.csv")
OUT_JSON = os.path.join(os.path.dirname(__file__), "forecast_demo.json")
# Gust dropped: collinear with wind speed, so it only splits the wind signal in the
# driver ranking without adding forecastable information.
FCAST = ["XW_WIND", "XW_GHI", "ERCOT_LOAD", "WEST_LOAD", "N_OUTAGE", "HOD", "MONTH", "DOW"]
BLOWOUT = -20          # $/MWh basis threshold for "blowout"
HBG = dict(max_iter=600, learning_rate=0.04, max_depth=6, l2_regularization=1.0, random_state=0)
LABEL = {"XW_WIND": "West wind (fcst)", "XW_GUST": "Wind gust (fcst)", "XW_GHI": "Solar GHI (fcst)",
         "ERCOT_LOAD": "ERCOT load", "WEST_LOAD": "West load", "N_OUTAGE": "Tx outages",
         "HOD": "Hour of day", "MONTH": "Season", "DOW": "Day of week"}

# -------------------------------------------------------------- train (forecast mode)
d = pd.read_csv(HOURLY, parse_dates=["HOUR"]).dropna(subset=["BASIS"] + FCAST).reset_index(drop=True)
X, y = d[FCAST], d["BASIS"]
# Recency weighting (12-mo half-life): West-TX basis is deteriorating with renewable build-out,
# so weight recent hours and let the forecast reflect the current regime, not the 3-yr average.
w = 0.5 ** ((d["HOUR"].max() - d["HOUR"]).dt.days / 365.0)
qmodels = {q: HistGradientBoostingRegressor(loss="quantile", quantile=q, **HBG).fit(X, y, sample_weight=w) for q in (0.1, 0.5, 0.9)}
clf = HistGradientBoostingClassifier(**HBG).fit(X, (y < BLOWOUT).astype(int), sample_weight=w)
print(f"trained forecast-mode model on {len(d)} hrs (recency-weighted; base blowout rate {(y < BLOWOUT).mean():.1%})")

# Largest drivers: how much blowout-prediction skill is lost when each input is shuffled.
# n_jobs=1 deliberately: loky worker processes throw on shutdown under Windows Task
# Scheduler (non-zero exit after the work completes); 9 features x 5 repeats is fast serial.
imp = permutation_importance(clf, X, (y < BLOWOUT).astype(int), n_repeats=5, random_state=0,
                             scoring="roc_auc", n_jobs=1)
drivers = sorted(({"feature": f, "label": LABEL[f], "importance": float(round(m, 4))}
                  for f, m in zip(FCAST, imp.importances_mean)), key=lambda r: -r["importance"])
print("largest drivers (AUC drop when shuffled):",
      ", ".join(f"{r['label']} {r['importance']:.3f}" for r in drivers[:5]))

# -------------------------------------------------------- forward feature frame
fc = xw.fetch_forecast(168)                       # Xweather 7-day hourly wind+GHI (1 call)
fc["HOD"], fc["MONTH"], fc["DOW"] = fc["HOUR"].dt.hour, fc["HOUR"].dt.month, fc["HOUR"].dt.dayofweek

# Real forwards from Yes Energy (the Jul-5 miss review showed persisted shapes carry no event
# info): ERCOT MTLF hourly load forecast + the scheduled forward outage count. Climatology /
# recent-level only as fallback (offline, or hours past the forecast horizon).
clim = d.groupby(["MONTH", "HOD"])[["ERCOT_LOAD", "WEST_LOAD"]].median().reset_index()
fc = fc.merge(clim.rename(columns={"ERCOT_LOAD": "_ERCOT_CLIM", "WEST_LOAD": "_WEST_CLIM"}),
              on=["MONTH", "HOD"], how="left")
fc["_OUT_RECENT"] = d.sort_values("HOUR").tail(30 * 24)["N_OUTAGE"].median()


def _forward_from_snowflake(fc):
    """ERCOT/WEST hourly load forecast (latest publish per target hour) + scheduled daily
    outage count over the fc window. Returns (load_df|None, out_df|None)."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
    from constraint_map.db import YES, query
    lo, hi = fc["HOUR"].min(), fc["HOUR"].max()
    ld = pd.DataFrame(query(f"""
        SELECT DATETIME, OBJECTID, LOAD FROM {YES}.LOAD_FORECASTS
        WHERE OBJECTID IN (10000712973, 10000712971) AND FORECASTTYPE = 'LF'
          AND DATETIME BETWEEN '{lo:%Y-%m-%d %H:%M}' AND '{hi:%Y-%m-%d %H:%M}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY OBJECTID, DATETIME ORDER BY PUBLISHDATE DESC) = 1"""))
    load = None
    if len(ld):
        ld["DATETIME"] = pd.to_datetime(ld["DATETIME"])
        load = ld.pivot_table(index="DATETIME", columns="OBJECTID", values="LOAD").reset_index()
        load.columns = ["HOUR"] + ["ERCOT_LOAD" if c == 10000712973 else "WEST_LOAD" for c in load.columns[1:]]
    # scheduled outages: same LN/XF + real-end rules as build_hourly, planned windows forward
    ot = pd.DataFrame(query(f"""
        WITH days AS (SELECT DATEADD('day', SEQ4(), '{lo:%Y-%m-%d}'::DATE)::DATE D
                      FROM TABLE(GENERATOR(ROWCOUNT=>9))),
        o AS (SELECT TICKETID, COALESCE(STARTDATE, PLANNED_STARTDATE)::DATE s,
                     COALESCE(ENDDATE, PLANNED_ENDDATE)::DATE e
              FROM {YES}.ERCOT_OUTAGES
              WHERE STATUS IN ('Apprv','Accpt') AND VOLTAGELEVEL >= 138
                AND EQUIPMENTTYPE IN ('LN','XF')
                AND COALESCE(STARTDATE, PLANNED_STARTDATE) IS NOT NULL
                AND COALESCE(ENDDATE, PLANNED_ENDDATE) IS NOT NULL
                AND DATEDIFF('day', COALESCE(STARTDATE, PLANNED_STARTDATE),
                             COALESCE(ENDDATE, PLANNED_ENDDATE)) BETWEEN 0 AND 365)
        SELECT d.D AS DAY, COUNT(DISTINCT o.TICKETID) N_OUTAGE
        FROM days d JOIN o ON d.D BETWEEN o.s AND o.e GROUP BY d.D"""))
    out = None
    if len(ot):
        ot["DAY"] = pd.to_datetime(ot["DAY"]).dt.date
        out = ot
    return load, out


try:
    _load_fwd, _out_fwd = _forward_from_snowflake(fc)
except Exception as e:
    print(f"forward Snowflake pull failed ({e}); using climatology/recent-level fallbacks")
    _load_fwd, _out_fwd = None, None

if _load_fwd is not None:
    fc = fc.merge(_load_fwd, on="HOUR", how="left")
    n_real = fc["ERCOT_LOAD"].notna().sum()
    print(f"load forecast (MTLF): {n_real}/{len(fc)} fwd hours real, rest climatology")
else:
    fc["ERCOT_LOAD"], fc["WEST_LOAD"] = np.nan, np.nan
fc["ERCOT_LOAD"] = fc["ERCOT_LOAD"].fillna(fc["_ERCOT_CLIM"])
fc["WEST_LOAD"] = fc["WEST_LOAD"].fillna(fc["_WEST_CLIM"])
if _out_fwd is not None:
    fc["N_OUTAGE"] = fc["HOUR"].dt.date.map(dict(zip(_out_fwd["DAY"], _out_fwd["N_OUTAGE"])))
    print(f"scheduled outages: {fc['N_OUTAGE'].notna().sum()}/{len(fc)} fwd hours real, rest recent level")
else:
    fc["N_OUTAGE"] = np.nan
fc["N_OUTAGE"] = fc["N_OUTAGE"].fillna(fc["_OUT_RECENT"])
fc = fc.drop(columns=["_ERCOT_CLIM", "_WEST_CLIM", "_OUT_RECENT"])

Xf = fc[FCAST]
fc["q10"], fc["q50"], fc["q90"] = (qmodels[q].predict(Xf) for q in (0.1, 0.5, 0.9))
fc["p_blowout"] = clf.predict_proba(Xf)[:, 1]

# ----------------------------------------------------------------- daily roll-up
day = fc.assign(DATE=fc["HOUR"].dt.date).groupby("DATE").agg(
    q10=("q10", "mean"), q50=("q50", "mean"), q90=("q90", "mean"),
    worst_q10=("q10", "min"), p_blowout=("p_blowout", "mean"),
    peak_risk_hr=("p_blowout", "idxmax")).reset_index()
day["peak_risk_hr"] = fc.loc[day["peak_risk_hr"], "HOUR"].dt.strftime("%a %H:00").values

print("\n7-day NBOHR basis forecast ($/MWh):")
print(f"{'date':<12}{'q50':>7}{'q10..q90':>16}{'worst hr q10':>14}{'blowout%':>10}  peak-risk hr")
for _, r in day.iterrows():
    print(f"{str(r['DATE']):<12}{r['q50']:>+7.1f}   [{r['q10']:>+5.1f},{r['q90']:>+5.1f}]"
          f"{r['worst_q10']:>+13.1f}{r['p_blowout']*100:>9.0f}%  {r['peak_risk_hr']}")

# ----------------------------------------------------------------- emit JSON for the visual
payload = {
    "generated_for": f"{fc['HOUR'].min():%Y-%m-%d %H:00} .. {fc['HOUR'].max():%Y-%m-%d %H:00} (CST)",
    "blowout_threshold": BLOWOUT,
    "base_rate": float(round((y < BLOWOUT).mean(), 4)),
    "drivers": drivers,
    "hourly": [{"t": h.strftime("%Y-%m-%dT%H:00"), "q10": round(a, 1), "q50": round(b, 1),
                "q90": round(c, 1), "wind": round(w, 1), "ghi": round(g, 0), "p": round(p, 3)}
               for h, a, b, c, w, g, p in zip(fc["HOUR"], fc["q10"], fc["q50"], fc["q90"],
                                              fc["XW_WIND"], fc["XW_GHI"], fc["p_blowout"])],
    "daily": [{"date": str(r["DATE"]), "q10": round(r["q10"], 1), "q50": round(r["q50"], 1),
               "q90": round(r["q90"], 1), "worst_q10": round(r["worst_q10"], 1),
               "p_blowout": round(r["p_blowout"], 3), "peak_risk_hr": r["peak_risk_hr"]}
              for _, r in day.iterrows()],
}
with open(OUT_JSON, "w") as f:
    json.dump(payload, f, indent=2)
print(f"\nwrote {OUT_JSON}")


def _num(x, fmt):
    return "NULL" if pd.isna(x) else format(x, fmt)


def log_to_snowflake(fc, drivers):
    """Persist this run's hourly forecast + drivers to Snowflake (idempotent per RUN_DATE).
    Re-running the same day replaces that day's rows."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
    from constraint_map.db import query   # SKYVEST_READ can write SKYVEST.DBO.* (see staging/stage.py)

    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_CONGEST_FORECAST (
        NODE STRING, RUN_DATE DATE, TARGET_HOUR TIMESTAMP_NTZ, LEAD_H INT,
        Q10 FLOAT, Q50 FLOAT, Q90 FLOAT, P_BLOWOUT FLOAT,
        XW_WIND FLOAT, XW_GHI FLOAT, ERCOT_LOAD FLOAT, WEST_LOAD FLOAT, N_OUTAGE FLOAT,
        LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")
    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_CONGEST_DRIVERS (
        NODE STRING, RUN_DATE DATE, FEATURE STRING, LABEL STRING, IMPORTANCE FLOAT,
        LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")

    origin = fc["HOUR"].min()
    run_date = origin.date()
    query(f"DELETE FROM SKYVEST.DBO.CM_CONGEST_FORECAST WHERE NODE='{NODE}' AND RUN_DATE='{run_date}'")
    query(f"DELETE FROM SKYVEST.DBO.CM_CONGEST_DRIVERS  WHERE NODE='{NODE}' AND RUN_DATE='{run_date}'")

    rows = []
    for _, r in fc.iterrows():
        lead = int((r["HOUR"] - origin).total_seconds() // 3600)
        rows.append(
            f"('{NODE}','{run_date}','{r['HOUR']:%Y-%m-%d %H:%M:%S}',{lead},"
            f"{_num(r['q10'], '.2f')},{_num(r['q50'], '.2f')},{_num(r['q90'], '.2f')},{_num(r['p_blowout'], '.4f')},"
            f"{_num(r['XW_WIND'], '.1f')},{_num(r['XW_GHI'], '.0f')},{_num(r['ERCOT_LOAD'], '.0f')},"
            f"{_num(r['WEST_LOAD'], '.0f')},{_num(r['N_OUTAGE'], '.1f')})")
    query("INSERT INTO SKYVEST.DBO.CM_CONGEST_FORECAST "
          "(NODE,RUN_DATE,TARGET_HOUR,LEAD_H,Q10,Q50,Q90,P_BLOWOUT,XW_WIND,XW_GHI,ERCOT_LOAD,WEST_LOAD,N_OUTAGE) "
          "VALUES " + ",".join(rows))

    drows = [f"('{NODE}','{run_date}','{d['feature']}','{d['label']}',{d['importance']:.4f})" for d in drivers]
    query("INSERT INTO SKYVEST.DBO.CM_CONGEST_DRIVERS (NODE,RUN_DATE,FEATURE,LABEL,IMPORTANCE) "
          "VALUES " + ",".join(drows))
    print(f"logged {len(rows)} forecast hrs + {len(drows)} drivers to Snowflake (RUN_DATE {run_date})")


if __name__ == "__main__" and "--log" in sys.argv:
    log_to_snowflake(fc, drivers)
