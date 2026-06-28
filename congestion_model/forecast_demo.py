"""Forward demo: project NBOHR (Bearkat) basis 7 days out from an Xweather forecast.

This is the payoff of the co-located Xweather pull -- the model running FORWARD on data
you actually have ahead of time. Forecast-mode features only (no actual generation):
  * XW_WIND / XW_GUST / XW_GHI  -> Xweather 7-day hourly forecast at the node (1 API call)
  * ERCOT_LOAD / WEST_LOAD      -> month x hour climatology from history (load is predictable)
  * N_OUTAGE                    -> recent level (outages move slowly)
  * HOD / MONTH / DOW           -> calendar

Outputs a daily basis band + per-hour blowout probability + the largest drivers, and writes
forecast_demo.json for the visual. Prints a readable summary.
"""
import json
import os

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

import xweather_features as xw   # same dir; run from congestion_model/

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
qmodels = {q: HistGradientBoostingRegressor(loss="quantile", quantile=q, **HBG).fit(X, y) for q in (0.1, 0.5, 0.9)}
clf = HistGradientBoostingClassifier(**HBG).fit(X, (y < BLOWOUT).astype(int))
print(f"trained forecast-mode model on {len(d)} hrs (base blowout rate {(y < BLOWOUT).mean():.1%})")

# Largest drivers: how much blowout-prediction skill is lost when each input is shuffled.
imp = permutation_importance(clf, X, (y < BLOWOUT).astype(int), n_repeats=5, random_state=0,
                             scoring="roc_auc", n_jobs=-1)
drivers = sorted(({"feature": f, "label": LABEL[f], "importance": float(round(m, 4))}
                  for f, m in zip(FCAST, imp.importances_mean)), key=lambda r: -r["importance"])
print("largest drivers (AUC drop when shuffled):",
      ", ".join(f"{r['label']} {r['importance']:.3f}" for r in drivers[:5]))

# -------------------------------------------------------- forward feature frame
fc = xw.fetch_forecast(168)                       # Xweather 7-day hourly wind+GHI (1 call)
fc["HOD"], fc["MONTH"], fc["DOW"] = fc["HOUR"].dt.hour, fc["HOUR"].dt.month, fc["HOUR"].dt.dayofweek

clim = d.groupby(["MONTH", "HOD"])[["ERCOT_LOAD", "WEST_LOAD"]].median().reset_index()
fc = fc.merge(clim, on=["MONTH", "HOD"], how="left")
fc["N_OUTAGE"] = d.sort_values("HOUR").tail(30 * 24)["N_OUTAGE"].median()   # recent level

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
