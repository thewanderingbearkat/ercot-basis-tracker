"""Multi-horizon BUDGET forecast for NBOHR basis -- monthly expected basis out to 3 years.

Short term (7d) is the weather-forecast-driven demo (forecast_demo.py). Beyond a week there
is no usable weather forecast, so the medium (3mo) and long (3y) horizons run the SAME
forecast-mode model over the CLIMATOLOGICAL distribution of conditions for each calendar
month -- i.e. "a normal weather year at today's grid." That gives a budgeting-grade monthly
basis: expected $/MWh, a P10-P90 range, and an expected blowout frequency.

Design choice (matches the statistical + structural split): the climatology is the
weather-normal BASELINE. Year-specific structural changes (load growth behind the node, a
new line, a new big load) are layered on top via structural.py / the dashboard calculator --
NOT baked into the statistical baseline. An optional --growth knob scales load for a crude
system-wide trend, default 0 (pure climatology).

    python horizon_forecast.py            # print the 36-month curve
    python horizon_forecast.py --log      # also write SKYVEST.DBO.CM_CONGEST_BUDGET
    python horizon_forecast.py --growth 0.03 --log
"""
import os
import sys

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))   # Snowflake creds for --log

NODE = "NBOHR_RN"
HOURLY = os.path.join(os.path.dirname(__file__), "nbohr_hourly.csv")
FCAST = ["XW_WIND", "XW_GHI", "ERCOT_LOAD", "WEST_LOAD", "N_OUTAGE", "HOD", "MONTH", "DOW"]
BLOWOUT = -20
HBG = dict(max_iter=500, learning_rate=0.04, max_depth=6, l2_regularization=1.0, random_state=0)
MONTHS = 36

growth = 0.0
if "--growth" in sys.argv:
    growth = float(sys.argv[sys.argv.index("--growth") + 1])

d = pd.read_csv(HOURLY, parse_dates=["HOUR"]).dropna(subset=["BASIS"] + FCAST).reset_index(drop=True)
# Actual McCrae generation (HARALD_UNIT1) for the GWA (wind-revenue) weighting -- build_gen.py.
_gpath = os.path.join(os.path.dirname(__file__), "nbohr_gen.csv")
if os.path.exists(_gpath):
    d = d.merge(pd.read_csv(_gpath, parse_dates=["HOUR"])[["HOUR", "GEN_MW"]], on="HOUR", how="left")
else:
    d["GEN_MW"] = np.nan
    print("  NOTE: nbohr_gen.csv missing -- GWA will fall back to unweighted; run build_gen.py")
X, y = d[FCAST], d["BASIS"]
# Recency weighting (12-month half-life): West-TX basis is deteriorating as renewables build
# out -- pooled climatology runs ~$1.8/MWh too optimistic on GWA vs the last 12 months. Weight
# recent hours so the budget reflects the CURRENT regime, not the 3-year average.
age_days = (d["HOUR"].max() - d["HOUR"]).dt.days
w = 0.5 ** (age_days / 365.0)
qm = {q: HistGradientBoostingRegressor(loss="quantile", quantile=q, **HBG).fit(X, y, sample_weight=w) for q in (0.1, 0.5, 0.9)}
# Mean model (default squared-error loss) for the EXPECTED value -- the q50 median understates
# the mean when basis has a fat negative tail, which is exactly the GWA case. Budget on the mean.
mean_model = HistGradientBoostingRegressor(**HBG).fit(X, y, sample_weight=w)
clf = HistGradientBoostingClassifier(**HBG).fit(X, (y < BLOWOUT).astype(int), sample_weight=w)
# Empirical realized ATC basis by calendar month (model-free cross-check). ATC = around-the-
# clock (flat hourly average). GWA (generation-weighted) is site-specific -- a later layer.
def _gw(vals, gen):
    """Generation-weighted average (wind-revenue / GWA); falls back to mean if no gen."""
    gw = np.nan_to_num(np.asarray(gen, float), nan=0.0).clip(min=0)
    return float(np.average(vals, weights=gw)) if gw.sum() > 0 else float(np.mean(vals))


hist_by_month = d.groupby("MONTH")["BASIS"].mean().to_dict()                                  # ATC realized
hist_gwa_by_month = {int(m): _gw(g_["BASIS"], g_["GEN_MW"]) for m, g_ in d.groupby("MONTH")}  # GWA realized
print(f"trained on {len(d)} hrs | ATC + GWA (actual HARALD gen) | load growth: {growth:.1%}/yr")

start = pd.Timestamp.now().normalize().replace(day=1)
rows = []
for k in range(1, MONTHS + 1):
    period = start + pd.DateOffset(months=k - 1)
    cal_month = period.month
    analog = d[d["MONTH"] == cal_month].copy()                 # climatological conditions for that month
    if growth:
        scale = (1 + growth) ** (k / 12.0)
        analog["ERCOT_LOAD"] *= scale
        analog["WEST_LOAD"] *= scale
    Xa = analog[FCAST]
    p10p, p50p, p90p = qm[0.1].predict(Xa), qm[0.5].predict(Xa), qm[0.9].predict(Xa)
    meanp = mean_model.predict(Xa)                              # expected value (mean, not median)
    ga = analog["GEN_MW"]                                       # actual gen shape for this month
    row = {"period": period, "k": k,
           "p10": float(np.mean(p10p)), "p50": float(np.mean(p50p)), "p90": float(np.mean(p90p)),
           "expected": float(np.mean(meanp)),
           "gwa_p10": _gw(p10p, ga), "gwa_p50": _gw(p50p, ga), "gwa_p90": _gw(p90p, ga),
           "expected_gwa": _gw(meanp, ga),
           "blowout_pct": float(np.mean(clf.predict_proba(Xa)[:, 1]) * 100),
           "hist": float(hist_by_month.get(cal_month, np.nan)),
           "hist_gwa": float(hist_gwa_by_month.get(cal_month, np.nan))}
    rows.append(row)

print("\nNBOHR monthly basis budget ($/MWh) -- climatology:")
print(f"{'month':<9}{'ATC exp':>9}{'GWA exp':>9}{'GWA hist':>10}{'blowout%':>10}")
for r in rows[:12]:
    print(f"{r['period']:%Y-%m}  {r['expected']:>+7.1f}{r['expected_gwa']:>+9.1f}{r['hist_gwa']:>+10.1f}{r['blowout_pct']:>9.0f}%")
print(f"... ({MONTHS} months total)")
yr = pd.DataFrame(rows); yr["year"] = yr["period"].dt.year
print("\nannual mean basis  (ATC vs GWA = wind revenue):")
for y_, g in yr.groupby("year"):
    print(f"  {y_}: ATC {g['expected'].mean():+.2f}   GWA {g['expected_gwa'].mean():+.2f} $/MWh   (blowout {g['blowout_pct'].mean():.0f}%)")


def log_to_snowflake(rows):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
    from constraint_map.db import query
    # Schema gained GWA columns -- recreate (the budget is fully regenerated each run).
    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_CONGEST_BUDGET (
        NODE STRING, RUN_DATE DATE, PERIOD DATE, MONTHS_AHEAD INT,
        P10 FLOAT, P50 FLOAT, P90 FLOAT, EXPECTED FLOAT, BLOWOUT_PCT FLOAT, HIST_BASIS FLOAT,
        GWA_P10 FLOAT, GWA_P50 FLOAT, GWA_P90 FLOAT, EXPECTED_GWA FLOAT, HIST_GWA FLOAT,
        GROWTH FLOAT, LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")
    for col in ("GWA_P10", "GWA_P50", "GWA_P90", "EXPECTED_GWA", "HIST_GWA"):
        query(f"ALTER TABLE SKYVEST.DBO.CM_CONGEST_BUDGET ADD COLUMN IF NOT EXISTS {col} FLOAT")
    run_date = start.date()
    query(f"DELETE FROM SKYVEST.DBO.CM_CONGEST_BUDGET WHERE NODE='{NODE}' AND RUN_DATE='{run_date}'")
    nz = lambda v: "NULL" if pd.isna(v) else round(v, 2)
    vals = ",".join(
        f"('{NODE}','{run_date}','{r['period']:%Y-%m-%d}',{r['k']},{r['p10']:.2f},{r['p50']:.2f},"
        f"{r['p90']:.2f},{r['expected']:.2f},{r['blowout_pct']:.1f},{nz(r['hist'])},"
        f"{r['gwa_p10']:.2f},{r['gwa_p50']:.2f},{r['gwa_p90']:.2f},{r['expected_gwa']:.2f},{nz(r['hist_gwa'])},"
        f"{growth})" for r in rows)
    query("INSERT INTO SKYVEST.DBO.CM_CONGEST_BUDGET "
          "(NODE,RUN_DATE,PERIOD,MONTHS_AHEAD,P10,P50,P90,EXPECTED,BLOWOUT_PCT,HIST_BASIS,"
          "GWA_P10,GWA_P50,GWA_P90,EXPECTED_GWA,HIST_GWA,GROWTH) VALUES " + vals)
    print(f"\nlogged {len(rows)} monthly budget rows (ATC+GWA) to Snowflake (RUN_DATE {run_date})")


if "--log" in sys.argv:
    log_to_snowflake(rows)
