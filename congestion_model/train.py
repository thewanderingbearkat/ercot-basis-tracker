"""NBOHR basis: quantile model (the statistical backbone) + OOD detector + a scenario
ladder. Trained out-of-time (train older, test recent) so the test number reflects real
generalization, not memorization. Each scenario returns a basis DISTRIBUTION and an
in-sample / out-of-sample label -- so the M&A deck shows the number AND how much to trust it."""
import os
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, IsolationForest
from sklearn.metrics import mean_absolute_error, r2_score

d = pd.read_csv(os.path.join(os.path.dirname(__file__), "nbohr_daily.csv"), parse_dates=["DAY"])
FEATURES = ["MID_TEMP", "MID_TEMP_MAX", "MID_WIND", "MID_WIND_MAX", "SA_WIND", "AB_WIND",
            "ERCOT_LOAD", "ERCOT_PEAK", "WEST_LOAD", "MONTH", "DOW"]
d = d.dropna(subset=["BASIS"] + FEATURES).reset_index(drop=True)
X, y = d[FEATURES], d["BASIS"]

cut = int(len(d) * 0.8)                       # out-of-TIME split
Xtr, Xte, ytr, yte = X.iloc[:cut], X.iloc[cut:], y.iloc[:cut], y.iloc[cut:]
print(f"{len(d)} days | train -> {d['DAY'].iloc[cut-1].date()} | test {d['DAY'].iloc[cut].date()} -> {d['DAY'].iloc[-1].date()} ({len(Xte)} days)")

QS = [0.1, 0.5, 0.9]
models = {}
for q in QS:
    m = GradientBoostingRegressor(loss="quantile", alpha=q, n_estimators=500,
                                  max_depth=3, learning_rate=0.03, subsample=0.8, random_state=0)
    models[q] = m.fit(Xtr, ytr)

p10, p50, p90 = (models[q].predict(Xte) for q in QS)
mae, r2 = mean_absolute_error(yte, p50), r2_score(yte, p50)
cov = np.mean((yte >= p10) & (yte <= p90))
naive = mean_absolute_error(yte, np.full(len(yte), ytr.mean()))
print(f"\nout-of-time test:  MAE {mae:.2f}  R2 {r2:.2f}  | 80% interval coverage {cov:.0%} (target 80%)")
print(f"   vs naive (predict train mean): MAE {naive:.2f}   -> {100*(1-mae/naive):.0f}% better")

imp = sorted(zip(FEATURES, models[0.5].feature_importances_), key=lambda t: -t[1])
print("top drivers:", ", ".join(f"{f} {v:.0%}" for f, v in imp[:6]))

# ---- OOD: per-feature support (catches load-over-max etc.) + multivariate novelty ----
iso = IsolationForest(n_estimators=300, random_state=0).fit(Xtr)
thresh = np.percentile(iso.score_samples(Xtr), 2)   # below 2nd pctile => novel combo
lo, hi = Xtr.quantile(0.005), Xtr.quantile(0.995)   # historical support per feature


def scenario(name, feat):
    x = pd.DataFrame([{**feat}])[FEATURES]
    q = {Q: models[Q].predict(x)[0] for Q in QS}
    oos = [c for c in FEATURES if x[c].iloc[0] < lo[c] or x[c].iloc[0] > hi[c]]
    if oos:
        flag = f"OUT-OF-SAMPLE [{', '.join(oos)}] -> structural"
    elif iso.score_samples(x)[0] < thresh:
        flag = "edge-of-sample (novel combo)"
    else:
        flag = "in-sample"
    print(f"  {name:30} basis q50 {q[0.5]:+6.1f}   [{q[0.1]:+5.1f}, {q[0.9]:+5.1f}]   {flag}")


recent = d.iloc[-90:][FEATURES].median().to_dict()
qn = lambda c, p: d[c].quantile(p)
print("\nscenario ladder (NBOHR daily basis $/MWh):")
scenario("status quo (recent median)", recent)
scenario("high West wind (p90)", {**recent, "MID_WIND": qn("MID_WIND", .9), "MID_WIND_MAX": qn("MID_WIND_MAX", .9),
                                   "SA_WIND": qn("SA_WIND", .9), "AB_WIND": qn("AB_WIND", .9)})
scenario("calm wind (p10)", {**recent, "MID_WIND": qn("MID_WIND", .1), "MID_WIND_MAX": qn("MID_WIND_MAX", .1),
                             "SA_WIND": qn("SA_WIND", .1), "AB_WIND": qn("AB_WIND", .1)})
scenario("summer peak (load+temp p95)", {**recent, "MID_TEMP": qn("MID_TEMP", .95), "MID_TEMP_MAX": qn("MID_TEMP_MAX", .95),
                                         "ERCOT_LOAD": qn("ERCOT_LOAD", .95), "ERCOT_PEAK": qn("ERCOT_PEAK", .97), "MONTH": 8})
scenario("load +15% over historic max", {**recent, "ERCOT_LOAD": d["ERCOT_LOAD"].max() * 1.15,
                                          "ERCOT_PEAK": d["ERCOT_PEAK"].max() * 1.15})
print("\n(the last rung trips OOD -> that's where you hand off to the structural shift-factor delta.)")
