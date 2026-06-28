"""NBOHR basis: quantile model (statistical backbone) + OOD detector + scenario ladder.
Hourly resolution with wind generation + outages. Trained out-of-time (older->recent) so
the test number reflects real generalization. Each scenario returns a basis DISTRIBUTION
and an in-sample / out-of-sample label (-> hand to the structural shift-factor delta)."""
import os
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, IsolationForest
from sklearn.metrics import mean_absolute_error, r2_score, roc_auc_score

HOURLY = os.path.join(os.path.dirname(__file__), "nbohr_hourly.csv")
d = pd.read_csv(HOURLY, parse_dates=["HOUR"])
FEATURES = ["WN_WIND", "ERCOT_SOLAR", "MID_TEMP", "MID_WIND", "SA_WIND",
            "ERCOT_LOAD", "WEST_LOAD", "N_OUTAGE", "HOD", "MONTH", "DOW",
            "XW_WIND", "XW_GUST", "XW_GHI"]   # co-located, forecastable -> forward-runnable
d = d.dropna(subset=["BASIS"] + FEATURES).reset_index(drop=True)
X, y = d[FEATURES], d["BASIS"]

cut = int(len(d) * 0.8)                       # out-of-TIME split
Xtr, Xte, ytr, yte = X.iloc[:cut], X.iloc[cut:], y.iloc[:cut], y.iloc[cut:]
print(f"{len(d)} hours | train -> {d['HOUR'].iloc[cut-1].date()} | test {d['HOUR'].iloc[cut].date()} -> {d['HOUR'].iloc[-1].date()} ({len(Xte)} hrs)")

QS = [0.1, 0.5, 0.9]
models = {q: HistGradientBoostingRegressor(loss="quantile", quantile=q, max_iter=600,
          learning_rate=0.04, max_depth=6, l2_regularization=1.0, random_state=0).fit(Xtr, ytr) for q in QS}

p10, p50, p90 = (models[q].predict(Xte) for q in QS)
mae, r2 = mean_absolute_error(yte, p50), r2_score(yte, p50)
cov = np.mean((yte >= p10) & (yte <= p90))
naive = mean_absolute_error(yte, np.full(len(yte), ytr.mean()))
print(f"\nout-of-time test:  MAE {mae:.2f}  R2 {r2:.2f}  | 80% interval coverage {cov:.0%} (target 80%)")
print(f"   vs naive (train mean) MAE {naive:.2f}  -> {100*(1-mae/naive):.0f}% better")
# R2 is a poor yardstick for a spike-dominated target. What matters for M&A: does the
# model's distribution flag blowout-RISK hours? Score the down-tail as a risk signal.
def pinball(yt, pr, q): e = yt - pr; return np.mean(np.maximum(q * e, (q - 1) * e))
pin = np.mean([pinball(yte, models[q].predict(Xte), q) for q in QS])
for thr in (-20, -50):
    yb = (yte < thr).astype(int)
    if yb.sum() >= 10:
        auc = roc_auc_score(yb, -p10)            # lower q10 => higher blowout risk
        print(f"   blowout discrimination (basis < {thr}): AUC {auc:.2f}  (base rate {yb.mean():.1%})")
print(f"   pinball loss (lower=better calibrated distribution): {pin:.2f}")

# permutation-free importance: drop-one-feature degradation on the median model is slow;
# use the model's native importance proxy via partial corr of feature with residual sign.
perm = []
base = mean_absolute_error(yte, p50)
rng = np.random.default_rng(0)
for f in FEATURES:
    Xp = Xte.copy(); Xp[f] = rng.permutation(Xp[f].values)
    perm.append((f, mean_absolute_error(yte, models[0.5].predict(Xp)) - base))
print("top drivers (MAE rise when shuffled):", ", ".join(f"{f} +{v:.2f}" for f, v in sorted(perm, key=lambda t: -t[1])[:6]))

# FORECAST MODE: actual generation (WN_WIND/ERCOT_SOLAR) and Yes-Energy actual weather are
# NOT known forward. Re-score blowout skill using only forecastable inputs -- the Xweather
# wind/GHI forecast + load forecast + planned outages + calendar. This is the real
# forward-run capability (the whole reason we pulled co-located Xweather).
FCAST = ["XW_WIND", "XW_GUST", "XW_GHI", "ERCOT_LOAD", "WEST_LOAD", "N_OUTAGE", "HOD", "MONTH", "DOW"]
fm = HistGradientBoostingRegressor(loss="quantile", quantile=0.1, max_iter=600,
     learning_rate=0.04, max_depth=6, l2_regularization=1.0, random_state=0).fit(Xtr[FCAST], ytr)
fp10 = fm.predict(Xte[FCAST])
print("forecast-mode (forecastable inputs only -- no actual generation):")
for thr in (-20, -50):
    yb = (yte < thr).astype(int)
    if yb.sum() >= 10:
        print(f"   blowout discrimination (basis < {thr}): AUC {roc_auc_score(yb, -fp10):.2f}")

iso = IsolationForest(n_estimators=300, random_state=0).fit(Xtr)
thresh = np.percentile(iso.score_samples(Xtr), 2)
lo, hi = X.quantile(0.005), X.quantile(0.995)   # support vs ALL history (not just train)


def scenario(name, feat):
    x = pd.DataFrame([{**recent, **feat}])[FEATURES]
    q = {Q: models[Q].predict(x)[0] for Q in QS}
    oos = [c for c in FEATURES if x[c].iloc[0] < lo[c] or x[c].iloc[0] > hi[c]]
    flag = (f"OUT-OF-SAMPLE [{', '.join(oos)}] -> structural" if oos
            else "edge-of-sample" if iso.score_samples(x)[0] < thresh else "in-sample")
    print(f"  {name:30} basis q50 {q[0.5]:+6.1f}  [{q[0.1]:+6.1f}, {q[0.9]:+6.1f}]   {flag}")


recent = d.iloc[-720:][FEATURES].median().to_dict()
qn = lambda c, p: d[c].quantile(p)
print("\nscenario ladder (NBOHR hourly basis $/MWh):")
scenario("status quo (recent median hr)", {})
scenario("high West wind, 3am (blowout)", {"WN_WIND": qn("WN_WIND", .95), "MID_WIND": qn("MID_WIND", .9),
          "SA_WIND": qn("SA_WIND", .9), "XW_WIND": qn("XW_WIND", .95), "XW_GUST": qn("XW_GUST", .95),
          "ERCOT_SOLAR": 0, "XW_GHI": 0, "HOD": 3, "ERCOT_LOAD": qn("ERCOT_LOAD", .3)})
scenario("calm wind, summer peak 5pm", {"WN_WIND": qn("WN_WIND", .1), "MID_WIND": qn("MID_WIND", .1),
          "SA_WIND": qn("SA_WIND", .1), "XW_WIND": qn("XW_WIND", .1), "XW_GUST": qn("XW_GUST", .1),
          "MID_TEMP": qn("MID_TEMP", .98), "XW_GHI": qn("XW_GHI", .9), "ERCOT_LOAD": qn("ERCOT_LOAD", .98),
          "HOD": 17, "MONTH": 8})
scenario("high wind + big outage", {"WN_WIND": qn("WN_WIND", .9), "XW_WIND": qn("XW_WIND", .9),
          "XW_GUST": qn("XW_GUST", .9), "N_OUTAGE": qn("N_OUTAGE", .98), "HOD": 3})
scenario("load +15% over historic max", {"ERCOT_LOAD": d["ERCOT_LOAD"].max() * 1.15})
print("\n(OOD rungs hand off to the structural shift-factor delta.)")
