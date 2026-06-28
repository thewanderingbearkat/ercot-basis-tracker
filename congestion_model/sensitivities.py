"""Derive NBOHR basis sensitivities to each large driver -- the coefficients behind the
dashboard scenario calculator. Run when the data updates; transcribe the printed numbers
into structural.py SENSITIVITIES (the app reads them from there).

Two regimes, because they differ and the difference IS the story:
  * ATC (all hours)        -- effect on the around-the-clock average basis.
  * congested / GWA proxy  -- effect in the windy (top wind-quartile) hours, where the
                              export GTC binds and where the wind plant actually generates,
                              so this is the wind-REVENUE sensitivity.
A driver that barely moves ATC can strongly move the revenue number (load is the classic case).
"""
import os

import numpy as np
import pandas as pd

d = pd.read_csv(os.path.join(os.path.dirname(__file__), "nbohr_hourly.csv"))
d = d.dropna(subset=["BASIS", "WN_WIND", "ERCOT_SOLAR", "WEST_LOAD", "ERCOT_LOAD", "N_OUTAGE"])
for c in ["WN_WIND", "ERCOT_SOLAR", "WEST_LOAD", "ERCOT_LOAD"]:
    d[c + "_gw"] = d[c] / 1000.0
FEATS = ["WN_WIND_gw", "ERCOT_SOLAR_gw", "WEST_LOAD_gw", "ERCOT_LOAD_gw", "N_OUTAGE"]


def coefs(sub):
    X = np.column_stack([np.ones(len(sub))] + [sub[f].values for f in FEATS])
    b, *_ = np.linalg.lstsq(X, sub["BASIS"].values, rcond=None)
    return b[1:]


allh = coefs(d)
windy = coefs(d[d["WN_WIND"] >= d["WN_WIND"].quantile(0.75)])
atc_base = d["BASIS"].mean()
gwa_base = np.average(d["BASIS"], weights=d["WN_WIND"].clip(lower=0))
wind_base = d[d["WN_WIND"] >= d["WN_WIND"].quantile(0.75)]["BASIS"].mean()

print(f"baselines: ATC {atc_base:+.2f}  GWA {gwa_base:+.2f}  congested(windy) {wind_base:+.2f}  $/MWh")
print(f"\nmarginal sensitivity ($/MWh per GW, per outage):")
print(f"  {'driver':16}{'ATC (all hrs)':>15}{'congested/GWA':>15}")
for i, f in enumerate(FEATS):
    print(f"  {f.replace('_gw',''):16}{allh[i]:>+15.2f}{windy[i]:>+15.2f}")
print("\nstructural (physics, out-of-sample):")
print("  GTC shadow price : NBOHR basis = -(shadow x 0.267 rel-shift-factor)")
print("  new West load    : = the WEST_LOAD congested slope (~+1.1/GW); ~6.8 GW flips windy -7 -> 0")
