"""Canadian Hills (SPP) core dataset: hourly basis + delivered generation + curtailment.

Node verified: CANADIAN_HILLS_1 = objid 10002511016 (all 5 PPAs settle at one LMP, ISO 'SI'
= SPP IM). Hub = SPPNORTH_HUB 10002511523. Basis = node RTLMP - hub RTLMP (RTCONG also kept).
Delivered hourly generation (sum of 5 PPA meters) from cdnhills_gen.csv; monthly economic
curtailment from the Anemoi file. Unlike McCrae, curtailment is the dominant economic
variable here (~2% in 2020 -> ~60% in 2024-25), so we carry it alongside basis.

Saves cdnhills_hourly.csv and prints the ATC vs GWA basis + curtailment headline by year.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from constraint_map.db import query, YES  # noqa: E402

NODE, HUB = 10002511016, 10002511524   # CANADIAN_HILLS_1 vs SPPSOUTH_HUB (CHW settles vs SPP South)
GEN_CSV = os.path.join(os.path.dirname(__file__), "cdnhills_gen.csv")
CURTAIL_XLSX = r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop\Anemoi_ProjectHourlyGeneration_v20260531.xlsx"

print("pulling Canadian Hills basis (node - SPP South hub) ...")
tgt = pd.DataFrame(query(f"""
    SELECT DATE_TRUNC('hour', DATETIME) AS HOUR,
           AVG(IFF(OBJECTID={NODE}, RTLMP, NULL)) AS NODE,
           AVG(IFF(OBJECTID={HUB},  RTLMP, NULL)) AS HUB,
           AVG(IFF(OBJECTID={NODE}, RTCONG, NULL)) AS NODE_CONG
    FROM {YES}.DART_PRICES
    WHERE OBJECTID IN ({NODE},{HUB}) AND RTLMP IS NOT NULL
      AND DATETIME >= '2019-01-01' AND DATETIME < CURRENT_DATE
    GROUP BY 1"""))
tgt["HOUR"] = pd.to_datetime(tgt["HOUR"])
for c in ("NODE", "HUB", "NODE_CONG"):
    tgt[c] = pd.to_numeric(tgt[c], errors="coerce")
tgt["BASIS"] = tgt["NODE"] - tgt["HUB"]

# Delivered hourly generation (sum of the 5 PPAs).
gen = pd.read_csv(GEN_CSV)
gen["HOUR"] = pd.to_datetime(gen["HOUR"], errors="coerce")
gen = gen.dropna(subset=["HOUR"]).drop_duplicates("HOUR")
d = tgt.merge(gen[["HOUR", "GEN_MW"]], on="HOUR", how="left")

# Co-located Xweather wind + solar (site forecast driver) -- backfilled by xweather_features.py.
xw_path = os.path.join(os.path.dirname(__file__), "cdnhills_xweather.csv")
if os.path.exists(xw_path):
    d = d.merge(pd.read_csv(xw_path, parse_dates=["HOUR"]), on="HOUR", how="left")

# Monthly economic curtailment (MWh) -> a per-hour curtailment RATE (the dominant variable here).
cur = pd.read_excel(CURTAIL_XLSX, sheet_name="CHW_Curtailment_monthly")
cur.columns = [str(c).strip() for c in cur.columns]
ccol = [c for c in cur.columns if "Curtail" in c][0]
cur_y = cur.groupby("Year")[ccol].sum()
cur["ym"] = pd.to_datetime(cur[[c for c in cur.columns if c == "Month-Ending"][0]]).dt.to_period("M")
d["ym"] = d["HOUR"].dt.to_period("M")
dgm = d.groupby("ym")["GEN_MW"].sum().rename("deliv_mwh")          # delivered MWh per month
cm = cur.set_index("ym")[ccol].rename("curt_mwh")
rate = (cm / (cm + dgm)).rename("CURT_RATE")
d = d.merge(rate, on="ym", how="left").drop(columns="ym")

d["HOD"], d["MONTH"], d["DOW"] = d["HOUR"].dt.hour, d["HOUR"].dt.month, d["HOUR"].dt.dayofweek
d = d.sort_values("HOUR").reset_index(drop=True)
d.to_csv(os.path.join(os.path.dirname(__file__), "cdnhills_hourly.csv"), index=False)
print(f"saved cdnhills_hourly.csv: {len(d)} hrs, cols {list(d.columns)}")


def gwa(v, w):
    w = np.nan_to_num(np.asarray(w, float), nan=0.0).clip(min=0)
    v = np.asarray(v, float)
    return float(np.average(v[~np.isnan(v)], weights=w[~np.isnan(v)])) if w[~np.isnan(v)].sum() > 0 else float(np.nanmean(v))


d["yr"] = d["HOUR"].dt.year
print(f"\nCanadian Hills basis (node - SPP South hub) by year:")
print(f"  {'yr':4}{'ATC':>8}{'GWA(deliv)':>11}{'deliv GWh':>11}{'curtail GWh':>12}{'curt%':>7}")
for y, g in d.groupby("yr"):
    deliv = g["GEN_MW"].sum() / 1000
    c = cur_y.get(y, np.nan) / 1000
    cpct = 100 * c / (c + deliv) if (deliv and not np.isnan(c)) else np.nan
    print(f"  {y:<4}{g['BASIS'].mean():>+8.2f}{gwa(g['BASIS'], g['GEN_MW']):>+11.2f}"
          f"{deliv:>11.0f}{c:>12.0f}{cpct:>6.0f}%")
print("\nNote: GWA on DELIVERED energy understates the hit -- ~60% of generation is curtailed")
print("before it can sell, so the dominant loss is volume (curtail GWh), not just price.")

dd = d.dropna(subset=["BASIS", "XW_WIND"])
qq = dd["XW_WIND"].quantile([.25, .75])
calm = dd[dd["XW_WIND"] <= qq.iloc[0]]["BASIS"].mean()
windy = dd[dd["XW_WIND"] >= qq.iloc[1]]["BASIS"].mean()
print(f"\ndriver check ({len(dd)} hrs): corr(basis, site wind) {dd['BASIS'].corr(dd['XW_WIND']):+.3f}")
print(f"  calm-hour basis {calm:+.1f} | windy-hour basis {windy:+.1f}  (spread {calm-windy:+.1f})")
print(f"  corr(basis, curtailment rate) {dd['BASIS'].corr(dd['CURT_RATE']):+.3f}  (curtailment vs price)")
