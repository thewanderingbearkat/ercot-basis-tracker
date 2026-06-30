"""Log the Canadian Hills (SPP) summary to Snowflake so the read-only dashboard can serve it.

Canadian Hills is a CURTAILMENT story: the node basis is brutal, but the plant is switched
off ~60% of the time, so the dominant economic loss is curtailed VOLUME, not the price on
delivered MWh. We log yearly + monthly: ATC basis, GWA (delivered) basis, delivered MWh,
curtailed MWh, curtailment %, the avg hub price, and a rough value-of-curtailed-energy.

Writes SKYVEST.DBO.CM_CDNHILLS_YEARLY and CM_CDNHILLS_MONTHLY.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from constraint_map.db import query  # noqa: E402

NODE = "CANADIAN_HILLS"
HOURLY = os.path.join(os.path.dirname(__file__), "cdnhills_hourly.csv")
CURTAIL_XLSX = r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop\Anemoi_ProjectHourlyGeneration_v20260531.xlsx"

d = pd.read_csv(HOURLY, parse_dates=["HOUR"])
cur = pd.read_excel(CURTAIL_XLSX, sheet_name="CHW_Curtailment_monthly")
cur.columns = [str(c).strip() for c in cur.columns]
ccol = [c for c in cur.columns if "Curtail" in c][0]
cur["P"] = pd.to_datetime(cur["Month-Ending"]).dt.to_period("M")
cur_m = cur.set_index("P")[ccol]                       # curtailed MWh by month


def gwa(v, w):
    w = np.nan_to_num(np.asarray(w, float), nan=0.0).clip(min=0)
    v = np.asarray(v, float); ok = ~np.isnan(v)
    return float(np.average(v[ok], weights=w[ok])) if w[ok].sum() > 0 else float(np.nanmean(v))


def summarize(g, curt_mwh):
    deliv = g["GEN_MW"].sum()
    cpct = curt_mwh / (curt_mwh + deliv) if (deliv + curt_mwh) else np.nan
    return {"atc": g["BASIS"].mean(), "gwa": gwa(g["BASIS"], g["GEN_MW"]),
            "deliv": deliv, "curt": curt_mwh, "cpct": cpct,
            "hub": g["HUB"].mean(), "node": g["NODE"].mean(),
            "lost_val": curt_mwh * g["HUB"].mean()}


# NOTE: the Anemoi file's "Year" column is mislabeled for 2026 (every 2026-month row still
# says Year=2025 -- only "Month" rolled over). Derive the year from the reliable Month-Ending
# date so 2026 curtailment isn't silently absorbed into 2025.
cur["YR"] = pd.to_datetime(cur["Month-Ending"]).dt.year
d["P"] = d["HOUR"].dt.to_period("M")
d["yr"] = d["HOUR"].dt.year
yearly = {y: summarize(g, cur[cur["YR"] == y][ccol].sum()) for y, g in d.groupby("yr")}
monthly = {p: summarize(g, float(cur_m.get(p, np.nan))) for p, g in d.groupby("P")}

print("Canadian Hills yearly (ATC | GWA | delivGWh | curtGWh | curt% | lost$M @hub):")
for y, s in yearly.items():
    print(f"  {y}: {s['atc']:+6.1f} {s['gwa']:+6.1f} {s['deliv']/1e3:7.0f} {s['curt']/1e3:7.0f} "
          f"{100*s['cpct']:4.0f}% {s['lost_val']/1e6:6.1f}")


def _n(v): return "NULL" if (v is None or (isinstance(v, float) and np.isnan(v))) else round(float(v), 4)


if "--log" in sys.argv:
    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_CDNHILLS_YEARLY (
        NODE STRING, YR INT, ATC FLOAT, GWA FLOAT, DELIV_MWH FLOAT, CURT_MWH FLOAT,
        CURT_PCT FLOAT, HUB_PRICE FLOAT, NODE_PRICE FLOAT, LOST_VALUE FLOAT,
        LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")
    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_CDNHILLS_MONTHLY (
        NODE STRING, PERIOD DATE, ATC FLOAT, GWA FLOAT, DELIV_MWH FLOAT, CURT_MWH FLOAT,
        CURT_PCT FLOAT, LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")
    query(f"DELETE FROM SKYVEST.DBO.CM_CDNHILLS_YEARLY WHERE NODE='{NODE}'")
    query("INSERT INTO SKYVEST.DBO.CM_CDNHILLS_YEARLY (NODE,YR,ATC,GWA,DELIV_MWH,CURT_MWH,CURT_PCT,HUB_PRICE,NODE_PRICE,LOST_VALUE) VALUES " +
          ",".join(f"('{NODE}',{y},{_n(s['atc'])},{_n(s['gwa'])},{_n(s['deliv'])},{_n(s['curt'])},{_n(s['cpct'])},{_n(s['hub'])},{_n(s['node'])},{_n(s['lost_val'])})"
                   for y, s in yearly.items()))
    query(f"DELETE FROM SKYVEST.DBO.CM_CDNHILLS_MONTHLY WHERE NODE='{NODE}'")
    query("INSERT INTO SKYVEST.DBO.CM_CDNHILLS_MONTHLY (NODE,PERIOD,ATC,GWA,DELIV_MWH,CURT_MWH,CURT_PCT) VALUES " +
          ",".join(f"('{NODE}','{p.start_time.date()}',{_n(s['atc'])},{_n(s['gwa'])},{_n(s['deliv'])},{_n(s['curt'])},{_n(s['cpct'])})"
                   for p, s in monthly.items()))
    print(f"\nlogged {len(yearly)} yearly + {len(monthly)} monthly rows to Snowflake")
