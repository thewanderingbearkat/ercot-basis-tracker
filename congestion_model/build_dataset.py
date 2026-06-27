"""Assemble the daily NBOHR (Bearkat) congestion/basis training table.

Target  : daily-avg node basis (NBOHR RTLMP - HB_WEST RTLMP) -- ERCOT is ~lossless,
          so this is the congestion basis the M&A desk cares about.
Features: forecastable drivers only (so the model doubles as a scenario engine):
          West-TX weather (wind = proxy for West wind gen -> export congestion; temp
          = load), ERCOT + West-zone load, Waha gas, calendar. Outages/structural
          deltas come in later layers.

Days are sliced on fixed CST (matches the rest of the stack). Saves nbohr_daily.csv.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from constraint_map.db import query  # noqa: E402

Y = "YES_ENERGY__FULL_DATASET.YESDATA"
NBOHR, HB_WEST = 10004202409, 10000697080
ERCOT_LOAD, WEST_LOAD = 10000712973, 10000712971
CST = "CONVERT_TIMEZONE('America/Chicago','Etc/GMT+6', DATETIME)::DATE"
WIN = "DATETIME >= DATEADD('year',-3,CURRENT_DATE) AND DATETIME < CURRENT_DATE"


def df(rows):
    return pd.DataFrame(rows)


print("pulling target (NBOHR basis) ...")
tgt = df(query(f"""
    SELECT {CST} AS DAY,
           AVG(CASE WHEN OBJECTID={NBOHR}   THEN RTLMP END) AS NODE,
           AVG(CASE WHEN OBJECTID={HB_WEST} THEN RTLMP END) AS HUB
    FROM {Y}.DART_PRICES
    WHERE OBJECTID IN ({NBOHR},{HB_WEST}) AND RTLMP IS NOT NULL AND {WIN}
    GROUP BY 1"""))
tgt["BASIS"] = tgt["NODE"] - tgt["HUB"]

print("pulling weather (Midland, San Angelo, Abilene) ...")
wx = df(query(f"""
    SELECT {CST} AS DAY,
      AVG(IFF(NAME LIKE 'TX - Midland%',  ACTUAL_DRY_BULB_TEMP_F, NULL)) MID_TEMP,
      MAX(IFF(NAME LIKE 'TX - Midland%',  ACTUAL_DRY_BULB_TEMP_F, NULL)) MID_TEMP_MAX,
      AVG(IFF(NAME LIKE 'TX - Midland%',  ACTUAL_WIND_SPEED_MPH, NULL)) MID_WIND,
      MAX(IFF(NAME LIKE 'TX - Midland%',  ACTUAL_WIND_SPEED_MPH, NULL)) MID_WIND_MAX,
      AVG(IFF(NAME LIKE 'TX - San Angelo%', ACTUAL_WIND_SPEED_MPH, NULL)) SA_WIND,
      AVG(IFF(NAME LIKE 'TX - Abilene%',  ACTUAL_WIND_SPEED_MPH, NULL)) AB_WIND
    FROM {Y}.ALL_WEATHER_MV
    WHERE (NAME LIKE 'TX - Midland%' OR NAME LIKE 'TX - San Angelo%' OR NAME LIKE 'TX - Abilene%')
      AND {WIN}
    GROUP BY 1"""))

print("pulling load (ERCOT + West) ...")
ld = df(query(f"""
    SELECT {CST} AS DAY,
      AVG(IFF(OBJECTID={ERCOT_LOAD}, VALUE, NULL)) ERCOT_LOAD,
      MAX(IFF(OBJECTID={ERCOT_LOAD}, VALUE, NULL)) ERCOT_PEAK,
      AVG(IFF(OBJECTID={WEST_LOAD},  VALUE, NULL)) WEST_LOAD
    FROM {Y}.TS_LOAD
    WHERE OBJECTID IN ({ERCOT_LOAD},{WEST_LOAD}) AND DATATYPEID=47 AND {WIN}
    GROUP BY 1"""))
# NOTE: Waha gas (FUEL_PRICES) only has DA data through 2017 -- re-source a current
# West-TX gas series before adding it as a feature.

print("merging ...")
d = tgt.merge(wx, on="DAY", how="left").merge(ld, on="DAY", how="left")
d["DAY"] = pd.to_datetime(d["DAY"])
d = d.sort_values("DAY").reset_index(drop=True)
d["MONTH"] = d["DAY"].dt.month
d["DOW"] = d["DAY"].dt.dayofweek
# Snowflake numerics arrive as Decimal -> coerce to float for pandas/sklearn.
numcols = [c for c in d.columns if c != "DAY"]
d[numcols] = d[numcols].apply(pd.to_numeric, errors="coerce").astype(float)

out = os.path.join(os.path.dirname(__file__), "nbohr_daily.csv")
d.to_csv(out, index=False)
print(f"\nsaved {len(d)} days -> {out}  ({d['DAY'].min().date()} .. {d['DAY'].max().date()})")
print("non-null per column:\n", d.notna().sum().to_string())
print(f"\nBASIS $/MWh: mean {d['BASIS'].mean():.2f}  std {d['BASIS'].std():.2f}  "
      f"min {d['BASIS'].min():.1f}  p5 {d['BASIS'].quantile(.05):.1f}  p95 {d['BASIS'].quantile(.95):.1f}  max {d['BASIS'].max():.1f}")
