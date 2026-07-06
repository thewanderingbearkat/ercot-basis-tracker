"""Hourly NBOHR basis training table -- the 'unlock' (hourly + wind generation + outages).

Target  : hourly node basis (NBOHR RTLMP - HB_WEST RTLMP).
Features: West/North WIND generation + ERCOT solar (TS_GEN), West-TX weather, ERCOT +
          West load, active >=138kV transmission outage count (daily, broadcast to hours),
          calendar. All hourly, joined on the native (Central) DATETIME hour.
Saves nbohr_hourly.csv.
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
WN_WIND_OBJ, ERCOT_SOLAR_OBJ = 10000821211, 10000712973   # TS_GEN: wind dtype 9067, solar dtype 660
HR = "DATE_TRUNC('hour', DATETIME)"
WIN = "DATETIME >= DATEADD('year',-3,CURRENT_DATE) AND DATETIME < CURRENT_DATE"
df = lambda rows: pd.DataFrame(rows)


def pull(label, sql):
    print("pulling", label, "...")
    return df(query(sql))


tgt = pull("target (NBOHR basis)", f"""
    SELECT {HR} AS HOUR,
           MAX(IFF(OBJECTID={NBOHR},   RTLMP, NULL)) AS NODE,
           MAX(IFF(OBJECTID={HB_WEST}, RTLMP, NULL)) AS HUB
    FROM {Y}.DART_PRICES
    WHERE OBJECTID IN ({NBOHR},{HB_WEST}) AND RTLMP IS NOT NULL AND {WIN}
    GROUP BY 1""")
tgt["BASIS"] = tgt["NODE"] - tgt["HUB"]

gen = pull("generation (wind/solar)", f"""
    SELECT {HR} AS HOUR,
      AVG(IFF(OBJECTID={WN_WIND_OBJ}   AND DATATYPEID=9067 AND VALUE<40000, VALUE, NULL)) WN_WIND,
      AVG(IFF(OBJECTID={ERCOT_SOLAR_OBJ} AND DATATYPEID=660  AND VALUE<40000, VALUE, NULL)) ERCOT_SOLAR
    FROM {Y}.TS_GEN
    WHERE ((OBJECTID={WN_WIND_OBJ} AND DATATYPEID=9067) OR (OBJECTID={ERCOT_SOLAR_OBJ} AND DATATYPEID=660)) AND {WIN}
    GROUP BY 1""")

wx = pull("weather", f"""
    SELECT {HR} AS HOUR,
      AVG(IFF(NAME LIKE 'TX - Midland%',    ACTUAL_DRY_BULB_TEMP_F, NULL)) MID_TEMP,
      AVG(IFF(NAME LIKE 'TX - Midland%',    ACTUAL_WIND_SPEED_MPH, NULL))  MID_WIND,
      AVG(IFF(NAME LIKE 'TX - San Angelo%', ACTUAL_WIND_SPEED_MPH, NULL))  SA_WIND
    FROM {Y}.ALL_WEATHER_MV
    WHERE (NAME LIKE 'TX - Midland%' OR NAME LIKE 'TX - San Angelo%') AND {WIN}
    GROUP BY 1""")

ld = pull("load", f"""
    SELECT {HR} AS HOUR,
      AVG(IFF(OBJECTID={ERCOT_LOAD}, VALUE, NULL)) ERCOT_LOAD,
      AVG(IFF(OBJECTID={WEST_LOAD},  VALUE, NULL)) WEST_LOAD
    FROM {Y}.TS_LOAD
    WHERE OBJECTID IN ({ERCOT_LOAD},{WEST_LOAD}) AND DATATYPEID=47 AND {WIN}
    GROUP BY 1""")

# NOTE: end date must be COALESCE(ENDDATE, PLANNED_ENDDATE) with an end REQUIRED --
# ~45% of tickets have null ENDDATE, and the old COALESCE(..., CURRENT_DATE) counted them
# as active forever (feature read ~220k = cumulative tickets-ever, a disguised time trend).
# Also restrict to LN/XF (lines + transformers); DSC/CB switching tickets dominate the raw
# pool ~10:1 and are noise for congestion. Sane value is tens of active outages per day.
out = pull("outages (active >=138kV lines+transformers, daily)", f"""
    WITH days AS (SELECT DATEADD('day', SEQ4(), DATEADD('year',-3,CURRENT_DATE))::DATE D
                  FROM TABLE(GENERATOR(ROWCOUNT=>1110))),
    o AS (SELECT TICKETID, STARTDATE::DATE s, COALESCE(ENDDATE, PLANNED_ENDDATE)::DATE e
          FROM {Y}.ERCOT_OUTAGES
          WHERE STATUS IN ('Apprv','Accpt') AND VOLTAGELEVEL >= 138
            AND EQUIPMENTTYPE IN ('LN','XF') AND STARTDATE IS NOT NULL
            AND COALESCE(ENDDATE, PLANNED_ENDDATE) IS NOT NULL
            AND DATEDIFF('day', STARTDATE, COALESCE(ENDDATE, PLANNED_ENDDATE)) BETWEEN 0 AND 365)
    SELECT d.D AS DAY, COUNT(DISTINCT o.TICKETID) N_OUTAGE
    FROM days d JOIN o ON d.D BETWEEN o.s AND o.e
    GROUP BY d.D""")

print("merging ...")
d = tgt.merge(gen, on="HOUR", how="left").merge(wx, on="HOUR", how="left").merge(ld, on="HOUR", how="left")
d["HOUR"] = pd.to_datetime(d["HOUR"])

# Co-located Xweather wind + solar at the Bearkat/NBOHR point (forecastable, so the model
# can run forward). Backfilled once by xweather_features.py -> nbohr_xweather.csv.
xw_path = os.path.join(os.path.dirname(__file__), "nbohr_xweather.csv")
if os.path.exists(xw_path):
    xw = pd.read_csv(xw_path, parse_dates=["HOUR"])
    d = d.merge(xw, on="HOUR", how="left")
    print(f"  merged Xweather features ({xw['HOUR'].min()} .. {xw['HOUR'].max()}, {len(xw)} hrs)")
else:
    print("  NOTE: nbohr_xweather.csv missing -- run xweather_features.py first")

d["DAY"] = d["HOUR"].dt.normalize()
out["DAY"] = pd.to_datetime(out["DAY"])
d = d.merge(out, on="DAY", how="left")
d = d.sort_values("HOUR").reset_index(drop=True)
d["HOD"] = d["HOUR"].dt.hour
d["MONTH"] = d["HOUR"].dt.month
d["DOW"] = d["HOUR"].dt.dayofweek
numcols = [c for c in d.columns if c not in ("HOUR", "DAY")]
d[numcols] = d[numcols].apply(pd.to_numeric, errors="coerce").astype(float)

path = os.path.join(os.path.dirname(__file__), "nbohr_hourly.csv")
d.to_csv(path, index=False)
print(f"\nsaved {len(d)} hours -> {path}  ({d['HOUR'].min()} .. {d['HOUR'].max()})")
print("non-null per column:\n", d.notna().sum().to_string())
print(f"\nBASIS $/MWh: mean {d['BASIS'].mean():.2f} std {d['BASIS'].std():.2f} "
      f"p1 {d['BASIS'].quantile(.01):.0f} p99 {d['BASIS'].quantile(.99):.0f} min {d['BASIS'].min():.0f} max {d['BASIS'].max():.0f}")
