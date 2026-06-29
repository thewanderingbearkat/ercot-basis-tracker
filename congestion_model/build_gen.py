"""Pull McCrae's actual hourly generation (resource HARALD_UNIT1) from ERCOT SCED data.

This is what makes the generation-weighted (GWA) basis real instead of a regional-wind proxy:
GWA = sum(basis x gen) / sum(gen), and the gen has to be THIS plant's output at the same
hourly resolution as the basis. Source: ERCOT_60D_SCED_GEN_RESOURCE_DATA_RAW (5-min SCED,
~60-day publication lag), TELEMETERED_NET_OUTPUT = actual MW delivered (already net of any
congestion curtailment, which is exactly what earns revenue). Rolled up to the native Central
hour to join nbohr_hourly.csv. Saves nbohr_gen.csv.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from constraint_map.db import query, YES  # noqa: E402

T = f"{YES}.ERCOT_60D_SCED_GEN_RESOURCE_DATA_RAW"
RES = "HARALD_UNIT1"           # McCrae II generation resource (settles at NBOHR_RN)
NAMEPLATE = 200                # MW sanity cap for HSL outliers (McCrae ~162 MW nameplate)

print(f"pulling {RES} hourly generation from SCED ...")
rows = query(f"""
    SELECT DATE_TRUNC('hour', SCED_TIME_STAMP) AS HOUR,
           AVG(TELEMETERED_NET_OUTPUT) AS GEN_MW,
           AVG(LEAST(HSL, {NAMEPLATE})) AS HSL_MW
    FROM {T}
    WHERE RES_NAME = '{RES}'
      AND SCED_TIME_STAMP >= DATEADD('year', -3, CURRENT_DATE)
      AND TELEMETERED_NET_OUTPUT IS NOT NULL
    GROUP BY 1""")
d = pd.DataFrame(rows)
d["HOUR"] = pd.to_datetime(d["HOUR"])
for c in ("GEN_MW", "HSL_MW"):
    d[c] = pd.to_numeric(d[c], errors="coerce").astype(float)
d = d.sort_values("HOUR").reset_index(drop=True)
# Curtailment proxy: how far actual output sits below the available (HSL) capability.
d["CURTAIL_MW"] = (d["HSL_MW"] - d["GEN_MW"]).clip(lower=0)

out = os.path.join(os.path.dirname(__file__), "nbohr_gen.csv")
d.to_csv(out, index=False)
print(f"saved {len(d)} hours -> {out}  ({d['HOUR'].min()} .. {d['HOUR'].max()})")
print(f"  GEN_MW: mean {d['GEN_MW'].mean():.1f}  p95 {d['GEN_MW'].quantile(.95):.0f}  max {d['GEN_MW'].max():.0f}  CF {d['GEN_MW'].mean()/NAMEPLATE:.0%}")
print(f"  curtailment: mean {d['CURTAIL_MW'].mean():.1f} MW  ({(d['CURTAIL_MW']>1).mean():.0%} of hours show >1 MW curtailed)")
