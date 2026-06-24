"""Backfill / refresh the constraint-map daily staging tables in SKYVEST.DBO.

The app's Snowflake role (SKYVEST_READ) can read AND write SKYVEST.DBO, so this
runs through the same key-pair connection -- no separate creds, no GRANTs (the
creating role IS the reading role). Idempotent + incremental: re-run anytime and
each INSERT only adds days not already staged.

    python "Constraints and Weather/staging/stage.py"            # create + backfill
    python "Constraints and Weather/staging/stage.py" --counts   # just show status

Because the app computes live recent days on top, this can lag by days/weeks and
the app stays current -- so an occasional re-run is enough.
"""
from __future__ import annotations

import os
import sys
import time

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from constraint_map.db import query  # noqa: E402

DBO = "SKYVEST.DBO"
YES = "YES_ENERGY__FULL_DATASET.YESDATA"
E_NODES = "10000697078,10000697080,10000698819,10004202409,10016076881,10016246152"
P_NODES = "51301,34497127,1318144721,1369011076,1369012529,124076095,1869196082,1348264769,2156109467"
YEARS = 3
WIN = f"DATETIME >= DATEADD('year',-{YEARS},CURRENT_DATE) AND DATETIME < CURRENT_DATE"

DDL = [
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_ERCOT_SF_DAILY (PRICENODEID NUMBER, DAY DATE, CONSTRAINTID NUMBER, CONSTRAINTNAME STRING, FACILITYID NUMBER, SF_SUM FLOAT)",
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_ERCOT_INTERVALS_DAILY (DAY DATE, N_INTERVALS NUMBER)",
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_ERCOT_LMP_DAILY (OBJECTID NUMBER, DAY DATE, RTLMP_SUM FLOAT, N NUMBER)",
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_PJM_DART_DAILY (OBJECTID NUMBER, DAY DATE, RTLMP_SUM FLOAT, RTCONG_SUM FLOAT, RTLOSS_SUM FLOAT, N NUMBER)",
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_PJM_CONSTRAINTS_DAILY (FACILITYID NUMBER, DAY DATE, N_BIND NUMBER, ABS_PRICE_SUM FLOAT, N_CTG NUMBER)",
    f"CREATE TABLE IF NOT EXISTS {DBO}.CM_PJM_INTERVALS_DAILY (DAY DATE, N_INTERVALS NUMBER)",
]

INSERTS = [
    ("CM_ERCOT_SF_DAILY", f"""
        INSERT INTO {DBO}.CM_ERCOT_SF_DAILY
        SELECT PRICENODEID, DATETIME::DATE, CONSTRAINTID, ANY_VALUE(CONSTRAINTNAME),
               ANY_VALUE(FACILITYID), SUM(-(SHADOWPRICE*SHIFTFACTOR))
        FROM {YES}.MARKET_SHIFT_FACTORS
        WHERE MARKET='RT' AND PRICENODEID IN ({E_NODES}) AND {WIN}
          AND DATETIME::DATE NOT IN (SELECT DISTINCT DAY FROM {DBO}.CM_ERCOT_SF_DAILY)
        GROUP BY PRICENODEID, DATETIME::DATE, CONSTRAINTID"""),
    ("CM_ERCOT_INTERVALS_DAILY", f"""
        INSERT INTO {DBO}.CM_ERCOT_INTERVALS_DAILY
        SELECT DATETIME::DATE, COUNT(DISTINCT DATETIME)
        FROM {YES}.MARKET_SHIFT_FACTORS
        WHERE MARKET='RT' AND PRICENODEID IN ({E_NODES}) AND {WIN}
          AND DATETIME::DATE NOT IN (SELECT DAY FROM {DBO}.CM_ERCOT_INTERVALS_DAILY)
        GROUP BY DATETIME::DATE"""),
    ("CM_ERCOT_LMP_DAILY", f"""
        INSERT INTO {DBO}.CM_ERCOT_LMP_DAILY
        SELECT OBJECTID, DATETIME::DATE, SUM(RTLMP), COUNT(*)
        FROM {YES}.DART_PRICES
        WHERE OBJECTID IN ({E_NODES}) AND RTLMP IS NOT NULL AND {WIN}
          AND DATETIME::DATE NOT IN (SELECT DISTINCT DAY FROM {DBO}.CM_ERCOT_LMP_DAILY)
        GROUP BY OBJECTID, DATETIME::DATE"""),
    # Per-(node,day) incremental so ADDING a node backfills its history (the
    # other tables aren't per-node, so their DAY-level skip is fine).
    ("CM_PJM_DART_DAILY", f"""
        INSERT INTO {DBO}.CM_PJM_DART_DAILY
        SELECT d.OBJECTID, d.DATETIME::DATE, SUM(d.RTLMP), SUM(COALESCE(d.RTCONG,0)), SUM(COALESCE(d.RTLOSS,0)), COUNT(*)
        FROM {YES}.DART_PRICES d
        WHERE d.OBJECTID IN ({P_NODES}) AND d.RTLMP IS NOT NULL
          AND d.DATETIME >= DATEADD('year',-{YEARS},CURRENT_DATE) AND d.DATETIME < CURRENT_DATE
          AND NOT EXISTS (SELECT 1 FROM {DBO}.CM_PJM_DART_DAILY t
                          WHERE t.OBJECTID = d.OBJECTID AND t.DAY = d.DATETIME::DATE)
        GROUP BY d.OBJECTID, d.DATETIME::DATE"""),
    ("CM_PJM_CONSTRAINTS_DAILY", f"""
        INSERT INTO {DBO}.CM_PJM_CONSTRAINTS_DAILY
        SELECT FACILITYID, DATETIME::DATE, COUNT(DISTINCT DATETIME), SUM(ABS(PRICE)), COUNT(DISTINCT CONTINGENCYID)
        FROM {YES}.CONSTRAINTS
        WHERE ISO='PJMISO' AND PRICE<>0 AND FACILITYID IS NOT NULL AND {WIN}
          AND DATETIME::DATE NOT IN (SELECT DISTINCT DAY FROM {DBO}.CM_PJM_CONSTRAINTS_DAILY)
        GROUP BY FACILITYID, DATETIME::DATE"""),
    ("CM_PJM_INTERVALS_DAILY", f"""
        INSERT INTO {DBO}.CM_PJM_INTERVALS_DAILY
        SELECT DATETIME::DATE, COUNT(DISTINCT DATETIME)
        FROM {YES}.CONSTRAINTS
        WHERE ISO='PJMISO' AND {WIN}
          AND DATETIME::DATE NOT IN (SELECT DAY FROM {DBO}.CM_PJM_INTERVALS_DAILY)
        GROUP BY DATETIME::DATE"""),
]

ALL_TABLES = [d.split("CM_")[1].split(" ")[0] for d in DDL]


def counts():
    for t in ALL_TABLES:
        try:
            r = query(f"SELECT COUNT(*) C, MIN(DAY) LO, MAX(DAY) HI FROM {DBO}.CM_{t}")[0]
            print(f"  CM_{t:26} rows={r['C']:>9}  {str(r['LO'])[:10]} -> {str(r['HI'])[:10]}")
        except Exception as e:
            print(f"  CM_{t:26} ERROR {str(e)[:60]}")


def main():
    if "--counts" in sys.argv:
        counts(); return
    print("creating tables ...")
    for d in DDL:
        query(d)
    print("backfilling (incremental) ...")
    for name, sql in INSERTS:
        t = time.time()
        query(sql)
        print(f"  {name:26} done in {time.time()-t:6.1f}s")
    print("status:")
    counts()


if __name__ == "__main__":
    main()
