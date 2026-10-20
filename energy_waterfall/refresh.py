"""Weekly energy-waterfall refresh (Render cron job): rebuild the trailing window for every enabled plant and replace
those hours in SkyVest Snowflake GOLD.PERFORMANCE.

    python -m energy_waterfall.refresh                       # trailing 21 days to yesterday, all enabled plants
    python -m energy_waterfall.refresh --plants MCW02 --start 2026-08-03 --end 2026-10-19 --recalibrate
    python -m energy_waterfall.refresh --ddl                 # create / migrate the tables (owner role, run locally)
    python -m energy_waterfall.refresh --dry-run --out out/   # build only, write CSVs
Allocations are reclassified for days after an event, so the window re-pulls a few weeks every run.
Exit code 0 = all plants loaded, 1 = at least one plant failed (others still load).
"""
import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from energy_waterfall import config as C
from energy_waterfall import db
from energy_waterfall.engine import build

log = logging.getLogger("energy_waterfall.refresh")
FACT = f"{db.SCHEMA}.ENERGY_WATERFALL_HOURLY"
SITE = f"{db.SCHEMA}.ENERGY_WATERFALL_SITE_HOURLY"
BUILD = f"{db.SCHEMA}.ENERGY_WATERFALL_BUILD"
FACT_COLS = ["PLANT_CODE", "HE_UTC", "LOCAL_DATE", "OBJECT_KEY", "STEP_ORDER", "STEP", "SUBCATEGORY", "DETAIL",
             "COMPENSATION", "MWH", "TURBINE_HOURS"]
SITE_COLS = ["PLANT_CODE", "HE_UTC", "LOCAL_DATE", "LT_POTENTIAL_MWH", "BUDGET_GROSS_MWH", "PU_HUB_WIND_MS",
             "PU_HUB_WIND_LT_MS", "RESOURCE_ACTUAL", "RESOURCE_LT", "RESOURCE_UNIT", "BZ_ACTUAL_MWH",
             "BZ_THEORETICAL_MWH", "BZ_UNAVAILABLE_MWH", "BZ_LOST_ALL_MWH", "TURBINES_MEASURED", "TURBINES_TOTAL",
             "RT_LMP", "DA_LMP", "PRICE_SOURCE", "METER_MWH", "METER_SOURCE", "BZ_METER_MWH",
             "RELATED_METER_MWH"]
TEXT = {"PLANT_CODE", "OBJECT_KEY", "STEP", "SUBCATEGORY", "DETAIL", "COMPENSATION", "RESOURCE_UNIT", "PRICE_SOURCE",
        "METER_SOURCE"}


def _value(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (np.floating, np.integer)):
        return None if np.isnan(v) else float(v)
    return v


def replace_window(conn, table: str, df: pd.DataFrame, cols: list[str], build_id: str, plant: str, w0: str, w1: str):
    """DELETE the plant's (w0, w1] hours and INSERT the rebuild, in one transaction via a session temp table."""
    tmp = f"STG_{uuid.uuid4().hex[:10].upper()}"
    cur = conn.cursor()
    cur.execute(f"CREATE TEMPORARY TABLE {tmp} ({', '.join(f'{c} VARCHAR' for c in cols)})")
    sql = f"INSERT INTO {tmp} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
    rows = [tuple(None if (x := _value(v)) is None else str(x) for v in r) for r in df[cols].itertuples(index=False)]
    for i in range(0, len(rows), 20000):
        cur.executemany(sql, rows[i:i + 20000])
    cast = []
    for c in cols:
        if c == "HE_UTC":
            cast.append("TO_TIMESTAMP_NTZ(HE_UTC)")
        elif c == "LOCAL_DATE":
            cast.append("TO_DATE(LOCAL_DATE)")
        elif c in TEXT:
            cast.append(f"COALESCE({c}, '')")
        else:
            cast.append(f"TRY_TO_DOUBLE({c})")
    cur.execute("BEGIN")
    cur.execute(f"DELETE FROM {table} WHERE PLANT_CODE = ? AND HE_UTC > ? AND HE_UTC <= ?", (plant, w0, w1))
    deleted = cur.rowcount
    cur.execute(f"INSERT INTO {table} ({', '.join(cols)}, BUILD_ID) SELECT {', '.join(cast)}, ? FROM {tmp}",
                (build_id,))
    inserted = cur.rowcount
    cur.execute("COMMIT")
    cur.execute(f"DROP TABLE IF EXISTS {tmp}")
    log.info("%s %s: deleted %d, inserted %d", plant, table, deleted, inserted)
    return inserted


def load(fact, site, b):
    conn = db.skyvest()
    w0, w1, plant, bid = b["WINDOW_START_UTC"], b["WINDOW_END_UTC"], b["PLANT_CODE"], b["BUILD_ID"]
    n = replace_window(conn, FACT, fact, FACT_COLS, bid, plant, w0, w1)
    replace_window(conn, SITE, site, SITE_COLS, bid, plant, w0, w1)
    conn.cursor().execute(
        f"""INSERT INTO {BUILD} (BUILD_ID, PLANT_CODE, WINDOW_START_UTC, WINDOW_END_UTC, BUILT_AT, POWERUP_K,
            STOPPED_ANEMOMETER_FACTOR, LT_YEARS, METHOD, FACT_ROWS)
            VALUES (?, ?, TO_TIMESTAMP_NTZ(?), TO_TIMESTAMP_NTZ(?), TO_TIMESTAMP_NTZ(?), ?, ?, ?, ?, ?)""",
        (bid, plant, w0, w1, b["BUILT_AT"], b["POWERUP_K"], b["STOPPED_ANEMOMETER_FACTOR"], b["LT_YEARS"],
         b["METHOD"], n))


def meter_only(plant: str, start: date, end: date):
    """Fill METER_MWH / METER_SOURCE / BZ_METER_MWH on existing site hours without rebuilding the waterfall."""
    from energy_waterfall import sources as S
    from energy_waterfall.engine import meter_series
    p = C.PLANTS[plant]
    hours = pd.date_range(pd.Timestamp(start) + pd.Timedelta(hours=1), pd.Timestamp(end) + pd.Timedelta(days=1), freq="h")
    meter = meter_series(plant, p, start, end).reindex(hours)
    related = S.related_meter_hourly(p, start, end).reindex(hours)
    bz = S.Bazefield()
    site = bz.plant_objects(plant)["site"]
    pts = ["ActivePowerMeter", "ActivePowerWTG" if p["tech"] == "wind" else "ActivePowerINV"]
    sd = bz.series(site["id"], pts, start, end, interval="1h") if site else pd.DataFrame()
    sd.index = pd.DatetimeIndex(sd.index) + pd.Timedelta(hours=1)
    sd = sd.reindex(hours)
    bzm, gross = sd.get(pts[0], pd.Series(np.nan, index=hours)), sd.get(pts[1], pd.Series(0.0, index=hours)).fillna(0)
    day = (hours - pd.Timedelta(hours=6)).date
    ok = (bzm.groupby(day).transform("sum") >= 0.5 * gross.groupby(day).transform("sum")) & (gross.groupby(day).transform("sum") > 0)
    df = pd.DataFrame({"PLANT_CODE": plant, "HE_UTC": hours, "METER_MWH": meter.values,
                       "METER_SOURCE": (p.get("meter") or {}).get("label", ""), "BZ_METER_MWH": bzm.where(ok.values).values,
                       "RELATED_METER_MWH": related.values})
    conn = db.skyvest()
    cols = list(df.columns)
    tmp = f"STG_{uuid.uuid4().hex[:10].upper()}"
    cur = conn.cursor()
    cur.execute(f"CREATE TEMPORARY TABLE {tmp} ({', '.join(f'{c} VARCHAR' for c in cols)})")
    rows = [tuple(None if (x := _value(v)) is None else str(x) for v in r) for r in df.itertuples(index=False)]
    cur.executemany(f"INSERT INTO {tmp} VALUES ({', '.join('?' * len(cols))})", rows)
    cur.execute(f"""UPDATE {SITE} t SET METER_MWH = TRY_TO_DOUBLE(s.METER_MWH), METER_SOURCE = s.METER_SOURCE,
                       BZ_METER_MWH = TRY_TO_DOUBLE(s.BZ_METER_MWH),
                       RELATED_METER_MWH = TRY_TO_DOUBLE(s.RELATED_METER_MWH)
                    FROM {tmp} s WHERE t.PLANT_CODE = s.PLANT_CODE AND t.HE_UTC = TO_TIMESTAMP_NTZ(s.HE_UTC)""")
    log.info("%s meter-only: %d site hours updated (%d with meter, %d with valid Bazefield meter)", plant, cur.rowcount,
             int(df["METER_MWH"].notna().sum()), int(df["BZ_METER_MWH"].notna().sum()))


def run_ddl():
    sql = (C.HERE / "ddl.sql").read_text(encoding="utf-8")
    import re
    cur = db.skyvest().cursor()
    for stmt in (s.strip() for s in re.sub(r"--[^\n]*", "", sql).split(";")):
        if stmt:
            cur.execute(stmt)
    log.info("DDL applied")


def main(argv=None) -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv(C.HERE.parent / ".env")
    except ImportError:
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(prog="energy_waterfall.refresh")
    ap.add_argument("--days", type=int, default=21, help="trailing days to rebuild (default 21)")
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    ap.add_argument("--plants", help="comma-separated plant codes (default: all enabled)")
    ap.add_argument("--recalibrate", action="store_true", help="recompute k / stopped-anemometer factor")
    ap.add_argument("--ddl", action="store_true", help="apply energy_waterfall/ddl.sql (table owner role)")
    ap.add_argument("--dry-run", action="store_true", help="build only; do not write to Snowflake")
    ap.add_argument("--meter-only", action="store_true", help="only (re)fill the meter columns for the window")
    ap.add_argument("--out", type=Path, help="also write fact/site CSVs here")
    a = ap.parse_args(argv)
    if a.ddl:
        run_ddl()
        return 0
    today = datetime.now(timezone.utc).date()
    end = a.end or today - timedelta(days=1)
    start = a.start or end - timedelta(days=a.days - 1)
    plants = a.plants.split(",") if a.plants else C.enabled_plants(today)
    log.info("refresh %s .. %s for %s", start, end, plants)
    if a.meter_only:
        for plant in plants:
            meter_only(plant, start, end)
        return 0
    failed = []
    for plant in plants:
        try:
            fact, site, b = build(plant, start, end, recalibrate=a.recalibrate)
            if a.out:
                a.out.mkdir(parents=True, exist_ok=True)
                fact.to_csv(a.out / f"{plant.lower()}_fact.csv.gz", index=False)
                site.to_csv(a.out / f"{plant.lower()}_site.csv.gz", index=False)
            s = fact.groupby("STEP_ORDER")["MWH"].sum().round(0)
            log.info("%s summary MWh by step: %s", plant, s.to_dict())
            if not a.dry_run:
                load(fact, site, b)
        except Exception:
            log.exception("%s failed", plant)
            failed.append(plant)
    if failed:
        log.error("failed plants: %s", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
