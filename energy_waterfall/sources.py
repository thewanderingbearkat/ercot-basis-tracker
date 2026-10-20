"""Data sources for the energy waterfall: Bazefield (read-only GETs), Vaisala PowerUp history, MISO node prices.

Bazefield (skyvest.bazefield.com, published API only - /BazeField.Services/api/openapi):
  * Bearer key, Accept: application/json mandatory; retry only transport errors and 429/502/503/504 honouring
    Retry-After (KB 36000615250 / 36000615256).
  * Aggregated time series are stamped at bucket START in UTC (inObjectTime=false); 10-min TIMEAVERAGE of the realtime
    points is the usable 10-min source (skyvest-notes.md).
  * Alarm and allocation lists return everything OVERLAPPING the window: dedupe by id (KB 36000615254 / 615255).
Never call insert / update / delete endpoints; never log the API key.
"""
import csv
import io
import logging
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------------------- Bazefield
class Bazefield:
    PACE_S = 0.25

    def __init__(self):
        host = (os.getenv("BAZEFIELD_BASE_URL") or "").strip().strip('"').rstrip("/")
        key = (os.getenv("BAZEFIELD_API_KEY") or "").strip().strip('"')
        if not host or not key:
            raise RuntimeError("BAZEFIELD_BASE_URL / BAZEFIELD_API_KEY not set")
        if not host.startswith("http"):
            host = "https://" + host
        self.base, self._key = host + "/BazeField.Services/api", key
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {key}", "Accept": "application/json"})
        self._last, self.calls = 0.0, 0

    def get(self, path: str, params: dict | None = None, timeout: int = 300) -> dict:
        for attempt in range(1, 6):
            wait = self.PACE_S - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            try:
                r = self.s.get(self.base + path, params=params, timeout=timeout)
            except requests.RequestException as e:
                self._last = time.monotonic()
                log.warning("bazefield %s transport error (%s), retry %d", path, str(e).replace(self._key, "***"),
                            attempt)
                time.sleep(min(60, 5 * 2 ** (attempt - 1)) + random.uniform(0, 2))
                continue
            self._last = time.monotonic()
            self.calls += 1
            if r.status_code in (429, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra and ra.replace(".", "", 1).isdigit() else min(60, 5 * 2 ** (attempt - 1))
                time.sleep(delay + random.uniform(0, 2))
                continue
            if "json" not in r.headers.get("Content-Type", ""):
                raise RuntimeError(f"bazefield {path}: HTTP {r.status_code} non-JSON response")
            if r.status_code >= 400:
                raise RuntimeError(f"bazefield {path}: HTTP {r.status_code} {r.text[:200]}")
            return r.json()
        raise RuntimeError(f"bazefield {path}: retries exhausted")

    def paged(self, path: str, params: dict, key: str, take: int = 1000) -> list:
        out, skip = [], 0
        while True:
            pl = self.get(path, {**params, "skip": skip, "take": take, "includeCount": "true"})
            got = pl.get(key) or []
            out += got
            skip += len(got)
            if len(got) < take:
                return out

    # -- objects ------------------------------------------------------------------------------------------------
    def plant_objects(self, plant: str) -> dict:
        """{'site': {...}, 'units': [{...}]} for a plant (object keys carry the plant code, e.g. MCW02-WTG011)."""
        site, units = None, []
        for o in self.get("/objects/structure").get("data") or []:
            a = o.get("attributes") or {}
            k = a.get("objectKey") or ""
            if not k.upper().startswith(plant):
                continue
            rec = {"id": (o.get("objectId") or a.get("objectId") or "").upper(), "key": k,
                   "type": str(a.get("objectTypeId"))}
            if rec["type"] == "102":
                site = rec
            elif rec["type"] in ("200", "206"):
                units.append(rec)
        return {"site": site, "units": sorted(units, key=lambda u: u["key"])}

    # -- time series --------------------------------------------------------------------------------------------
    def series(self, object_id: str, points: list[str], start: date, end: date, interval: str = "10m",
               chunk_days: int = 10) -> pd.DataFrame:
        """TIMEAVERAGE per bucket for [start, end] (UTC days), indexed by bucket START (UTC, naive)."""
        cols = {p: {} for p in points}
        w0 = start
        while w0 <= end:
            w1 = min(end + timedelta(days=1), w0 + timedelta(days=chunk_days))
            pl = self.get("/objects/timeseries/aggregated", {
                "objectIds": object_id, "points": ",".join(points), "aggregates": "TIMEAVERAGE",
                "from": f"{w0}T00:00:00Z", "to": f"{w1}T00:00:00Z", "interval": interval, "inObjectTime": "false"})
            for p, sl in ((pl.get("objects") or {}).get(object_id, {}).get("points") or {}).items():
                for s0 in sl or []:
                    for x in s0.get("timeSeries") or []:
                        v = x.get("v")
                        if isinstance(v, (int, float)) and not (int(x.get("q") or 0) & 0x200000):   # skip NoData
                            cols[p][x["t"]] = float(v)
            w0 = w1
        df = pd.DataFrame({p: pd.Series(v, dtype=float) for p, v in cols.items()})
        df.index = pd.to_datetime(df.index.astype("int64"), unit="ms")
        return df.sort_index()

    def alarms(self, unit_ids: list[str], start: date, end: date) -> list[dict]:
        out, seen = [], set()
        for i in range(0, len(unit_ids), 50):
            ids = ",".join(unit_ids[i:i + 50])
            w0 = start
            while w0 <= end:
                w1 = min(end + timedelta(days=1), w0 + timedelta(days=7))
                for a in self.paged("/objects/alarmhistory", {"objectIds": ids, "from": f"{w0}T00:00:00Z",
                                                             "to": f"{w1}T00:00:00Z", "inUtcTime": "true",
                                                             "inObjectTime": "false"}, "alarms"):
                    if a.get("id") in seen:
                        continue
                    seen.add(a.get("id"))
                    out.append({"object_id": (a.get("objectId") or "").upper(), "type": a.get("type"),
                                "start": a.get("start"), "end": a.get("end") or a.get("stopTime"),
                                "code": a.get("code"), "root": bool(a.get("isRootCause"))})
                w0 = w1
        return out

    def allocations(self, unit_ids: list[str], atype: str, start: date, end: date) -> list[dict]:
        flags = {k: "true" for k in ("includeOverridden", "includeManual", "includeUnedited", "includeSystemAdjusted",
                                     "includeAvailable", "includeExcluded", "includeSystemAllocs")}
        out = {}
        for i in range(0, len(unit_ids), 50):
            ids = ",".join(unit_ids[i:i + 50])
            w0 = start
            while w0 <= end:
                w1 = min(end + timedelta(days=1), w0 + timedelta(days=31))
                path = f"/availability/linked/{atype}/from/{w0}T00:00:00Z/to/{w1}T00:00:00Z"
                for a in self.paged(path, {"objectIds": ids, "inUtcTime": "true", "inObjectTime": "false", **flags},
                                    "allocations"):
                    cat = a.get("category") or {}
                    out[a["id"]] = {"object_id": (a.get("objectId") or "").upper(), "start": a.get("start"),
                                    "end": a.get("end"), "cat": cat.get("name") or "Uncategorized",
                                    "available": bool(cat.get("available", True)),
                                    "excluded": int(cat.get("excluded") or 0),
                                    "root": " ".join((a.get("rootCause") or "").split()),
                                    "lost_kwh": float(a.get("lostProduction") or 0)}
                w0 = w1
        return list(out.values())


def bz_time(s) -> pd.Timestamp | None:
    if not s:
        return None
    t = pd.to_datetime(str(s)[:19], errors="coerce")
    return None if pd.isna(t) or t.year < 2000 else t          # controllers can emit year-1056 events


# ----------------------------------------------------------------------------------------------- PowerUp
POWERUP_URL = "https://app.powerup.xweather.com/PowerupAPI/"
POWERUP_TZ = "America/Chicago"   # MW elements are stamped hour-ending in the account's local clock; weather = UTC


def powerup_history(srcid: str, element: int, start: date, end: date, utc_stamps: bool) -> pd.Series:
    """Hourly PowerUp history indexed by hour ending UTC (naive)."""
    guid, secret = os.getenv("VAISALA_POWERUP_USER_GUID"), os.getenv("VAISALA_POWERUP_USER_SECRET")
    if not guid or not secret:
        raise RuntimeError("VAISALA_POWERUP_USER_GUID / _SECRET not set")
    if end <= start:
        end = start + timedelta(days=1)
    for attempt in range(4):
        try:
            r = requests.get(POWERUP_URL + f"Download/history/{srcid}/{element}/{start:%Y-%m-%d}/{end:%Y-%m-%d}",
                             headers={"UserGUID": guid, "UserSecret": secret}, timeout=120)
            r.raise_for_status()
            break
        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(5 * (attempt + 1))
    body = r.json()
    data = (body.get("data") if isinstance(body, dict) else None) or {}
    tz = ZoneInfo(POWERUP_TZ)
    out = {}
    for d, v in zip(data.get("measDates") or [], data.get("measValues") or []):
        if v is None or not isinstance(d, dict):
            continue
        local = datetime(d["year"], d["month"], d["day"], d.get("hour", 0), d.get("minute", 0))
        if utc_stamps:
            he = local
        else:
            aware = local.replace(tzinfo=tz)
            if aware.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != local:
                continue                                   # nonexistent local hour (spring forward)
            he = aware.astimezone(timezone.utc).replace(tzinfo=None)
        out.setdefault(pd.Timestamp(he), float(v))         # ambiguous fall-back hour: keep the first
    return pd.Series(out, dtype=float).sort_index()


# ----------------------------------------------------------------------------------------------- MISO prices
def miso_hourly(start: date, end: date, node: str) -> pd.DataFrame:
    """MISO public market reports: RT final + DA ex-post hourly LMP (HE in EST) -> HE_UTC frame."""
    rows = []
    d = start
    while d <= end:
        for col, f in (("RT_LMP", f"{d:%Y%m%d}_rt_lmp_final.csv"), ("DA_LMP", f"{d:%Y%m%d}_da_expost_lmp.csv")):
            try:
                r = requests.get("https://docs.misoenergy.org/marketreports/" + f, timeout=120)
            except requests.RequestException:
                continue
            if r.status_code != 200:
                continue
            for row in csv.reader(io.StringIO(r.text)):
                if len(row) >= 27 and row[0] == node and row[2] == "LMP":
                    for h, v in enumerate(row[3:27], 1):
                        if v:
                            rows.append({"HE_UTC": pd.Timestamp(d) + pd.Timedelta(hours=h + 5), "col": col, "v": float(v)})
        d += timedelta(days=1)
    if not rows:
        return pd.DataFrame(columns=["RT_LMP", "DA_LMP"])
    return pd.DataFrame(rows).pivot_table(index="HE_UTC", columns="col", values="v")


YES_VIEW = "YES_ENERGY__FULL_DATASET.YESDATA.TS_PRICES_5MIN_LMP_MISO_V"
YES_NODES = {"ALTW.LKFLD.IPL": 10000764310}


def yes_five_min(start: date, end: date, node: str) -> pd.Series:
    """MISO 5-minute RT LMP from the Yes Energy share, indexed by interval END (UTC, naive).
    DATETIME is the interval end in Eastern prevailing time (TIMEZONE EDT/EST); hourly means reproduce MISO's hourly
    RT LMP to $0.09 (verified 2026-10-19). Uses the existing SNOWFLAKE_* (Yes Energy) key-pair credentials."""
    from energy_waterfall.db import yes_energy
    cur = yes_energy().cursor()
    cur.execute(f"""SELECT DATETIME, TIMEZONE, VALUE FROM {YES_VIEW} WHERE OBJECTID = %s
                    AND DATETIME > %s AND DATETIME <= %s ORDER BY DATETIME""",
                (YES_NODES[node], str(start - timedelta(days=1)), str(end + timedelta(days=2))))
    off = {"EDT": 4, "EST": 5}
    s = pd.Series({pd.Timestamp(dt) + pd.Timedelta(hours=off.get(tz, 5)): float(v) for dt, tz, v in cur.fetchall()
                   if v is not None}, dtype=float).sort_index()
    return s[(s.index > pd.Timestamp(start)) & (s.index <= pd.Timestamp(end) + pd.Timedelta(days=1))]
