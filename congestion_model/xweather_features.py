"""Co-located Xweather wind + solar features for the NBOHR (Bearkat) congestion model.

WHY: the model's strongest driver is Yes Energy wind GENERATION (TS_GEN) -- but that's
actuals-only and lagged, so you can't run the model forward on it. Xweather wind speed +
GHI AT THE NODE are forecastable, so adding them is the bridge that lets the trained model
project basis on a forecast. We pull ONE point (the Bearkat complex) and get wind + solar
in a SINGLE /conditions call, so 3 years of hourly history is ~37 calls -- paid once.

LOW-USAGE design (Xweather access is limited): every 30-day chunk is cached to disk
(xw_cache/), 429s back off, and the daily cap bails cleanly. A second run is essentially
free and resumes from wherever a throttled first run got to -- same contract as solar_ghi.py.

    python xweather_features.py            # full 3yr backfill -> nbohr_xweather.csv
    python xweather_features.py --smoke    # one recent day, to verify access + fields
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Bearkat complex / NBOHR_RN -- Glasscock County, TX (constraint_map/sites.py).
LAT, LON = 31.7272, -101.5820
BASE_URL = "https://data.api.xweather.com"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "xw_cache")
OUT = os.path.join(os.path.dirname(__file__), "nbohr_xweather.csv")

# Wind speed/gust (forecastable proxy for wind generation) + GHI (solar), all in one call.
FIELDS = "periods.dateTimeISO,periods.windSpeedMPH,periods.windGustMPH,periods.solrad.ghiWM2"
CHUNK_DAYS = 30           # hourly -> ~720 periods/request; 3yr ~= 37 cached calls
THROTTLE_SECONDS = 3.0    # polite gap to stay under the per-minute limit
MAX_RETRIES = 5


class DailyQuotaExceeded(RuntimeError):
    """The daily request cap (maxhits_daily) is spent -- stop and resume after reset."""


def _cache_path(start: str, end: str) -> str:
    return os.path.join(CACHE_DIR, f"wxsolar_{LAT}_{LON}_{start}_{end}_1hr.json")


def _fetch_chunk(session: requests.Session, start: str, end: str) -> list:
    """Hourly wind+GHI periods for [start, end). Cached; 429s retried with backoff."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cpath = _cache_path(start, end)
    if os.path.exists(cpath):
        with open(cpath) as f:
            return json.load(f)

    params = {
        "from": start, "to": end, "filter": "1hr", "limit": CHUNK_DAYS * 24 + 5,
        "fields": FIELDS,
        "client_id": os.getenv("XWEATHER_CLIENT_ID"),
        "client_secret": os.getenv("XWEATHER_CLIENT_SECRET"),
    }
    url = f"{BASE_URL}/conditions/{LAT},{LON}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=40)
        except requests.RequestException as e:   # transient DNS/connection blip -- back off
            wait = min(30, 5 * (2 ** attempt))
            print(f"  network error on {start} ({e.__class__.__name__}); retrying in {wait}s")
            time.sleep(wait)
            continue
        body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        err = (body.get("error") or {}).get("code", "")

        if err in ("maxhits_daily", "maxhits"):
            raise DailyQuotaExceeded(resp.headers.get("Retry-After", "unknown"))
        if resp.status_code == 429:
            wait = min(60, 15 * (2 ** attempt))
            print(f"  per-minute limit on {start}; backing off {wait:.0f}s")
            time.sleep(wait)
            continue
        if not body.get("success"):
            print(f"  {start}->{end}: no data (err={err})")
            return []   # don't cache -- a later run may have access
        raw = body["response"][0].get("periods", [])
        periods = [{
            "ts": p["dateTimeISO"],
            "wind": p.get("windSpeedMPH"),
            "gust": p.get("windGustMPH"),
            "ghi": (p.get("solrad") or {}).get("ghiWM2"),
        } for p in raw]
        with open(cpath, "w") as f:   # cache only confirmed-good responses
            json.dump(periods, f)
        time.sleep(THROTTLE_SECONDS)
        return periods

    print(f"  {start}->{end}: gave up after {MAX_RETRIES} per-minute retries")
    return []


def fetch(start_date: str, end_date: str) -> pd.DataFrame:
    """Hourly Xweather wind+GHI over [start_date, end_date], indexed by naive Central hour
    (to join the Yes Energy native-Central DATETIME hour in build_hourly.py)."""
    session = requests.Session()
    rows = []
    cur = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    n_chunks = ((end - cur).days // CHUNK_DAYS) + 1
    i = 0
    while cur <= end:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS), end + timedelta(days=1))
        i += 1
        print(f"[{i}/{n_chunks}] {cur:%Y-%m-%d} -> {chunk_end:%Y-%m-%d}")
        rows.extend(_fetch_chunk(session, cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end
    if not rows:
        return pd.DataFrame(columns=["HOUR", "XW_WIND", "XW_GUST", "XW_GHI"])

    df = pd.DataFrame(rows)
    ts = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/Chicago").dt.tz_localize(None)
    df["HOUR"] = ts.dt.floor("h")
    df = (df.drop(columns="ts")
            .rename(columns={"wind": "XW_WIND", "gust": "XW_GUST", "ghi": "XW_GHI"})
            .groupby("HOUR", as_index=False).mean()
            .sort_values("HOUR"))
    return df


def fetch_forecast(hours: int = 168) -> pd.DataFrame:
    """Forward hourly Xweather wind+GHI for the next `hours` (default 7d) at the NBOHR point.
    ONE API call. Returns HOUR (naive Central) + XW_WIND/XW_GUST/XW_GHI, same schema as fetch()."""
    params = {
        "filter": "1hr", "limit": hours, "fields": FIELDS,
        "client_id": os.getenv("XWEATHER_CLIENT_ID"),
        "client_secret": os.getenv("XWEATHER_CLIENT_SECRET"),
    }
    resp = requests.get(f"{BASE_URL}/forecasts/{LAT},{LON}", params=params, timeout=40)
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"forecast fetch failed: {(body.get('error') or {})}")
    raw = body["response"][0].get("periods", [])
    rows = [{
        "ts": p.get("dateTimeISO"),
        "XW_WIND": p.get("windSpeedMPH"),
        "XW_GUST": p.get("windGustMPH"),
        "XW_GHI": (p.get("solrad") or {}).get("ghiWM2"),
    } for p in raw]
    df = pd.DataFrame(rows)
    ts = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/Chicago").dt.tz_localize(None)
    df["HOUR"] = ts.dt.floor("h")
    return df.drop(columns="ts").groupby("HOUR", as_index=False).mean().sort_values("HOUR")


def main():
    ap = argparse.ArgumentParser(description="Backfill co-located Xweather wind+solar for a site")
    ap.add_argument("--start", help="YYYY-MM-DD (default: 3 years ago)")
    ap.add_argument("--end", help="YYYY-MM-DD (default: today)")
    ap.add_argument("--lat", type=float, help="override site latitude (default Bearkat/NBOHR)")
    ap.add_argument("--lon", type=float, help="override site longitude")
    ap.add_argument("--out", help="output csv (default nbohr_xweather.csv)")
    ap.add_argument("--smoke", action="store_true", help="fetch one recent day and exit")
    args = ap.parse_args()
    # Point override for other sites (e.g. Canadian Hills); cache filenames include lat/lon
    # so they never collide with Bearkat's. Defaults keep the McCrae import unchanged.
    global LAT, LON, OUT
    if args.lat is not None and args.lon is not None:
        LAT, LON = args.lat, args.lon
    if args.out:
        OUT = os.path.join(os.path.dirname(__file__), args.out)

    if args.smoke:
        yday = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        nxt = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        df = fetch(yday, nxt)
        print(f"smoke: {len(df)} hourly rows for {yday}")
        print(df.head(12).to_string(index=False))
        return

    end = args.end or datetime.now().strftime("%Y-%m-%d")
    start = args.start or (datetime.now() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
    print(f"Backfilling Xweather wind+solar for NBOHR ({LAT},{LON}): {start} -> {end}")
    try:
        df = fetch(start, end)
    except DailyQuotaExceeded as e:
        print(f"\n*** Xweather daily request cap reached (resets: {e}). ***")
        print("Cached chunks are saved; just re-run after reset and it resumes.")
        return
    df.to_csv(OUT, index=False)
    print(f"\nsaved {len(df)} hours -> {OUT}  ({df['HOUR'].min()} .. {df['HOUR'].max()})")
    print("non-null per column:\n", df.notna().sum().to_string())
    for c in ("XW_WIND", "XW_GUST", "XW_GHI"):
        if df[c].notna().any():
            print(f"  {c}: mean {df[c].mean():.1f}  p95 {df[c].quantile(.95):.1f}  max {df[c].max():.1f}")


if __name__ == "__main__":
    main()
