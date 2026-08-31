"""Fit empirical site power curves: XWeather observed weather -> hourly generation.

WHY: the shadow trader's wind forecast for BKI/BKII is regional STWPF scaled by
historical share -- indirect and crude. The PowerUp API gives site-level wind and
irradiance FORECASTS, so if a stable mapping f(site weather) -> plant MWh exists,
the forward gen forecast becomes f(forecast weather): a per-site physical forecast
with no regional-share approximation. This script answers the feasibility question
BEFORE any strategy wiring: how much of hourly gen does the curve explain?

Method (per asset):
  1. Backfill hourly XWeather /conditions (10m wind, GHI, temp) at the site since
     BACKTEST_START_DATE; 30-day chunks cached to data/wx_trial/cond_cache/.
     (10m wind, not 80m -- /conditions doesn't carry 80m. Consistency rule: the
     curve must later be APPLIED to the forecast's 10m field, windSpeedMPH.)
  2. Pull Tenaska hourly gen actuals (cached to data/wx_trial/gen_cache.json).
  3. Join on 'YYYY-MM-DD HE##', fit a binned curve: median and q75 gen per
     wind/GHI bin. q75 approximates AVAILABLE power (curtailment-robust);
     median reflects typical DELIVERED power. The gap between them per bin is
     itself a curtailment fingerprint worth eyeballing.
  4. Score on a holdout (last 45 days, out-of-sample): predict via the median
     curve from OBSERVED weather. This is the skill CEILING for a curve-based
     forecast -- forecast-weather error (measured by the wx_trial snapshots)
     comes on top of it.

    python fit_power_curve.py            # fit + report + save curves
    python fit_power_curve.py --refresh  # force re-fetch of gen actuals

Output: data/wx_trial/power_curves.json  {asset: {kind, bin_width, bins: {center:
{n, median, q75}}}} plus console diagnostics.
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta
from statistics import median, quantiles
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from shadow_trader.config import ASSET_CONFIG, BACKTEST_START_DATE, DART_ASSETS, DATA_DIR  # noqa: E402

BASE_URL = "https://data.api.xweather.com"
OUT_DIR = os.path.join(DATA_DIR, "wx_trial")
COND_CACHE = os.path.join(OUT_DIR, "cond_cache")
GEN_CACHE = os.path.join(OUT_DIR, "gen_cache.json")
CURVES_OUT = os.path.join(OUT_DIR, "power_curves.json")

CHUNK_DAYS = 30
HOLDOUT_DAYS = 45
WIND_BIN_MPH = 2.0
GHI_BIN_WM2 = 50.0
_CT = ZoneInfo("America/Chicago")


# ----------------------------------------------------------------------------- weather

def _hour_key(iso_str: str) -> str | None:
    try:
        dt = datetime.fromisoformat(iso_str).astimezone(_CT)
    except ValueError:
        return None
    return f"{dt:%Y-%m-%d} HE{dt.hour + 1:02d}"


def _fetch_conditions_chunk(lat: float, lon: float, start: str, end: str) -> list:
    """One cached 30-day /conditions chunk -> [{ts, wind, ghi, temp}]."""
    os.makedirs(COND_CACHE, exist_ok=True)
    cpath = os.path.join(COND_CACHE, f"cond_{lat}_{lon}_{start}_{end}.json")
    if os.path.exists(cpath):
        with open(cpath) as f:
            return json.load(f)
    params = {
        "from": start, "to": end, "filter": "1hr", "limit": CHUNK_DAYS * 24 + 5,
        "fields": "periods.dateTimeISO,periods.windSpeedMPH,periods.solrad.ghiWM2,periods.tempF",
        "client_id": os.getenv("XWEATHER_CLIENT_ID"),
        "client_secret": os.getenv("XWEATHER_CLIENT_SECRET"),
    }
    for attempt in range(4):
        resp = requests.get(f"{BASE_URL}/conditions/{lat},{lon}", params=params, timeout=40)
        body = resp.json()
        err = (body.get("error") or {}).get("code", "")
        if err == "maxhits_min":
            # Archive requests count multiple hits per call; the minute window resets fast.
            print(f"  per-minute cap on {start}; waiting 65s")
            time.sleep(65)
            continue
        break
    if not body.get("success"):
        print(f"  ! conditions {start}->{end}: {err}")
        return []
    rows = [{
        "ts": p["dateTimeISO"],
        "wind": p.get("windSpeedMPH"),
        "ghi": (p.get("solrad") or {}).get("ghiWM2"),
        "temp": p.get("tempF"),
    } for p in body["response"][0].get("periods", [])]
    with open(cpath, "w") as f:
        json.dump(rows, f)
    time.sleep(1.0)
    return rows


def fetch_conditions(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """{hour_key: {wind, ghi, temp}} over [start_date, end_date]."""
    out = {}
    cur = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while cur <= end:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS), end + timedelta(days=1))
        print(f"  conditions {cur:%Y-%m-%d} -> {chunk_end:%Y-%m-%d}")
        for r in _fetch_conditions_chunk(lat, lon, f"{cur:%Y-%m-%d}", f"{chunk_end:%Y-%m-%d}"):
            key = _hour_key(r["ts"])
            if key:
                out[key] = {"wind": r["wind"], "ghi": r["ghi"], "temp": r["temp"]}
        cur = chunk_end
    return out


# ----------------------------------------------------------------------------- gen

def fetch_gen(start_date: str, end_date: str, refresh: bool) -> dict:
    """Tenaska hourly gen actuals for all DART assets, cached across runs."""
    if os.path.exists(GEN_CACHE) and not refresh:
        with open(GEN_CACHE) as f:
            cached = json.load(f)
        if cached.get("end_date", "") >= end_date:
            print(f"gen actuals: using cache ({cached['start_date']} -> {cached['end_date']})")
            return cached["generation"]
    from shadow_trader.data import fetch_generation
    from shadow_trader.tenaska import get_tenaska_token
    print(f"gen actuals: fetching {start_date} -> {end_date} from Tenaska")
    token = get_tenaska_token()
    if not token:
        raise SystemExit("Could not obtain Tenaska token (check TENASKA_API_* in .env)")
    gen = fetch_generation(start_date, end_date, token)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(GEN_CACHE, "w") as f:
        json.dump({"start_date": start_date, "end_date": end_date, "generation": gen}, f)
    return gen


# ----------------------------------------------------------------------------- fit

def fit_binned_curve(pairs: list[tuple[float, float]], bin_width: float) -> dict:
    """[(x, gen_mwh)] -> {bin_center: {n, median, q75}}, bins with >= 5 samples."""
    bins: dict[float, list[float]] = {}
    for x, y in pairs:
        center = (int(x / bin_width) + 0.5) * bin_width
        bins.setdefault(center, []).append(y)
    out = {}
    for center in sorted(bins):
        ys = sorted(bins[center])
        if len(ys) < 5:
            continue
        q75 = quantiles(ys, n=4)[2] if len(ys) >= 4 else ys[-1]
        out[round(center, 1)] = {"n": len(ys), "median": round(median(ys), 2),
                                 "q75": round(q75, 2)}
    return out


def predict(curve: dict, x: float) -> float | None:
    """Median-curve lookup with linear interpolation between bin centers."""
    if not curve:
        return None
    centers = sorted(curve)
    if x <= centers[0]:
        return curve[centers[0]]["median"]
    if x >= centers[-1]:
        return curve[centers[-1]]["median"]
    for lo, hi in zip(centers, centers[1:]):
        if lo <= x <= hi:
            w = (x - lo) / (hi - lo)
            return curve[lo]["median"] * (1 - w) + curve[hi]["median"] * w
    return None


def score(pairs: list[tuple[float, float]], curve: dict, label: str) -> None:
    preds = [(predict(curve, x), y) for x, y in pairs]
    preds = [(p, y) for p, y in preds if p is not None]
    if len(preds) < 20:
        print(f"  {label}: too few samples ({len(preds)})")
        return
    n = len(preds)
    mae = sum(abs(p - y) for p, y in preds) / n
    mean_y = sum(y for _, y in preds) / n
    ss_res = sum((p - y) ** 2 for p, y in preds)
    ss_tot = sum((y - mean_y) ** 2 for _, y in preds) or 1e-9
    r2 = 1 - ss_res / ss_tot
    print(f"  {label}: n={n}  MAE={mae:.2f} MWh/h  R2={r2:.3f}  (mean gen {mean_y:.1f})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--refresh", action="store_true", help="re-fetch gen actuals")
    args = ap.parse_args()

    end_date = (datetime.now(_CT) - timedelta(days=1)).strftime("%Y-%m-%d")
    holdout_start = (datetime.now(_CT) - timedelta(days=HOLDOUT_DAYS)).strftime("%Y-%m-%d")
    gen = fetch_gen(BACKTEST_START_DATE, end_date, args.refresh)

    cond_by_coords: dict[tuple, dict] = {}
    curves = {}
    for asset in DART_ASSETS:
        cfg = ASSET_CONFIG[asset]
        lat, lon = cfg.get("latitude"), cfg.get("longitude")
        if lat is None:
            continue
        if (lat, lon) not in cond_by_coords:
            print(f"\nXWeather conditions backfill for ({lat}, {lon}):")
            cond_by_coords[(lat, lon)] = fetch_conditions(lat, lon, BACKTEST_START_DATE, end_date)
        cond = cond_by_coords[(lat, lon)]

        var = "wind" if cfg["tech"] == "wind" else "ghi"
        bin_width = WIND_BIN_MPH if var == "wind" else GHI_BIN_WM2
        # Wind curves are fit per day/night stratum: nocturnal shear in West Texas
        # means the same 10m wind maps to very different hub-height wind after dark.
        # GHI > 10 W/m2 is the stratum test -- observable in BOTH /conditions (fitting)
        # and /forecasts (application), so no sunrise math and no train/apply mismatch.
        train: dict[str, list] = {}
        hold: dict[str, list] = {}
        for key, g in gen.get(asset, {}).items():
            wx = cond.get(key)
            if not wx or wx.get(var) is None or g.get("gen_mwh") is None:
                continue
            if var == "wind":
                stratum = "day" if (wx.get("ghi") or 0) > 10 else "night"
            else:
                stratum = "all"
            pair = (float(wx[var]), float(g["gen_mwh"]))
            bucket = hold if key[:10] >= holdout_start else train
            bucket.setdefault(stratum, []).append(pair)

        n_train, n_hold = sum(map(len, train.values())), sum(map(len, hold.values()))
        print(f"\n=== {asset} ({cfg['tech']}, curve on {var}) ===")
        print(f"  joined hours: train={n_train}, holdout={n_hold}")
        strata = {}
        for stratum, pairs in train.items():
            curve = fit_binned_curve(pairs, bin_width)
            if not curve:
                continue
            strata[stratum] = curve
            score(pairs, curve, f"in-sample  [{stratum}]")
            score(hold.get(stratum, []), curve, f"holdout {HOLDOUT_DAYS}d [{stratum}]")
            gaps = [(c, v["q75"] - v["median"]) for c, v in curve.items() if v["q75"] > 0]
            worst = max(gaps, key=lambda t: t[1]) if gaps else None
            if worst:
                print(f"    [{stratum}] bins={len(curve)}, top bin {sorted(curve)[-1]} {var}, "
                      f"largest q75-median gap {worst[1]:.1f} MWh at {worst[0]} {var}")
        if not strata:
            print("  no curve fit (insufficient data)")
            continue
        curves[asset] = {"kind": var, "bin_width": bin_width, "fitted_through": end_date,
                         "strata": strata}

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CURVES_OUT, "w") as f:
        json.dump(curves, f, indent=1)
    print(f"\nWrote {CURVES_OUT} ({len(curves)} asset curves)")
    print("Reminder: holdout R2 here is the CEILING (curve applied to observed weather).")
    print("Forecast-weather error stacks on top -- that's what wx_trial snapshots measure.")


if __name__ == "__main__":
    main()
