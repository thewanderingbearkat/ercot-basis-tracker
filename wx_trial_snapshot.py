"""Daily dual-source forecast snapshot for the XWeather PowerUp vs Open-Meteo trial.

WHY: XWeather has no archived-forecast endpoint, so we cannot backtest what
PowerUp *would have* predicted on past days. The only honest comparison is a
forward trial: every morning BEFORE the ERCOT DA bid deadline (10:00 CT),
snapshot what each source forecasts for the coming days, then score against
actuals once enough days accumulate. Same lookahead-bias rule the shadow
trader's backtests already follow (see shadow_trader/weather.py docstring).

Captured per shadow-trader site, per run:
  open_meteo        the exact multi-model mean/spread series the production
                    gating uses today (shadow_trader.weather.fetch_point_weather)
  xweather_forecast /forecasts point forecast, ALL scalar fields kept raw --
                    field mapping happens at scoring time, after the probe
                    (xweather_powerup_probe.py) tells us the real names
  powerup_ensemble  Vaisala PowerUp per-farm production forecast (51-member
                    ensemble reduced to mean + percentiles per hour), via
                    shadow_trader/powerup.py -- empty until farm entitlements
                    are approved on the PowerUp account

Baseline generation forecasts (ERCOT STWPF-share for BKI/BKII, Tenaska for
Holstein) are NOT re-fetched here: the production ledger already persists them
per bid (shadow_trader/ledger.py), so scoring joins against that.

Run manually or via Windows Task Scheduler daily at ~08:45 CT:
    python wx_trial_snapshot.py            # snapshot now (skips if today's exists)
    python wx_trial_snapshot.py --force    # re-snapshot today

Output: data/wx_trial/snap_YYYY-MM-DD.json (local machine only; data/ is
gitignored and Render's disk is ephemeral, so this trial runs on the local box).
"""
import argparse
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from shadow_trader.config import ASSET_CONFIG, DATA_DIR  # noqa: E402
from shadow_trader.weather import fetch_point_weather  # noqa: E402
from shadow_trader.powerup import fetch_asset_ensembles  # noqa: E402

BASE_URL = "https://data.api.xweather.com"
OUT_DIR = os.path.join(DATA_DIR, "wx_trial")
TIMEOUT = 40
FORWARD_DAYS = 7          # snapshot horizon; scoring mostly cares about D+1 (the DA bid day)
_CT = ZoneInfo("America/Chicago")

def _creds() -> dict:
    cid, secret = os.getenv("XWEATHER_CLIENT_ID"), os.getenv("XWEATHER_CLIENT_SECRET")
    if not cid or not secret:
        raise SystemExit("XWEATHER_CLIENT_ID / XWEATHER_CLIENT_SECRET not set in .env")
    return {"client_id": cid, "client_secret": secret}


def _get(path: str, **params) -> list:
    """GET {BASE}/{path}; unwrapped response list, [] on any failure (logged to stdout)."""
    params.update(_creds())
    try:
        resp = requests.get(f"{BASE_URL}/{path}", params=params, timeout=TIMEOUT)
        body = resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  ! {path}: {e.__class__.__name__}")
        return []
    if not body.get("success"):
        print(f"  ! {path}: {(body.get('error') or {}).get('code', f'HTTP {resp.status_code}')}")
        return []
    r = body.get("response", [])
    return r if isinstance(r, list) else [r]


def _hour_key(iso_str: str) -> str | None:
    """XWeather dateTimeISO (offset-aware, hour-beginning) -> 'YYYY-MM-DD HE##' in CT.

    Hour-beginning semantics assumed to match Open-Meteo; scoring should sanity-check
    this once real data is in hand (compare both sources' diurnal solar/wind shapes).
    """
    try:
        dt = datetime.fromisoformat(iso_str).astimezone(_CT)
    except ValueError:
        return None
    return f"{dt:%Y-%m-%d} HE{dt.hour + 1:02d}"


def _flatten_scalars(obj: dict, prefix: str = "") -> dict:
    """Keep every numeric/bool field (one nesting level dotted); drop strings except time."""
    out = {}
    for k, v in obj.items():
        name = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flatten_scalars(v, prefix=f"{name}."))
        elif isinstance(v, (int, float, bool)):
            out[name] = v
    return out


def _periods_to_hours(periods: list) -> dict:
    out = {}
    for p in periods:
        key = _hour_key(p.get("dateTimeISO", ""))
        if key:
            out[key] = _flatten_scalars(p)
    return out


def fetch_xweather_forecast(lat: float, lon: float) -> dict:
    rows = _get(f"forecasts/{lat},{lon}", filter="1hr", limit=FORWARD_DAYS * 24 + 24)
    periods = rows[0].get("periods", []) if rows else []
    return _periods_to_hours(periods)


def build_snapshot(issue_date: str) -> dict:
    start = (datetime.strptime(issue_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    end = (datetime.strptime(issue_date, "%Y-%m-%d") + timedelta(days=FORWARD_DAYS)).strftime("%Y-%m-%d")

    snap = {
        "issued_at": datetime.now(_CT).isoformat(),
        "issue_date": issue_date,
        "window": {"start": start, "end": end},
        "assets": {},
    }

    # PowerUp per-farm ensemble forecasts (one GetList + one call per farm).
    # Never lets a PowerUp failure kill the weather snapshot.
    try:
        ensembles = fetch_asset_ensembles(
            {a: c["tech"] for a, c in ASSET_CONFIG.items() if c.get("latitude")})
    except Exception as e:
        print(f"  ! powerup ensembles: {e.__class__.__name__}: {e}")
        ensembles = {}

    # Sites sharing coordinates (BKI/BKII) reuse one fetch per source, like production.
    point_cache: dict[tuple, dict] = {}
    for asset, cfg in ASSET_CONFIG.items():
        lat, lon = cfg.get("latitude"), cfg.get("longitude")
        if lat is None or lon is None:
            continue
        print(f"{asset} ({lat}, {lon}):")
        if (lat, lon) not in point_cache:
            om = fetch_point_weather(lat, lon, start, end)
            print(f"  open-meteo: {len(om)} hours")
            xw = fetch_xweather_forecast(lat, lon)
            print(f"  xweather /forecasts: {len(xw)} hours")
            point_cache[(lat, lon)] = {"open_meteo": om, "xweather_forecast": xw}
        entry = dict(point_cache[(lat, lon)])

        pu = ensembles.get(asset) or {}
        entry["powerup_ensemble"] = pu or None
        print(f"  powerup ensemble: {len(pu.get('hours', {}))} hours")
        snap["assets"][asset] = entry
    return snap


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="overwrite today's snapshot")
    args = ap.parse_args()

    issue_date = datetime.now(_CT).strftime("%Y-%m-%d")
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f"snap_{issue_date}.json")
    if os.path.exists(out_path) and not args.force:
        print(f"{out_path} already exists; use --force to overwrite.")
        return

    snap = build_snapshot(issue_date)
    with open(out_path, "w") as f:
        json.dump(snap, f, indent=1)
    n_hours = sum(len(a.get("xweather_forecast", {})) for a in snap["assets"].values())
    print(f"\nWrote {out_path} ({n_hours} xweather forecast hours across "
          f"{len(snap['assets'])} assets)")


if __name__ == "__main__":
    main()
