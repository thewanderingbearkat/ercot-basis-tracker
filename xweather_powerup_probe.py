"""One-shot capability probe for the paid XWeather PowerUp subscription.

WHY: the PowerUp upgrade may unlock endpoints/fields the free tier didn't have
(site-level energy output via /energy/farm, 80/100m winds in /forecasts, solar
irradiance archives). Before wiring anything into the shadow trader, run this
once to learn exactly what the new credentials can see:

    python xweather_powerup_probe.py

For each shadow-trader site it reports, per endpoint: accessible or not, how far
forward the forecast runs, and every scalar field name the response carries
(so the snapshot/scoring scripts can map real field names instead of guesses).
It also lists the /energy/farm candidates nearest each site -- pick the right
farm IDs from that list and paste them into FARM_IDS in wx_trial_snapshot.py.

Read-only, ~a dozen API hits total. Uses XWEATHER_CLIENT_ID / XWEATHER_CLIENT_SECRET
from .env (drop the new PowerUp app credentials in there first).
"""
import os
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from shadow_trader.config import ASSET_CONFIG  # noqa: E402

BASE_URL = "https://data.api.xweather.com"
TIMEOUT = 40


def _creds() -> dict:
    cid, secret = os.getenv("XWEATHER_CLIENT_ID"), os.getenv("XWEATHER_CLIENT_SECRET")
    if not cid or not secret:
        raise SystemExit("XWEATHER_CLIENT_ID / XWEATHER_CLIENT_SECRET not set in .env")
    return {"client_id": cid, "client_secret": secret}


def _get(path: str, **params):
    """GET {BASE}/{path}; returns (unwrapped response, error_code, interesting headers)."""
    params.update(_creds())
    try:
        resp = requests.get(f"{BASE_URL}/{path}", params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        return None, f"network:{e.__class__.__name__}", {}
    headers = {k: v for k, v in resp.headers.items()
               if "ratelimit" in k.lower() or "hits" in k.lower() or "remaining" in k.lower()}
    try:
        body = resp.json()
    except ValueError:
        return None, f"non-json HTTP {resp.status_code}", headers
    if not body.get("success"):
        err = (body.get("error") or {}).get("code", f"HTTP {resp.status_code}")
        return None, err, headers
    r = body.get("response", [])
    return (r if isinstance(r, list) else [r]), None, headers


def scalar_fields(obj: dict, prefix: str = "") -> list[str]:
    """Flatten one record to dotted scalar field names (one nesting level is enough here)."""
    out = []
    for k, v in obj.items():
        name = f"{prefix}{k}"
        if isinstance(v, dict):
            out.extend(scalar_fields(v, prefix=f"{name}."))
        elif not isinstance(v, list):
            out.append(name)
    return sorted(out)


def probe_forecasts(lat: float, lon: float) -> None:
    rows, err, hdrs = _get(f"forecasts/{lat},{lon}", filter="1hr", limit=260)
    if err:
        print(f"  /forecasts ............... BLOCKED ({err})")
        return
    periods = rows[0].get("periods", []) if rows else []
    if not periods:
        print("  /forecasts ............... ok but 0 periods")
        return
    first, last = periods[0]["dateTimeISO"], periods[-1]["dateTimeISO"]
    print(f"  /forecasts ............... OK: {len(periods)} hourly periods, {first} -> {last}")
    print(f"      fields: {', '.join(scalar_fields(periods[0]))}")
    if hdrs:
        print(f"      quota headers: {hdrs}")


def probe_conditions(lat: float, lon: float) -> None:
    y = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    t = datetime.now().strftime("%Y-%m-%d")
    rows, err, _ = _get(f"conditions/{lat},{lon}", filter="1hr", **{"from": y, "to": t})
    if err:
        print(f"  /conditions (actuals) .... BLOCKED ({err})")
        return
    periods = rows[0].get("periods", []) if rows else []
    print(f"  /conditions (actuals) .... OK: {len(periods)} hourly periods for {y}")
    if periods:
        print(f"      fields: {', '.join(scalar_fields(periods[0]))}")


def probe_energy_farms(lat: float, lon: float) -> None:
    rows, err, _ = _get("energy/farm/closest", p=f"{lat},{lon}", limit=6)
    if err:
        print(f"  /energy/farm/closest ..... BLOCKED ({err})")
        return
    print(f"  /energy/farm/closest ..... OK: {len(rows)} farms near site")
    farm_id = None
    for r in rows:
        place = r.get("place") or {}
        prof = r.get("profile") or {}
        rid = r.get("id") or place.get("name")
        label = prof.get("name") or place.get("name") or "?"
        kind = prof.get("type") or prof.get("fuelType") or "?"
        cap = prof.get("capacityMW") or prof.get("capacity") or "?"
        print(f"      id={rid!r:40} name={label!r} type={kind} capacity={cap}")
        farm_id = farm_id or rid
    if farm_id:
        frows, ferr, _ = _get(f"energy/farm/{farm_id}", filter="1hr", limit=260)
        if ferr:
            print(f"      forecast for {farm_id!r}: BLOCKED ({ferr})")
            return
        periods = (frows[0].get("periods", []) if frows else [])
        if periods:
            print(f"      forecast for {farm_id!r}: {len(periods)} periods, "
                  f"{periods[0].get('dateTimeISO')} -> {periods[-1].get('dateTimeISO')}")
            print(f"      fields: {', '.join(scalar_fields(periods[0]))}")
        else:
            print(f"      forecast for {farm_id!r}: ok but 0 periods; top-level fields: "
                  f"{', '.join(scalar_fields(frows[0])) if frows else 'none'}")


def probe_irradiance_archive(lat: float, lon: float) -> None:
    frm = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")
    to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows, err, _ = _get("renewables/irradiance/archive", p=f"{lat},{lon}", **{"from": frm, "to": to})
    if err:
        print(f"  /renewables/irradiance ... BLOCKED ({err})")
        return
    print(f"  /renewables/irradiance ... OK ({len(rows)} records {frm} -> {to})")
    if rows:
        sample = rows[0].get("periods", [rows[0]])
        sample = sample[0] if isinstance(sample, list) and sample else rows[0]
        print(f"      fields: {', '.join(scalar_fields(sample))}")


def main() -> None:
    print(f"XWeather PowerUp capability probe -- {datetime.now():%Y-%m-%d %H:%M}")
    seen_coords = set()
    for asset, cfg in ASSET_CONFIG.items():
        lat, lon = cfg.get("latitude"), cfg.get("longitude")
        if lat is None or (lat, lon) in seen_coords:
            continue
        seen_coords.add((lat, lon))
        names = [a for a, c in ASSET_CONFIG.items()
                 if (c.get("latitude"), c.get("longitude")) == (lat, lon)]
        print(f"\n=== {'/'.join(names)} ({lat}, {lon}) [{cfg.get('tech')}] ===")
        probe_forecasts(lat, lon)
        probe_conditions(lat, lon)
        probe_energy_farms(lat, lon)
        if cfg.get("tech") == "solar":
            probe_irradiance_archive(lat, lon)
    print("\nNext step: copy the correct farm ids into FARM_IDS in wx_trial_snapshot.py")


if __name__ == "__main__":
    main()
