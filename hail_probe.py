"""Throwaway smoke test for the Vaisala XWeather (AerisWeather) hail API.

Goal: confirm the trial credentials work and see what real hail payloads look
like for Holstein Solar BEFORE committing to a tab. Not wired into the app.

Run:
    python hail_probe.py                 # Holstein + national schema peek
    python hail_probe.py 2024-05-25      # also pull hail/archive for that UTC day

Needs in .env (from your XWeather trial dashboard -> Apps):
    XWEATHER_CLIENT_ID=...
    XWEATHER_CLIENT_SECRET=...
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data.api.xweather.com"

# Holstein Solar -- Wingate / Nolan County, TX (matches shadow_trader/config.py).
HOLSTEIN_LAT, HOLSTEIN_LON = 32.03, -100.45
LOC = f"{HOLSTEIN_LAT},{HOLSTEIN_LON}"

CLIENT_ID = os.getenv("XWEATHER_CLIENT_ID")
CLIENT_SECRET = os.getenv("XWEATHER_CLIENT_SECRET")


def call(endpoint: str, action: str, **extra_params) -> dict:
    """GET {BASE_URL}/{endpoint}/{action} with auth + any extra query params.

    `from` is a Python keyword, so callers pass `from_`; translate it back.
    """
    extra_params = {k.rstrip("_"): v for k, v in extra_params.items()}
    params = {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, **extra_params}
    url = f"{BASE_URL}/{endpoint}/{action}"
    resp = requests.get(url, params=params, timeout=30)
    label = f"{endpoint}/{action}" + (f"?p={extra_params['p']}" if "p" in extra_params else "")
    print(f"\n=== {label}  ->  HTTP {resp.status_code} ===")
    try:
        body = resp.json()
    except ValueError:
        print(resp.text[:500])
        return {}
    if not body.get("success"):
        print("API error:", body.get("error"))
    resp_obj = body.get("response", body)
    n = len(resp_obj) if isinstance(resp_obj, list) else "n/a"
    print(f"(response items: {n})")
    print(json.dumps(resp_obj, indent=2)[:2500])
    return body


def main() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit(
            "Missing XWEATHER_CLIENT_ID / XWEATHER_CLIENT_SECRET in .env.\n"
            "Grab them from your XWeather trial dashboard (Apps section)."
        )

    # 1. Sanity check that auth + location parsing work -- nearest place to Holstein.
    call("places", "closest", p=LOC, limit=1)

    # 2. The live predictor: nowcast hail threat AT the plant (empty == all clear).
    call("hail/threats", LOC)

    # 3. Storm cells near the plant + forecast tracks (lead time). Wide radius so we
    #    catch anything in the region; empty == no active cells nearby right now.
    call("stormcells", "closest", p=LOC, radius="250mi", limit=5)

    # 4. SCHEMA PEEK: nearest active hail threat / storm cell ANYWHERE in CONUS, so we
    #    can see the real field layout even while Holstein is calm. Big radius.
    call("hail/threats", "closest", p=LOC, radius="2000mi", limit=2)
    call("stormcells", "closest", p="39.0,-98.0", radius="2000mi", limit=2)

    # 5. Historical hail at the plant for a given UTC day (24h window per the docs).
    #    Pass a date as argv[1] to probe a specific (ideally stormy) day.
    if len(sys.argv) > 1:
        day = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        frm = day.strftime("%Y-%m-%dT%H:%M:%SZ")
        to = (day + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        call("hail/archive", LOC, from_=frm, to=to)


if __name__ == "__main__":
    main()
