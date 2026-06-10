"""Plant-level weather forecasts from Open-Meteo (free, keyless) for trader risk gating.

Two endpoints, same request/response shape:
  - api.open-meteo.com .................. forward forecasts (used by generate_bids)
  - historical-forecast-api.open-meteo.com  archived model forecasts (used by backtests)

The historical-forecast endpoint returns what the weather models *predicted at the time*,
not reanalysis of what actually happened. That matters for backtesting: gating decisions
must be reproducible from information available before the DA bid deadline, otherwise the
backtest has lookahead bias and overstates the strategy.

We query several NWP models per request (best_match, GFS, ICON) and keep, per hour:
  - the cross-model mean of each variable (our working forecast), and
  - the cross-model spread (population stdev) of hub-height wind and cloud cover.
Model disagreement is the cheapest honest uncertainty signal available without paying for
an ensemble feed -- when GFS and ICON diverge on tomorrow's wind, a real desk sizes down.

Variables (units chosen to match how the desk talks about weather):
  temp_f         2m temperature, degF
  wind_mph       80m wind speed, mph (closest common-denominator level to hub height)
  gust_mph       10m wind gusts, mph
  cloud_pct      total cloud cover, %
  wind_spread    cross-model stdev of wind_mph
  cloud_spread   cross-model stdev of cloud_pct

Output is keyed 'YYYY-MM-DD HE##' in America/Chicago, matching the rest of the codebase.
"""
import logging
from statistics import mean, pstdev

import requests

from shadow_trader.config import ASSET_CONFIG

logger = logging.getLogger(__name__)

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# wind_speed_80m is the highest level available across all three models; ERCOT-class
# turbine hubs sit ~80-90m so it's a reasonable hub-height proxy for ramp/cut-out gating.
HOURLY_VARS = ["temperature_2m", "wind_speed_80m", "wind_gusts_10m", "cloud_cover"]
MODELS = ["best_match", "gfs_seamless", "icon_seamless"]


def _hour_key_from_local_iso(iso_str: str) -> str:
    """Open-Meteo returns hour-beginning local timestamps ('2026-06-10T13:00').
    Convert to the project's hour-ending key: HE = local hour + 1."""
    date_str, time_part = iso_str.split("T")
    he = int(time_part[:2]) + 1
    return f"{date_str} HE{he:02d}"


def _collect(hourly: dict, var: str) -> dict[int, list[float]]:
    """Gather each model's series for `var` -> {row_index: [model values...]}, skipping nulls.

    With multiple models requested, Open-Meteo suffixes keys per model
    (e.g. 'wind_speed_80m_gfs_seamless'). A single-model request uses the bare name.
    """
    by_idx: dict[int, list[float]] = {}
    candidates = [var] + [f"{var}_{m}" for m in MODELS]
    for key in candidates:
        series = hourly.get(key)
        if not series:
            continue
        for idx, val in enumerate(series):
            if val is None:
                continue
            by_idx.setdefault(idx, []).append(float(val))
    return by_idx


def fetch_point_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    historical: bool = False,
) -> dict:
    """Fetch hourly multi-model weather for one location. Returns {hour_key: {vars...}}.

    historical=True routes to the archived-forecast endpoint (coverage from 2022 onward);
    historical=False routes to the live forecast endpoint (up to ~16 days forward).
    """
    url = HISTORICAL_FORECAST_URL if historical else FORECAST_URL
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "models": ",".join(MODELS),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "America/Chicago",
    }
    try:
        resp = requests.get(url, params=params, timeout=60)
    except Exception as e:
        logger.warning("Open-Meteo fetch error (%s, %s): %s", latitude, longitude, e)
        return {}
    if resp.status_code != 200:
        logger.warning("Open-Meteo HTTP %s: %s", resp.status_code, resp.text[:200])
        return {}

    hourly = resp.json().get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        logger.warning("Open-Meteo returned no hourly data for (%s, %s)", latitude, longitude)
        return {}

    temp = _collect(hourly, "temperature_2m")
    wind = _collect(hourly, "wind_speed_80m")
    gust = _collect(hourly, "wind_gusts_10m")
    cloud = _collect(hourly, "cloud_cover")

    out = {}
    for idx, iso_str in enumerate(times):
        record = {}
        t, w, g, c = temp.get(idx), wind.get(idx), gust.get(idx), cloud.get(idx)
        if t:
            record["temp_f"] = round(mean(t), 1)
        if w:
            record["wind_mph"] = round(mean(w), 1)
            record["wind_spread"] = round(pstdev(w), 1) if len(w) > 1 else 0.0
        if g:
            record["gust_mph"] = round(mean(g), 1)
        if c:
            record["cloud_pct"] = round(mean(c), 1)
            record["cloud_spread"] = round(pstdev(c), 1) if len(c) > 1 else 0.0
        if record:
            out[_hour_key_from_local_iso(iso_str)] = record
    return out


def fetch_asset_weather(
    asset_keys: list[str],
    start_date: str,
    end_date: str,
    historical: bool = False,
) -> dict:
    """Fetch weather for each asset's configured lat/lon. Returns {asset: {hour_key: {...}}}.

    Assets sharing coordinates (e.g. BKI/BKII at the same site) reuse one fetch.
    Assets with no coordinates configured get an empty dict -- the decision engine
    treats missing weather as 'no gate', so the strategy degrades to forecast-only.
    """
    out: dict[str, dict] = {}
    by_coords: dict[tuple, list[str]] = {}
    for asset in asset_keys:
        cfg = ASSET_CONFIG.get(asset, {})
        lat, lon = cfg.get("latitude"), cfg.get("longitude")
        if lat is None or lon is None:
            logger.warning("No coordinates configured for %s; skipping weather", asset)
            out[asset] = {}
            continue
        by_coords.setdefault((lat, lon), []).append(asset)

    for (lat, lon), assets in by_coords.items():
        logger.info(
            "Fetching %s weather for %s at (%.3f, %.3f), %s -> %s",
            "historical-forecast" if historical else "forward-forecast",
            "/".join(assets), lat, lon, start_date, end_date,
        )
        wx = fetch_point_weather(lat, lon, start_date, end_date, historical=historical)
        logger.info("  %d hours of weather returned", len(wx))
        for asset in assets:
            out[asset] = wx
    return out
