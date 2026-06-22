"""Thin client for the Vaisala XWeather hail endpoints.

Auth is "userless": an API ID + secret passed as query params on every request
(XWeather calls them client_id / client_secret). Read from the environment:
    XWEATHER_CLIENT_ID
    XWEATHER_CLIENT_SECRET

Every response is wrapped as {success, error, response}. We unwrap to `response`
(a list) and return [] on any failure, logging the error -- the dashboard treats
"no data" and "fetch failed" the same way (shows nothing rather than crashing).
"""
import logging
import os

import requests

from hail_tracker.config import XWEATHER_BASE_URL

logger = logging.getLogger(__name__)


class XWeatherError(RuntimeError):
    """Raised for auth/credential problems the UI should surface explicitly."""


def _credentials() -> tuple[str, str]:
    cid = os.getenv("XWEATHER_CLIENT_ID")
    secret = os.getenv("XWEATHER_CLIENT_SECRET")
    if not cid or not secret:
        raise XWeatherError(
            "XWEATHER_CLIENT_ID / XWEATHER_CLIENT_SECRET not set in environment (.env)."
        )
    return cid, secret


def _get(endpoint: str, action: str, **params) -> list:
    """GET {BASE}/{endpoint}/{action}; return the unwrapped `response` list.

    `from`/`to` are XWeather query params but `from` is a Python keyword, so
    callers pass `from_`/`to_`; we strip the trailing underscore here.
    """
    cid, secret = _credentials()
    params = {k.rstrip("_"): v for k, v in params.items()}
    params.update(client_id=cid, client_secret=secret)
    url = f"{XWEATHER_BASE_URL}/{endpoint}/{action}"
    try:
        resp = requests.get(url, params=params, timeout=30)
    except requests.RequestException as e:
        logger.warning("XWeather request error %s/%s: %s", endpoint, action, e)
        return []

    try:
        body = resp.json()
    except ValueError:
        logger.warning("XWeather non-JSON %s/%s (HTTP %s)", endpoint, action, resp.status_code)
        return []

    if not body.get("success"):
        err = body.get("error") or {}
        code = err.get("code", "")
        # Credential problems are worth surfacing loudly to the UI.
        if code in ("invalid_client", "unauthorized_appid", "invalid_grant", "no_subscription"):
            raise XWeatherError(f"XWeather auth/subscription error: {err}")
        # "no_data" / "warn_no_data" just mean the all-clear -- not an error.
        if code not in ("", "no_data", "warn_no_data"):
            logger.info("XWeather %s/%s returned %s", endpoint, action, err)
        return []

    response = body.get("response", [])
    return response if isinstance(response, list) else [response]


def nearest_place(lat: float, lon: float) -> dict | None:
    """Closest named populated place to the point (for labeling the site)."""
    rows = _get("places", "closest", p=f"{lat},{lon}", limit=1)
    return rows[0] if rows else None


def point_hail_threats(lat: float, lon: float) -> list:
    """Forward nowcast hail threats AT the point. Empty list == all clear."""
    return _get("hail/threats", f"{lat},{lon}")


def nearby_storm_cells(lat: float, lon: float, radius: str, limit: int) -> list:
    """Active radar storm cells near the point, nearest-first, each with a hail
    assessment, movement vector, and (when available) a forecast track."""
    return _get("stormcells", "closest", p=f"{lat},{lon}", radius=radius, limit=limit)


def hail_archive_day(lat: float, lon: float, from_iso: str, to_iso: str) -> list:
    """Historical hourly hail series at the point for a <=24h UTC window."""
    return _get("hail/archive", f"{lat},{lon}", from_=from_iso, to_=to_iso)
