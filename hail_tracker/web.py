"""Hail monitor, exposed as a Flask Blueprint and mounted on the host app.

    GET /hail                       -- HTML dashboard tab (Leaflet storm map)
    GET /api/hail/status            -- live site status + assessed storm cells (cached)
    GET /api/hail/archive?date=...  -- historical hourly hail at the site for a UTC day
    GET /api/hail/radar/<z>/<x>/<y>.png -- proxied XWeather radar tiles (secret stays server-side)

Routes use bare paths; the host app registers the blueprint directly (see app.py).
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, Response, jsonify, render_template, request

from hail_tracker import xweather
from hail_tracker.assess import build_assessment
from hail_tracker.config import (
    CELL_SEARCH_LIMIT,
    CELL_SEARCH_RADIUS,
    SITE,
    STATUS_CACHE_TTL,
    THRESHOLDS,
    XWEATHER_BASE_URL,
)

logger = logging.getLogger(__name__)

hail_bp = Blueprint(
    "hail",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)

# Tiny in-process cache for the live status, keyed by nothing (single site).
_status_cache: dict | None = None
_status_cache_at: float = 0.0


def _build_status(site: dict | None = None) -> dict:
    """Fetch + assess the current hail picture for `site` (defaults to Holstein)."""
    site = site or SITE
    lat, lon = site["lat"], site["lon"]
    cells = xweather.nearby_storm_cells(lat, lon, CELL_SEARCH_RADIUS, CELL_SEARCH_LIMIT)
    threats = xweather.point_hail_threats(lat, lon)
    assessment = build_assessment(cells, threats, lat, lon)
    assessment.update(
        site=site,
        thresholds=THRESHOLDS,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
    return assessment


def _override_site() -> dict | None:
    """Optional ?lat=&lon=(&name=) override, for testing the monitor against active
    storms anywhere without waiting for weather to reach Holstein. Returns None when
    no valid override is supplied (the normal Holstein case)."""
    lat, lon = request.args.get("lat"), request.args.get("lon")
    if lat is None or lon is None:
        return None
    try:
        site = {"key": "OVERRIDE", "name": request.args.get("name", "Custom location"),
                "lat": float(lat), "lon": float(lon)}
    except ValueError:
        return None
    return site


@hail_bp.route("/hail")
def index():
    return render_template("hail_dashboard.html", site=SITE)


@hail_bp.route("/api/hail/status")
def status():
    """Live status. Cached for STATUS_CACHE_TTL seconds; ?fresh=1 bypasses the cache."""
    global _status_cache, _status_cache_at
    override = _override_site()
    # Overridden (demo/test) locations bypass the single-slot Holstein cache.
    if override is not None:
        try:
            return jsonify({**_build_status(override), "cache_age_seconds": 0})
        except xweather.XWeatherError as e:
            return jsonify({"error": str(e)}), 502

    fresh = request.args.get("fresh") == "1"
    age = time.time() - _status_cache_at
    if not fresh and _status_cache is not None and age < STATUS_CACHE_TTL:
        return jsonify({**_status_cache, "cache_age_seconds": round(age, 1)})
    try:
        data = _build_status()
    except xweather.XWeatherError as e:
        return jsonify({"error": str(e)}), 502
    _status_cache = data
    _status_cache_at = time.time()
    return jsonify({**data, "cache_age_seconds": 0})


@hail_bp.route("/api/hail/archive")
def archive():
    """Historical hourly hail at the site for a single UTC day (?date=YYYY-MM-DD,
    defaults to yesterday). Returns the raw hourly series for charting."""
    date_str = request.args.get("date")
    try:
        day = (
            datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if date_str
            else datetime.now(timezone.utc) - timedelta(days=1)
        )
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400

    day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    frm = day.strftime("%Y-%m-%dT%H:%M:%SZ")
    to = (day + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        rows = xweather.hail_archive_day(SITE["lat"], SITE["lon"], frm, to)
    except xweather.XWeatherError as e:
        return jsonify({"error": str(e)}), 502
    return jsonify({"date": day.strftime("%Y-%m-%d"), "site": SITE, "series": rows})


# XWeather raster radar tiles. We proxy them so the client_secret never reaches the
# browser. If the trial doesn't include the Maps product these 403, and the UI just
# leaves the radar layer toggled off -- harmless.
_RADAR_LAYER = "radar"


@hail_bp.route("/api/hail/radar/<int:z>/<int:x>/<int:y>.png")
def radar_tile(z: int, x: int, y: int):
    cid = os.getenv("XWEATHER_CLIENT_ID")
    secret = os.getenv("XWEATHER_CLIENT_SECRET")
    if not cid or not secret:
        return Response(status=503)
    url = f"https://maps.api.xweather.com/{cid}_{secret}/{_RADAR_LAYER}/{z}/{x}/{y}/current.png"
    try:
        r = requests.get(url, timeout=15)
    except requests.RequestException:
        return Response(status=502)
    if r.status_code != 200:
        return Response(status=r.status_code)
    return Response(r.content, content_type=r.headers.get("Content-Type", "image/png"))
