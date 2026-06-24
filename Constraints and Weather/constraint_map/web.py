"""West Texas constraint map, exposed as a Flask Blueprint mounted on the host app.

    GET /constraints                  -- HTML map (Leaflet): TX grid, our sites,
                                         live binding constraints drawn as segments
    GET /api/constraints/active       -- live binding constraints + per-site impact
                                         + endpoint geometry (cached briefly)
    GET /api/constraints/basemap      -- TX transmission lines basemap (>=min_kv)
    GET /api/constraints/sites        -- our four sites with coordinates

Routes use bare paths; the host app registers the blueprint directly (see app.py).
"""
import logging
import os
import time
from dataclasses import asdict

from flask import Blueprint, jsonify, render_template, request

from .analytics import historical_attribution, price_bridge
from .basis import basis_decomposition
from .constraints import active_constraints
from .geo import attach_geometry, load_basemap
from .sites import SITES

logger = logging.getLogger(__name__)

constraints_bp = Blueprint(
    "constraints",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)

# Active constraints move only every SCED interval (~5 min); cache to match that
# cadence so a page with several clients doesn't re-hit Snowflake on every poll.
ACTIVE_CACHE_TTL = 300.0
_active_cache: dict | None = None
_active_cache_at: float = 0.0


def _sites_payload() -> list[dict]:
    return [asdict(s) for s in SITES.values()]


@constraints_bp.route("/constraints")
def index():
    return render_template("constraint_map.html", sites=_sites_payload())


@constraints_bp.route("/api/constraints/active")
def api_active():
    """Live binding constraints with per-site impact and endpoint geometry.
    Cached for ACTIVE_CACHE_TTL seconds; ?fresh=1 bypasses the cache."""
    global _active_cache, _active_cache_at
    fresh = request.args.get("fresh") == "1"
    age = time.time() - _active_cache_at
    if not fresh and _active_cache is not None and age < ACTIVE_CACHE_TTL:
        return jsonify({**_active_cache, "cache_age_seconds": round(age, 1)})
    try:
        data = attach_geometry(active_constraints())
    except Exception as e:  # surface DB/credential errors to the UI, don't 500
        logger.exception("active_constraints failed")
        return jsonify({"error": str(e)}), 502
    _active_cache = data
    _active_cache_at = time.time()
    return jsonify({**data, "cache_age_seconds": 0})


@constraints_bp.route("/api/constraints/basemap")
def api_basemap():
    """TX transmission-line basemap. ?min_kv=N filters by voltage (default 100)."""
    try:
        min_kv = int(request.args.get("min_kv", 100))
    except ValueError:
        min_kv = 100
    return jsonify(load_basemap(min_kv))


@constraints_bp.route("/api/constraints/sites")
def api_sites():
    return jsonify({"sites": _sites_payload()})


# Point-in-time price bridge (hub -> constraints -> site RT LMP). Cached like /active.
_bridge_cache: dict | None = None
_bridge_cache_at: float = 0.0


@constraints_bp.route("/api/constraints/bridge")
def api_bridge():
    global _bridge_cache, _bridge_cache_at
    fresh = request.args.get("fresh") == "1"
    age = time.time() - _bridge_cache_at
    if not fresh and _bridge_cache is not None and age < ACTIVE_CACHE_TTL:
        return jsonify({**_bridge_cache, "cache_age_seconds": round(age, 1)})
    try:
        data = price_bridge()
    except Exception as e:
        logger.exception("price_bridge failed")
        return jsonify({"error": str(e)}), 502
    _bridge_cache, _bridge_cache_at = data, time.time()
    return jsonify({**data, "cache_age_seconds": 0})


# Historical ATC attribution. Heavier query (aggregates the shift-factor table over
# a window), so cache longer and key the cache by the requested day count.
_attrib_cache: dict[int, tuple[float, dict]] = {}
ATTRIB_CACHE_TTL = 3600.0


@constraints_bp.route("/api/constraints/attribution")
def api_attribution():
    try:
        days = max(1, min(90, int(request.args.get("days", 30))))
    except ValueError:
        days = 30
    hit = _attrib_cache.get(days)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    try:
        data = historical_attribution(days)
    except Exception as e:
        logger.exception("historical_attribution failed")
        return jsonify({"error": str(e)}), 502
    _attrib_cache[days] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})


# Basis decomposition over a window (preset days anchored to the last full
# operating day, or a custom start/end): which constraints drive node vs hub,
# plus map geometry. Reads staged daily aggregates + a live tail.
_basis_cache: dict[tuple, tuple[float, dict]] = {}


@constraints_bp.route("/api/constraints/basis")
def api_basis():
    site = request.args.get("site", next(iter(SITES)))
    if site not in SITES:
        return jsonify({"error": f"unknown site {site}"}), 400
    start = request.args.get("start") or None     # custom range (YYYY-MM-DD)
    end = request.args.get("end") or None
    try:
        days = max(1, min(1100, int(request.args.get("days", 1))))   # up to ~3y
    except ValueError:
        days = 1
    key = (site, days, start, end)
    hit = _basis_cache.get(key)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    try:
        data = basis_decomposition(site, days=days, start=start, end=end)
    except Exception as e:
        logger.exception("ercot basis_decomposition failed")
        return jsonify({"error": str(e)}), 502
    _basis_cache[key] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})
