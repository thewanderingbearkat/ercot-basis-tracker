"""PJM Constraint Map, exposed as a Flask Blueprint mounted on the host app.

    GET /pjm-constraints                 -- HTML page
    GET /api/pjm/congestion              -- live node congestion + hub basis + history
    GET /api/pjm/attribution?site=&days= -- daily constraint attribution (modeled shares
                                            x authoritative RTCONG)
    GET /api/pjm/sites                   -- our PJM sites
"""
import logging
import os
import time
from dataclasses import asdict

from flask import Blueprint, jsonify, render_template, request

from constraint_map.geo import load_basemap

from .attribution import daily_attribution
from .basis import basis_decomposition
from .binding import binding_constraints
from .congestion import node_congestion
from .mapping import driver_map
from .sites import SITES

PJM_BASEMAP = os.path.join(os.path.dirname(__file__), "..", "data", "pjm_transmission_lines.geojson")

logger = logging.getLogger(__name__)

pjm_constraints_bp = Blueprint(
    "pjm_constraints",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)

CONG_CACHE_TTL = 300.0
_cong_cache = None
_cong_cache_at = 0.0

ATTRIB_CACHE_TTL = 3600.0
_attrib_cache: dict = {}


def _sites_payload():
    return [asdict(s) for s in SITES.values()]


@pjm_constraints_bp.route("/pjm-constraints")
def index():
    return render_template("pjm_constraint_map.html", sites=_sites_payload())


@pjm_constraints_bp.route("/api/pjm/sites")
def api_sites():
    return jsonify({"sites": _sites_payload()})


@pjm_constraints_bp.route("/api/pjm/basemap")
def api_basemap():
    """Nationwide HIFLD transmission basemap (>=69kV), gzipped -- shared with the
    ERCOT tab. The client filters by voltage class, so min_kv is ignored."""
    from constraint_map.web import serve_basemap
    return serve_basemap()


@pjm_constraints_bp.route("/api/pjm/congestion")
def api_congestion():
    global _cong_cache, _cong_cache_at
    fresh = request.args.get("fresh") == "1"
    age = time.time() - _cong_cache_at
    if not fresh and _cong_cache is not None and age < CONG_CACHE_TTL:
        return jsonify({**_cong_cache, "cache_age_seconds": round(age, 1)})
    try:
        data = node_congestion(days=int(request.args.get("days", 7)))
    except Exception as e:
        logger.exception("pjm node_congestion failed")
        return jsonify({"error": str(e)}), 502
    _cong_cache, _cong_cache_at = data, time.time()
    return jsonify({**data, "cache_age_seconds": 0})


def _window_args():
    """(days, start, end) -- preset days (up to ~3y) or a custom start/end range."""
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    try:
        days = max(1, min(1100, int(request.args.get("days", 1))))
    except ValueError:
        days = 1
    return days, start, end


@pjm_constraints_bp.route("/api/pjm/attribution")
def api_attribution():
    site = request.args.get("site", next(iter(SITES)))
    days, start, end = _window_args()
    key = (site, days, start, end)
    hit = _attrib_cache.get(key)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    if site not in SITES:
        return jsonify({"error": f"unknown site {site}"}), 400
    try:
        data = daily_attribution(site, days=days, start=start, end=end)
    except Exception as e:
        logger.exception("pjm daily_attribution failed")
        return jsonify({"error": str(e)}), 502
    _attrib_cache[key] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})


_basis_cache: dict = {}


@pjm_constraints_bp.route("/api/pjm/basis")
def api_basis():
    site = request.args.get("site", next(iter(SITES)))
    days, start, end = _window_args()
    key = (site, days, start, end)
    hit = _basis_cache.get(key)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    if site not in SITES:
        return jsonify({"error": f"unknown site {site}"}), 400
    try:
        data = basis_decomposition(site, days=days, start=start, end=end)
    except Exception as e:
        logger.exception("pjm basis_decomposition failed")
        return jsonify({"error": str(e)}), 502
    _basis_cache[key] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})


# Authoritative system-wide binding constraints (not node-specific). Keyed by
# window only -- doesn't depend on the selected site.
_binding_cache: dict = {}


@pjm_constraints_bp.route("/api/pjm/binding")
def api_binding():
    days, start, end = _window_args()
    key = (days, start, end)
    hit = _binding_cache.get(key)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    try:
        data = binding_constraints(days=days, start=start, end=end)
    except Exception as e:
        logger.exception("pjm binding_constraints failed")
        return jsonify({"error": str(e)}), 502
    _binding_cache[key] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})


_map_cache: dict = {}


@pjm_constraints_bp.route("/api/pjm/map")
def api_map():
    site = request.args.get("site", next(iter(SITES)))
    days, start, end = _window_args()
    key = (site, days, start, end)
    hit = _map_cache.get(key)
    if hit and (time.time() - hit[0]) < ATTRIB_CACHE_TTL and request.args.get("fresh") != "1":
        return jsonify({**hit[1], "cache_age_seconds": round(time.time() - hit[0], 1)})
    if site not in SITES:
        return jsonify({"error": f"unknown site {site}"}), 400
    try:
        data = driver_map(site, days=days, start=start, end=end)
    except Exception as e:
        logger.exception("pjm driver_map failed")
        return jsonify({"error": str(e)}), 502
    _map_cache[key] = (time.time(), data)
    return jsonify({**data, "cache_age_seconds": 0})
