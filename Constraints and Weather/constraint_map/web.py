"""West Texas constraint map, exposed as a Flask Blueprint mounted on the host app.

    GET /constraints                  -- HTML map (Leaflet): TX grid, our sites,
                                         live binding constraints drawn as segments
    GET /api/constraints/active       -- live binding constraints + per-site impact
                                         + endpoint geometry (cached briefly)
    GET /api/constraints/basemap      -- TX transmission lines basemap (>=min_kv)
    GET /api/constraints/sites        -- our four sites with coordinates

Routes use bare paths; the host app registers the blueprint directly (see app.py).
"""
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import asdict

from flask import Blueprint, Response, jsonify, render_template, request

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
    """Nationwide HIFLD transmission basemap (>=69kV), gzipped. The client filters
    by voltage class, so min_kv is accepted for compatibility but ignored."""
    return serve_basemap()


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


# ---------------------------------------------------------------------------
# Infrastructure overlays (shared by both map tabs). OpenStreetMap data, ODbL.
#   /api/infra/<layer>            -- static nationwide GeoJSON (wind/solar, data centers)
#   /api/infra/dense/<layer>      -- live viewport Overpass proxy (substations, pipelines)
#   /static/infra_layers.js       -- the shared frontend module
# ---------------------------------------------------------------------------
_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_INFRA_FILES = {
    "generation": "infra_generation.geojson",
    "datacenters": "infra_datacenters.geojson",
}
_infra_bytes_cache: dict = {}

# Nationwide transmission basemap (>=69kV, HIFLD), pre-gzipped. Shared by both tabs
# in place of the old regional files; the kV-class filter (incl. its All/off toggle)
# is what shows/hides it client-side. Voltage-colored violet/blue/teal in the UI.
_BASEMAP_GZ = os.path.join(_DATA_DIR, "transmission_us.geojson.gz")
_basemap_gz_bytes = None


def serve_basemap():
    global _basemap_gz_bytes
    if _basemap_gz_bytes is None:
        with open(_BASEMAP_GZ, "rb") as fh:
            _basemap_gz_bytes = fh.read()
    resp = Response(_basemap_gz_bytes, mimetype="application/json")
    resp.headers["Content-Encoding"] = "gzip"
    resp.headers["Vary"] = "Accept-Encoding"
    return resp


@constraints_bp.route("/api/infra/<layer>")
def api_infra(layer):
    """Serve a static nationwide GeoJSON. Prefers a pre-gzipped sibling (.gz)
    with Content-Encoding so big layers (transmission ~16MB raw) ship ~4x smaller;
    the browser decodes transparently. Falls back to the raw file."""
    fname = _INFRA_FILES.get(layer)
    if not fname:
        return jsonify({"error": f"unknown infra layer {layer}"}), 404
    if layer not in _infra_bytes_cache:
        raw = os.path.join(_DATA_DIR, fname)
        gz = raw + ".gz"
        if os.path.exists(gz):
            with open(gz, "rb") as fh:
                _infra_bytes_cache[layer] = (fh.read(), True)
        elif os.path.exists(raw):
            with open(raw, "rb") as fh:
                _infra_bytes_cache[layer] = (fh.read(), False)
        else:
            return jsonify({"error": f"infra layer {layer} not deployed"}), 404
    body, gzipped = _infra_bytes_cache[layer]
    resp = Response(body, mimetype="application/json")
    if gzipped:
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Vary"] = "Accept-Encoding"
    return resp


_OVERPASS_EP = "https://overpass-api.de/api/interpreter"
_DENSE_Q = {
    "substations": '[out:json][timeout:60];(way["power"="substation"]({bbox});'
                   'node["power"="substation"]({bbox}););out center tags;',
    "pipelines": '[out:json][timeout:60];way["man_made"="pipeline"]({bbox});out geom tags;',
}
_infra_dense_cache: dict = {}


def _overpass_geojson(query):
    """POST an Overpass query and convert elements to a GeoJSON FeatureCollection."""
    req = urllib.request.Request(
        _OVERPASS_EP,
        data=urllib.parse.urlencode({"data": query}).encode(),
        headers={"User-Agent": "constraint-map-infra/1.0"},
    )
    payload = json.load(urllib.request.urlopen(req, timeout=70))
    feats = []
    for el in payload.get("elements", []):
        tg = el.get("tags", {})
        if el.get("type") == "way" and el.get("geometry"):          # pipelines -> lines
            co = [[round(g["lon"], 5), round(g["lat"], 5)] for g in el["geometry"]]
            if len(co) < 2:
                continue
            geom = {"type": "LineString", "coordinates": co}
        else:                                                       # substations -> points
            c = el.get("center")
            if not c and el.get("lat") is not None:
                c = {"lat": el["lat"], "lon": el["lon"]}
            if not c:
                continue
            geom = {"type": "Point", "coordinates": [round(c["lon"], 5), round(c["lat"], 5)]}
        feats.append({"type": "Feature", "geometry": geom, "properties": {
            "name": tg.get("name"), "operator": tg.get("operator"),
            "substance": tg.get("substance"), "voltage": tg.get("voltage")}})
    return {"type": "FeatureCollection", "features": feats}


@constraints_bp.route("/api/infra/dense/<layer>")
def api_infra_dense(layer):
    if layer not in _DENSE_Q:
        return jsonify({"error": f"unknown dense layer {layer}"}), 404
    try:
        s, w, n, e = (round(float(x), 2) for x in request.args.get("bbox", "").split(","))
    except Exception:
        return jsonify({"error": "bad bbox"}), 400
    if (n - s) * (e - w) > 3.0:        # guard: viewport too large -> only serve when zoomed in
        return jsonify({"type": "FeatureCollection", "features": [], "note": "zoom in"})
    key = (layer, s, w, n, e)
    hit = _infra_dense_cache.get(key)
    if hit and (time.time() - hit[0]) < 86400:
        return jsonify(hit[1])
    try:
        gj = _overpass_geojson(_DENSE_Q[layer].format(bbox=f"{s},{w},{n},{e}"))
    except Exception as ex:
        logger.warning("overpass dense fetch failed (%s): %s", layer, ex)
        return jsonify({"type": "FeatureCollection", "features": []})
    _infra_dense_cache[key] = (time.time(), gj)
    return jsonify(gj)


@constraints_bp.route("/static/infra_layers.js")
def infra_layers_js():
    path = os.path.join(os.path.dirname(__file__), "..", "static", "infra_layers.js")
    with open(path, encoding="utf-8") as fh:
        return Response(fh.read(), mimetype="application/javascript")
