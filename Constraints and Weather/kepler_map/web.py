"""Kepler/deck.gl tab blueprint.

    GET /kepler                       -- the deck.gl scene (HTML)
    GET /api/kepler/transmission      -- transmission lines as arcs (source/target
                                         endpoints + voltage), >=min_kv
    GET /api/kepler/congestion        -- time-stamped, geolocated ERCOT binding
                                         constraints for a day (drives the time layer)

Facility points for the hexagon layer reuse the existing /api/infra/* endpoints.
"""
import gzip
import json
import logging
import os
import time

from flask import Blueprint, jsonify, render_template, request

from constraint_map.constraints import latest_interval
from constraint_map.db import YES, query
from constraint_map.geo import facility_geometry

logger = logging.getLogger(__name__)

kepler_bp = Blueprint("kepler_map", __name__, template_folder="templates")

_DATA = os.path.join(os.path.dirname(__file__), "..", "data")
_TRANSMISSION_GZ = os.path.join(_DATA, "transmission_us.geojson.gz")
_arc_cache: dict[int, list] = {}


@kepler_bp.route("/kepler")
def kepler_page():
    return render_template("kepler_map.html")


def _line_endpoints(geom):
    """First and last coordinate of a (Multi)LineString -> (source, target)."""
    t, co = geom.get("type"), geom.get("coordinates") or []
    if t == "LineString":
        return (co[0], co[-1]) if len(co) >= 2 else (None, None)
    if t == "MultiLineString":
        segs = [s for s in co if s]
        if segs and len(segs[0]) and len(segs[-1]):
            return segs[0][0], segs[-1][-1]
    return None, None


@kepler_bp.route("/api/kepler/transmission")
def api_transmission_arcs():
    """Transmission lines as great-circle arcs. Defaults to the >=345kV backbone
    (~3.5k arcs) so the ArcLayer stays snappy; ?min_kv=N to widen."""
    try:
        min_kv = int(request.args.get("min_kv", 345))
    except ValueError:
        min_kv = 345
    if min_kv not in _arc_cache:
        with gzip.open(_TRANSMISSION_GZ, "rt", encoding="utf-8") as fh:
            gj = json.load(fh)
        arcs = []
        for ft in gj.get("features", []):
            v = (ft.get("properties") or {}).get("VOLTAGE") or 0
            if v < min_kv:
                continue
            s, t = _line_endpoints(ft.get("geometry") or {})
            if not s or not t:
                continue
            arcs.append({"source": s, "target": t, "voltage": v})
        _arc_cache[min_kv] = arcs
        logger.info("kepler transmission arcs >=%skV: %d", min_kv, len(arcs))
    return jsonify(_arc_cache[min_kv])


_cong_cache: dict = {}
_CONG_TTL = 600


@kepler_bp.route("/api/kepler/congestion")
def api_congestion_time():
    """Time-stamped, geolocated ERCOT binding constraints for a day.

    ?date=YYYY-MM-DD (default: the most recent 24h of SCED data). Each point is a
    binding constraint at one SCED interval, placed at its constrained facility's
    midpoint, carrying the shadow price -> the time layer animates these.
    """
    date = request.args.get("date") or None
    key = date or "_latest"
    hit = _cong_cache.get(key)
    if hit and (time.time() - hit[0]) < _CONG_TTL:
        return jsonify(hit[1])

    if date:
        d0_sql = "%(date)s::DATE"
        d1_sql = "DATEADD('day', 1, %(date)s::DATE)"
        params = {"date": date}
    else:
        latest = latest_interval()
        if not latest:
            return jsonify({"points": [], "t0": None, "t1": None, "note": "no SCED data"})
        # most recent 24h ending at the latest interval
        d0_sql = "DATEADD('day', -1, %(latest)s::TIMESTAMP_NTZ)"
        d1_sql = "DATEADD('minute', 5, %(latest)s::TIMESTAMP_NTZ)"
        params = {"latest": latest}

    sql = f"""
        SELECT c.DATETIME, c.FACILITYID, c.CONSTRAINTNAME, c.REPORTED_NAME, c.PRICE
        FROM {YES}.CONSTRAINTS c
        WHERE c.ISO = 'ERCOT' AND c.PRICE <> 0 AND c.FACILITYID IS NOT NULL
          AND c.DATETIME >= {d0_sql} AND c.DATETIME < {d1_sql}
    """
    try:
        rows = query(sql, params)
    except Exception as e:
        logger.exception("kepler congestion query failed")
        return jsonify({"error": str(e)}), 502

    geo = facility_geometry({r["FACILITYID"] for r in rows})
    points, t0, t1 = [], None, None
    for r in rows:
        g = geo.get(r["FACILITYID"])
        if not g:
            continue
        frm, to = g.get("from"), g.get("to")
        pts = [p for p in (frm, to) if p]
        if not pts:
            continue
        lon = sum(p["lon"] for p in pts) / len(pts)
        lat = sum(p["lat"] for p in pts) / len(pts)
        dt = r["DATETIME"]
        epoch = int(dt.timestamp()) if dt else None
        if epoch is None:
            continue
        t0 = epoch if t0 is None else min(t0, epoch)
        t1 = epoch if t1 is None else max(t1, epoch)
        points.append({
            "position": [round(lon, 5), round(lat, 5)],
            "t": epoch,
            "price": round(float(r["PRICE"]), 2) if r["PRICE"] is not None else 0,
            "name": r["REPORTED_NAME"] or r["CONSTRAINTNAME"],
        })
    out = {"points": points, "t0": t0, "t1": t1}
    _cong_cache[key] = (time.time(), out)
    logger.info("kepler congestion: %d points over %s", len(points), key)
    return jsonify(out)
