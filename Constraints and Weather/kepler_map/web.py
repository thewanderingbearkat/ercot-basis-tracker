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
from constraint_map.sites import SETTLEMENT_POINTS, SITES, SITES_BY_SP

logger = logging.getLogger(__name__)

kepler_bp = Blueprint("kepler_map", __name__, template_folder="templates")

_DATA = os.path.join(os.path.dirname(__file__), "..", "data")
# Pre-built arcs (>=138kV) carrying endpoint substation names (SUB_1/SUB_2) and
# OWNER from HIFLD, so the hover can read "substation -> substation, kV". The
# basemap file (transmission_us.geojson.gz) stays name-free / lean.
_TRANSMISSION_ARCS_GZ = os.path.join(_DATA, "transmission_arcs.json.gz")
_arcs_all = None
_arc_cache: dict[int, list] = {}


@kepler_bp.route("/kepler")
def kepler_page():
    return render_template("kepler_map.html")


def _load_all_arcs():
    global _arcs_all
    if _arcs_all is None:
        with gzip.open(_TRANSMISSION_ARCS_GZ, "rt", encoding="utf-8") as fh:
            _arcs_all = json.load(fh)
    return _arcs_all


@kepler_bp.route("/api/kepler/transmission")
def api_transmission_arcs():
    """Transmission lines as arcs with endpoint substation names + owner (HIFLD).
    Defaults to the >=345kV backbone; ?min_kv=N (down to 138) widens."""
    try:
        min_kv = int(request.args.get("min_kv", 345))
    except ValueError:
        min_kv = 345
    if min_kv not in _arc_cache:
        _arc_cache[min_kv] = [a for a in _load_all_arcs() if a["voltage"] >= min_kv]
        logger.info("kepler transmission arcs >=%skV: %d", min_kv, len(_arc_cache[min_kv]))
    return jsonify(_arc_cache[min_kv])


_cong_cache: dict = {}
_CONG_TTL = 600


def _sp_in_clause():
    return ", ".join(f"'{sp}'" for sp in SETTLEMENT_POINTS)


def _site_labels():
    """settlement point -> a friendly label of the sites that settle there."""
    return {sp: " / ".join(SITES[k].display_name for k in keys)
            for sp, keys in SITES_BY_SP.items()}


@kepler_bp.route("/api/kepler/sites")
def api_sites():
    """Our own assets (ERCOT + PJM site configs) so the map can flag them."""
    out = []
    try:
        from constraint_map.sites import SITES as E
        for s in E.values():
            out.append({"name": s.display_name, "position": [s.lon, s.lat],
                        "fuel": s.fuel, "iso": "ERCOT", "node": s.settlement_point})
    except Exception:
        logger.exception("ercot sites load failed")
    try:
        from pjm_constraint_map.sites import SITES as P
        for s in P.values():
            out.append({"name": s.display_name, "position": [s.lon, s.lat],
                        "fuel": s.fuel, "iso": "PJM", "node": s.pnode_name})
    except Exception:
        logger.exception("pjm sites load failed")
    return jsonify(out)


@kepler_bp.route("/api/kepler/congestion")
def api_congestion_time():
    """ERCOT binding constraints over a day, as time-stamped LINES (the constrained
    element's from->to geometry) plus the per-interval congestion push at each of
    our settlement points.

    ?date=YYYY-MM-DD (default: the most recent 24h of SCED data). Returns:
      lines        [{source,target,t,price,name,node_impact}]  -- light up over time
      node_series  {sp: [{t, impact}]}   -- congestion $/MWh at our node per interval
      node_labels  {sp: "Bearkat I / McCrae (BKII)"}
    node_impact / impact sign: NEGATIVE = constraint pushing the node's price (and
    our basis) DOWN; positive = lifting it. impact = -(shadow_price * shift_factor).
    """
    date = request.args.get("date") or None
    key = date or "_latest"
    hit = _cong_cache.get(key)
    if hit and (time.time() - hit[0]) < _CONG_TTL:
        return jsonify(hit[1])

    if date:
        d0_sql, d1_sql = "%(date)s::DATE", "DATEADD('day', 1, %(date)s::DATE)"
        params = {"date": date}
    else:
        latest = latest_interval()
        if not latest:
            return jsonify({"lines": [], "node_series": {}, "node_labels": {}, "t0": None, "t1": None})
        d0_sql = "DATEADD('day', -1, %(latest)s::TIMESTAMP_NTZ)"
        d1_sql = "DATEADD('minute', 5, %(latest)s::TIMESTAMP_NTZ)"
        params = {"latest": latest}

    # (A) every binding constraint over the window -> the lines that light up.
    lines_sql = f"""
        SELECT c.DATETIME, c.CONSTRAINTID, c.FACILITYID, c.CONSTRAINTNAME, c.REPORTED_NAME, c.PRICE
        FROM {YES}.CONSTRAINTS c
        WHERE c.ISO = 'ERCOT' AND c.PRICE <> 0 AND c.FACILITYID IS NOT NULL
          AND c.DATETIME >= {d0_sql} AND c.DATETIME < {d1_sql}
    """
    # (B) the congestion push at OUR settlement points, per (constraint, interval).
    impact_sql = f"""
        SELECT c.DATETIME, c.CONSTRAINTID, sf.SETTLEMENTPOINT,
               -(c.PRICE * sf.SHIFTFACTOR) AS IMPACT
        FROM {YES}.CONSTRAINTS c
        JOIN {YES}.ERCOT_SCED_SHIFT_FACTORS sf
          ON sf.DATETIME = c.DATETIME AND sf.CONSTRAINTID = c.CONSTRAINTID
        WHERE c.ISO = 'ERCOT' AND c.PRICE <> 0
          AND sf.SETTLEMENTPOINT IN ({_sp_in_clause()})
          AND c.DATETIME >= {d0_sql} AND c.DATETIME < {d1_sql}
          AND sf.DATETIME >= {d0_sql} AND sf.DATETIME < {d1_sql}
    """
    try:
        line_rows = query(lines_sql, params)
        impact_rows = query(impact_sql, params)
    except Exception as e:
        logger.exception("kepler congestion query failed")
        return jsonify({"error": str(e)}), 502

    # Per-(constraint, interval) impact on each of our SPs -> attach to lines + roll
    # up into a per-SP, per-interval net series for the node box.
    impact_by_key: dict = {}
    series: dict = {sp: {} for sp in SETTLEMENT_POINTS}
    for r in impact_rows:
        dt = r["DATETIME"]
        if not dt:
            continue
        epoch = int(dt.timestamp())
        imp = round(float(r["IMPACT"]), 3) if r["IMPACT"] is not None else 0.0
        sp = r["SETTLEMENTPOINT"]
        impact_by_key.setdefault((r["CONSTRAINTID"], epoch), {})[sp] = imp
        series[sp][epoch] = series[sp].get(epoch, 0.0) + imp

    geo = facility_geometry({r["FACILITYID"] for r in line_rows})
    lines, t0, t1 = [], None, None
    for r in line_rows:
        g = geo.get(r["FACILITYID"])
        if not g:
            continue
        frm, to = g.get("from"), g.get("to")
        if not frm or not to:
            continue
        dt = r["DATETIME"]
        if not dt:
            continue
        epoch = int(dt.timestamp())
        ni = impact_by_key.get((r["CONSTRAINTID"], epoch))
        if not ni:
            continue   # only show constraints that actually touch one of our nodes
        t0 = epoch if t0 is None else min(t0, epoch)
        t1 = epoch if t1 is None else max(t1, epoch)
        lines.append({
            "source": [round(frm["lon"], 5), round(frm["lat"], 5)],
            "target": [round(to["lon"], 5), round(to["lat"], 5)],
            "t": epoch,
            "price": round(float(r["PRICE"]), 2) if r["PRICE"] is not None else 0,
            "name": r["REPORTED_NAME"] or r["CONSTRAINTNAME"],
            "node_impact": {k: round(v, 2) for k, v in ni.items()},
        })

    node_series = {sp: [{"t": t, "impact": round(v, 3)} for t, v in sorted(pts.items())]
                   for sp, pts in series.items()}
    # widen the time range to the full node-impact series so the slider/sparkline
    # cover the whole day even if the touching-lines are sparse at the edges.
    for pts in node_series.values():
        for e in pts:
            t0 = e["t"] if t0 is None else min(t0, e["t"])
            t1 = e["t"] if t1 is None else max(t1, e["t"])
    out = {"lines": lines, "node_series": node_series, "node_labels": _site_labels(),
           "settlement_points": list(SETTLEMENT_POINTS), "t0": t0, "t1": t1}
    _cong_cache[key] = (time.time(), out)
    logger.info("kepler congestion: %d lines, %d impact rows over %s", len(lines), len(impact_rows), key)
    return jsonify(out)
