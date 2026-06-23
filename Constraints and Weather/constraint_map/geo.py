"""Geographic layer for the constraint map.

The hard part of "where is this constraint" is solved entirely inside Snowflake:

    CONSTRAINTS.FACILITYID
      -> FACILITIES (the monitored line)              .FROMSTATIONID / .TOSTATIONID
      -> STATIONS_GEO (YES_GEODATA.GEO)               .LAT / .LON

STATIONS_GEO keys on the same OBJECTID as STATIONS/FACILITIES, so each binding
constraint's two endpoint substations resolve to real coordinates with no name
matching or external geocoding. Over a 90-day window of constraints binding on
our nodes, ~100% have at least one endpoint located and ~86% have both (a
drawable straight segment); the rest fall back to their single known endpoint.

Each constraint segment is then *snapped* to the real HIFLD line geometry in the
basemap by matching the two endpoint coordinates (no name matching -- pure
geometry), so the congestion overlay traces the actual conductor path instead of
a straight bar. Constraints with no nearby basemap line fall back to a straight
segment between the two stations.
"""
from __future__ import annotations

import json
import math
import os
from typing import Any, Iterable

from .db import query

# Fully-qualified sources (the connection pins no default database/schema).
FACILITIES = "YES_ENERGY__FULL_DATASET.YESDATA.FACILITIES"
STATIONS_GEO = "YES_GEODATA.GEO.STATIONS_GEO"

# Static basemap shipped alongside the package (real HIFLD line geometry, TX).
BASEMAP_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "tx_transmission_lines.geojson")

# Cache of the trimmed basemap, keyed by (path, min_kv) -- we never ship the raw
# file to the browser; we filter by voltage and round coordinates first.
_basemap_cache: dict[tuple, dict[str, Any]] = {}


def load_basemap(min_kv: int = 100, path: str | None = None) -> dict[str, Any]:
    """Return a HIFLD transmission basemap as GeoJSON, trimmed for the browser.

    Keeps only lines at/above `min_kv` (HIFLD VOLTAGE; -999999 = unknown is kept
    so we don't silently drop real lines), and rounds coordinates to 5 decimals
    (~1 m) to cut payload. `path` selects the basemap file (defaults to the TX
    one); result is cached per (path, min_kv) so ERCOT and PJM don't collide.
    """
    p = path or BASEMAP_PATH
    key = (p, min_kv)
    if key in _basemap_cache:
        return _basemap_cache[key]

    with open(p, encoding="utf-8") as fh:
        raw = json.load(fh)

    def keep(props: dict[str, Any]) -> bool:
        v = props.get("VOLTAGE")
        return v is None or v < 0 or v >= min_kv

    def round_coords(c: Any) -> Any:
        if c and isinstance(c[0], (int, float)):
            return [round(c[0], 5), round(c[1], 5)]
        return [round_coords(x) for x in c]

    feats = []
    for f in raw.get("features", []):
        p = f.get("properties", {})
        if not keep(p):
            continue
        g = f.get("geometry") or {}
        feats.append({
            "type": "Feature",
            "geometry": {"type": g.get("type"), "coordinates": round_coords(g.get("coordinates", []))},
            # Only the handful of props the map actually styles/labels with.
            "properties": {"VOLTAGE": p.get("VOLTAGE"), "VOLT_CLASS": p.get("VOLT_CLASS")},
        })

    fc = {"type": "FeatureCollection", "features": feats}
    _basemap_cache[key] = fc
    return fc


def facility_geometry(facility_ids: Iterable[Any]) -> dict[Any, dict[str, Any]]:
    """Resolve each facility id to its two endpoint substations + coordinates.

    Returns {facility_id: {voltage, from: {name, lat, lon}, to: {name, lat, lon}}}.
    An endpoint whose station has no geo row is returned as None, so callers can
    decide whether to draw a full segment, a single point, or skip.
    """
    ids = [fid for fid in dict.fromkeys(facility_ids) if fid is not None]
    if not ids:
        return {}

    in_clause = ", ".join(str(int(fid)) for fid in ids)
    sql = f"""
        SELECT f.OBJECTID AS FACILITY_ID, f.VOLTAGE,
               gf.STATIONNAME AS FROM_NAME, gf.LAT AS FROM_LAT, gf.LON AS FROM_LON,
               gt.STATIONNAME AS TO_NAME,   gt.LAT AS TO_LAT,   gt.LON AS TO_LON
        FROM {FACILITIES} f
        LEFT JOIN {STATIONS_GEO} gf ON gf.OBJECTID = f.FROMSTATIONID
        LEFT JOIN {STATIONS_GEO} gt ON gt.OBJECTID = f.TOSTATIONID
        WHERE f.OBJECTID IN ({in_clause})
    """
    out: dict[Any, dict[str, Any]] = {}
    for r in query(sql):
        out[r["FACILITY_ID"]] = {
            "voltage": float(r["VOLTAGE"]) if r["VOLTAGE"] is not None else None,
            "from": _endpoint(r["FROM_NAME"], r["FROM_LAT"], r["FROM_LON"]),
            "to": _endpoint(r["TO_NAME"], r["TO_LAT"], r["TO_LON"]),
        }
    return out


def _endpoint(name: Any, lat: Any, lon: Any) -> dict[str, Any] | None:
    if lat is None or lon is None:
        return None
    return {"name": name, "lat": float(lat), "lon": float(lon)}


# --- Snap constraint segments to real HIFLD line geometry -------------------
# We index every basemap line by its two endpoint coordinates, then match a
# constraint (whose endpoints come from STATIONS_GEO) to the basemap line whose
# ends sit on the same two substations. Pure geometry -- no names involved.
_lines_index: list[tuple[tuple[float, float], tuple[float, float], list[list[float]]]] | None = None


def _line_index():
    """Lazily build [(end_a, end_b, latlon_path), ...] over all basemap lines."""
    global _lines_index
    if _lines_index is not None:
        return _lines_index
    with open(BASEMAP_PATH, encoding="utf-8") as fh:
        raw = json.load(fh)
    idx = []
    for f in raw.get("features", []):
        g = f.get("geometry") or {}
        t = g.get("type")
        parts = [g.get("coordinates", [])] if t == "LineString" else (g.get("coordinates", []) if t == "MultiLineString" else [])
        for ls in parts:
            if not ls or len(ls) < 2:
                continue
            path = [[p[1], p[0]] for p in ls]   # [lon,lat] -> [lat,lon]
            idx.append((tuple(path[0]), tuple(path[-1]), path))
    _lines_index = idx
    return idx


def _d2(a, b) -> float:
    """Squared planar distance in degrees, longitude scaled by latitude (good
    enough for nearest-line matching at Texas latitudes)."""
    dlat = a[0] - b[0]
    dlon = (a[1] - b[1]) * math.cos(math.radians(a[0]))
    return dlat * dlat + dlon * dlon


# HIFLD splits each line into many short segments that connect end-to-end at
# shared vertices, so a constraint rarely matches a single feature. We instead
# build a graph (nodes = segment endpoints, edges = segments carrying their full
# polyline) and route the shortest network path between the two substations.
_graph: dict[tuple, list[tuple]] | None = None
_nodes: list[tuple] | None = None


def _node_key(pt) -> tuple:
    return (round(pt[0], 5), round(pt[1], 5))   # ~1 m; HIFLD segments share exact vertices


def _seg_len(path) -> float:
    return sum(_d2(path[i], path[i + 1]) ** 0.5 for i in range(len(path) - 1))


def _build_graph():
    global _graph, _nodes
    if _graph is not None:
        return _graph, _nodes
    adj: dict[tuple, list[tuple]] = {}
    for _ea, _eb, path in _line_index():
        a, b = _node_key(path[0]), _node_key(path[-1])
        if a == b:
            continue
        w = _seg_len(path)
        adj.setdefault(a, []).append((b, w, path))
        adj.setdefault(b, []).append((a, w, list(reversed(path))))
    _graph, _nodes = adj, list(adj.keys())
    return _graph, _nodes


def _nearest_node(pt, tol_km: float):
    adj, nodes = _build_graph()
    tol = (tol_km / 111.0) ** 2
    best, best_d = None, None
    for n in nodes:
        d = _d2(pt, n)
        if best_d is None or d < best_d:
            best_d, best = d, n
    return best if (best is not None and best_d <= tol) else None


def _snap_path(frm: dict, to: dict, tol_km: float = 3.0) -> list[list[float]] | None:
    """Shortest network path between the two substations along basemap geometry,
    oriented from->to. None if either endpoint isn't near the network or the two
    are not connected within a sane distance."""
    import heapq

    adj, _ = _build_graph()
    src = _nearest_node((frm["lat"], frm["lon"]), tol_km)
    dst = _nearest_node((to["lat"], to["lon"]), tol_km)
    if src is None or dst is None or src == dst:
        return None

    # Cap the search so a disconnected/odd pair doesn't wander the whole state.
    direct = _d2(src, dst) ** 0.5
    budget = max(0.05, direct * 4)   # allow some meander, not a cross-state detour

    dist = {src: 0.0}
    prev: dict[tuple, tuple] = {}      # node -> (prev_node, segment_path)
    pq = [(0.0, src)]
    while pq:
        d, u = heapq.heappop(pq)
        if u == dst:
            break
        if d > dist.get(u, float("inf")) or d > budget:
            continue
        for v, w, seg in adj.get(u, []):
            nd = d + w
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = (u, seg)
                heapq.heappush(pq, (nd, v))

    if dst not in prev and dst != src:
        return None
    # Reconstruct path from dst back to src.
    chain: list[list[float]] = []
    cur = dst
    while cur != src:
        u, seg = prev[cur]
        chain.extend(reversed(seg))   # seg is u->cur; we're walking cur->src
        cur = u
    chain.reverse()
    return chain or None


def _single_feature_path(frm: dict, to: dict, tol_km: float = 3.0) -> list[list[float]] | None:
    """Fallback: a single basemap feature whose two endpoints land on both
    substations. Catches lines that exist as one segment but whose endpoints the
    graph snapped to the wrong coincident node (common in dense metros)."""
    f = (frm["lat"], frm["lon"])
    t = (to["lat"], to["lon"])
    tol = (tol_km / 111.0) ** 2
    best, best_cost, fwd = None, None, True
    for ea, eb, path in _line_index():
        c_fwd = _d2(f, ea) + _d2(t, eb)
        c_rev = _d2(f, eb) + _d2(t, ea)
        cost = c_fwd if c_fwd <= c_rev else c_rev
        if best_cost is None or cost < best_cost:
            best_cost, best, fwd = cost, path, c_fwd <= c_rev
    if best is None:
        return None
    a, b = (best[0], best[-1]) if fwd else (best[-1], best[0])
    if _d2(f, a) <= tol and _d2(t, b) <= tol:
        return best if fwd else list(reversed(best))
    return None


def _resolve_path(frm: dict, to: dict) -> list[list[float]] | None:
    """Real basemap path for a constraint: graph route first, single feature
    second, else None (caller draws an approximate straight segment)."""
    return _snap_path(frm, to) or _single_feature_path(frm, to)


def attach_geometry(result: dict[str, Any]) -> dict[str, Any]:
    """Enrich an active_constraints() result in place with endpoint geometry.

    Adds a `geometry` key to each constraint: {from, to, voltage, drawable}.
    `drawable` is True only when both endpoints resolved (a full segment).
    """
    constraints = result.get("constraints", [])
    geo = facility_geometry(c.get("facility_id") for c in constraints)
    for c in constraints:
        g = geo.get(c.get("facility_id"))
        if g is None:
            c["geometry"] = {"from": None, "to": None, "voltage": None, "drawable": False,
                             "path": None, "snapped": False}
            continue
        drawable = bool(g["from"] and g["to"])
        path = _resolve_path(g["from"], g["to"]) if drawable else None
        c["geometry"] = {**g, "drawable": drawable, "path": path, "snapped": path is not None}
    return result
