"""Turn raw XWeather storm cells into a Holstein-centric threat assessment.

For each cell we compute, relative to the site:
  - great-circle distance and bearing,
  - the component of the cell's motion aimed at the site (closing speed),
  - the cross-track miss distance (how far off-center its track passes), and
  - an ETA in minutes if it's closing.

A cell is INBOUND when it carries hail, its track passes within the corridor, and
its ETA is inside the horizon. It's a WATCH when it carries hail and sits within
the watch radius but isn't cleanly aimed at us. The site status is the worst of
all cells, also elevated to WATCH by any point nowcast threat from hail/threats.
"""
import math

from hail_tracker.config import (
    STATUS_CLEAR,
    STATUS_INBOUND,
    STATUS_WATCH,
    THRESHOLDS,
)

EARTH_RADIUS_MI = 3958.8


def _haversine_mi(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return EARTH_RADIUS_MI * 2 * math.asin(min(1.0, math.sqrt(a)))


def _bearing_deg(lat1, lon1, lat2, lon2) -> float:
    """Initial compass bearing from point 1 -> point 2, degrees [0,360)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _cell_is_hailbearing(hail: dict) -> bool:
    prob = hail.get("prob") or 0
    size = hail.get("maxSizeIN") or 0
    return prob >= THRESHOLDS["min_hail_prob"] or size >= THRESHOLDS["min_hail_size_in"]


def assess_cell(cell: dict, site_lat: float, site_lon: float) -> dict:
    """Flatten one XWeather storm cell and compute its geometry to the site."""
    ob = cell.get("ob", {})
    loc = cell.get("loc", {})
    clat, clon = loc.get("lat"), loc.get("long")
    hail = ob.get("hail", {}) or {}
    mv = ob.get("movement", {}) or {}

    dist_mi = _haversine_mi(clat, clon, site_lat, site_lon) if clat is not None else None
    bearing_to_site = _bearing_deg(clat, clon, site_lat, site_lon) if clat is not None else None

    speed = mv.get("speedMPH") or 0
    move_dir = mv.get("dirToDEG")  # compass direction the cell is moving toward

    closing_mph = None
    cross_track_mi = None
    eta_min = None
    approaching = False
    if dist_mi is not None and move_dir is not None and speed >= THRESHOLDS["min_movement_mph"]:
        # Angle between where the cell is going and where the site is, from the cell.
        delta = math.radians((bearing_to_site - move_dir + 180) % 360 - 180)
        closing_mph = speed * math.cos(delta)          # >0 = getting closer
        cross_track_mi = abs(dist_mi * math.sin(delta))  # perpendicular miss distance
        approaching = closing_mph > 0
        if closing_mph > 0:
            eta_min = (dist_mi / closing_mph) * 60.0

    hailbearing = _cell_is_hailbearing(hail)
    inbound = bool(
        hailbearing
        and approaching
        and cross_track_mi is not None
        and cross_track_mi <= THRESHOLDS["inbound_corridor_mi"]
        and eta_min is not None
        and eta_min <= THRESHOLDS["inbound_eta_min"]
    )
    watch = bool(
        hailbearing
        and dist_mi is not None
        and dist_mi <= THRESHOLDS["watch_radius_mi"]
    )

    if inbound:
        level = STATUS_INBOUND
    elif watch:
        level = STATUS_WATCH
    else:
        level = STATUS_CLEAR

    return {
        "id": cell.get("id"),
        "lat": clat,
        "lon": clon,
        "place": ob.get("location"),
        "hail_prob": hail.get("prob"),
        "hail_prob_severe": hail.get("probSevere"),
        "hail_max_size_in": hail.get("maxSizeIN"),
        "dbz_max": ob.get("dbzm"),
        "vil": ob.get("vil"),
        "top_ft": ob.get("topFT"),
        "move_dir": mv.get("dirTo"),
        "move_dir_deg": move_dir,
        "speed_mph": speed,
        "distance_mi": round(dist_mi, 1) if dist_mi is not None else None,
        "bearing_to_site_deg": round(bearing_to_site) if bearing_to_site is not None else None,
        "closing_mph": round(closing_mph, 1) if closing_mph is not None else None,
        "cross_track_mi": round(cross_track_mi, 1) if cross_track_mi is not None else None,
        "eta_min": round(eta_min) if eta_min is not None else None,
        "approaching": approaching,
        "hailbearing": hailbearing,
        "level": level,
        # Forecast track points (lat/long) if XWeather supplied any, for drawing.
        "forecast_locs": [
            {"lat": p.get("loc", {}).get("lat"), "lon": p.get("loc", {}).get("long"),
             "ts": p.get("timestamp")}
            for p in (cell.get("forecast", {}) or {}).get("locs", [])
            if p.get("loc")
        ],
    }


def _level_rank(level: str) -> int:
    return {STATUS_CLEAR: 0, STATUS_WATCH: 1, STATUS_INBOUND: 2}.get(level, 0)


def build_assessment(cells: list, point_threats: list, site_lat: float, site_lon: float) -> dict:
    """Assess all cells, derive the overall site status, and sort the cell list by
    severity then ETA so the dashboard's most-urgent item is first."""
    assessed = [assess_cell(c, site_lat, site_lon) for c in cells]

    overall = STATUS_CLEAR
    for c in assessed:
        if _level_rank(c["level"]) > _level_rank(overall):
            overall = c["level"]
    # A direct point nowcast threat is at least a WATCH even with no resolved cell.
    if point_threats and _level_rank(overall) < _level_rank(STATUS_WATCH):
        overall = STATUS_WATCH

    def sort_key(c):
        # Worst level first; then soonest ETA (None -> last); then nearest.
        return (
            -_level_rank(c["level"]),
            c["eta_min"] if c["eta_min"] is not None else 1e9,
            c["distance_mi"] if c["distance_mi"] is not None else 1e9,
        )

    assessed.sort(key=sort_key)

    inbound = [c for c in assessed if c["level"] == STATUS_INBOUND]
    nearest_threat = next((c for c in assessed if c["hailbearing"]), None)

    return {
        "status": overall,
        "cells": assessed,
        "cell_count": len(assessed),
        "inbound_count": len(inbound),
        "point_threat_count": len(point_threats),
        "nearest_hail_cell": nearest_threat,
        "soonest_eta_min": inbound[0]["eta_min"] if inbound else None,
    }
