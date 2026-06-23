"""Geographic features for the PJM map.

Reuses the ISO-agnostic `constraint_map.geo.facility_geometry` (FACILITYID ->
FACILITIES -> STATIONS_GEO) to place each daily-driver constraint between its two
endpoint substations. v1 draws straight segments between real endpoints; conductor
routing (as on the ERCOT map) can be layered in later once geo.py is generalized
to a PJM basemap.
"""
from __future__ import annotations

from typing import Any

from constraint_map.geo import facility_geometry

from .attribution import daily_attribution
from .sites import SITES


def driver_map(site_key: str, days: int = 30, top: int = 15) -> dict[str, Any]:
    attr = daily_attribution(site_key, days, top=top)
    geo = facility_geometry([d["facility_id"] for d in attr["drivers"]])

    drivers = []
    for d in attr["drivers"]:
        g = geo.get(d["facility_id"]) or {}
        frm, to = g.get("from"), g.get("to")
        drivers.append({
            **d,
            "from": frm, "to": to, "voltage": g.get("voltage"),
            "drawable": bool(frm and to),
        })

    s = SITES[site_key]
    return {
        "site": {"key": s.key, "name": s.display_name, "pnode": s.pnode_name,
                 "lat": s.lat, "lon": s.lon, "fuel": s.fuel},
        "avg_congestion": attr["avg_congestion"], "days": days,
        "drivers": drivers,
    }
