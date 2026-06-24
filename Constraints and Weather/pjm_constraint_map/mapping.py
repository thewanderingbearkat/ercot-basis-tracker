"""Geographic features for the PJM map.

Reuses the ISO-agnostic `constraint_map.geo.facility_geometry` (FACILITYID ->
FACILITIES -> STATIONS_GEO) to place each daily-driver constraint between its two
endpoint substations. v1 draws straight segments between real endpoints; conductor
routing (as on the ERCOT map) can be layered in later once geo.py is generalized
to a PJM basemap.
"""
from __future__ import annotations

import os
from typing import Any

from constraint_map.geo import facility_geometry, routed_path

from .attribution import daily_attribution
from .sites import SITES

PJM_BASEMAP = os.path.join(os.path.dirname(__file__), "..", "data", "pjm_transmission_lines.geojson")


def driver_map(site_key: str, days: int = 1, top: int = 15,
               start: str | None = None, end: str | None = None) -> dict[str, Any]:
    attr = daily_attribution(site_key, days=days, top=top, start=start, end=end)

    drivers = attr["drivers"]
    geo = facility_geometry([d["facility_id"] for d in drivers])
    for d in drivers:
        g = geo.get(d["facility_id"]) or {}
        frm, to = g.get("from"), g.get("to")
        path = routed_path(frm, to, PJM_BASEMAP, tol_km=5.0)
        d.update({"from": frm, "to": to, "voltage": g.get("voltage"),
                  "drawable": bool(frm and to), "path": path, "snapped": path is not None})

    s = SITES[site_key]
    return {
        "site": {"key": s.key, "name": s.display_name, "pnode": s.pnode_name,
                 "lat": s.lat, "lon": s.lon, "fuel": s.fuel},
        "avg_congestion": attr["avg_congestion"], "hub_name": attr["hub_name"],
        "hub_avg_congestion": attr["hub_avg_congestion"], "congestion_basis": attr["congestion_basis"],
        "as_of": attr["as_of"], "start": attr["start"], "days": attr["days"],
        "drivers": drivers, "other_contrib": attr["other_contrib"], "other_count": attr["other_count"],
    }
