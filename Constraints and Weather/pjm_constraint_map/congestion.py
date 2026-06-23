"""Authoritative node congestion for our PJM sites.

PJM publishes the congestion component of LMP directly, so unlike ERCOT we read
the real number from `DART_PRICES.RTCONG` rather than reconstructing it. This is
the dollar magnitude the UI shows; the BETA shift factors (see attribution.py)
only apportion it across constraints. Basis is computed against each site's own
reference hub/zone.
"""
from __future__ import annotations

from typing import Any

from constraint_map.db import YES, query   # reuse the shared Snowflake connection

from .sites import SITES


def node_congestion(days: int = 7) -> dict[str, Any]:
    """Latest + recent hourly congestion and hub-relative basis for each site."""
    node_ids = [s.node_id for s in SITES.values()] + [s.hub_node_id for s in SITES.values()]
    ids = ",".join(str(i) for i in dict.fromkeys(node_ids))
    rows = query(f"""
        SELECT OBJECTID, DATETIME, RTLMP, RTCONG
        FROM {YES}.DART_PRICES
        WHERE OBJECTID IN ({ids}) AND RTLMP IS NOT NULL
          AND DATETIME >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP)
        ORDER BY DATETIME
    """)
    series: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        series.setdefault(r["OBJECTID"], []).append({
            "t": r["DATETIME"].isoformat() if r["DATETIME"] else None,
            "lmp": float(r["RTLMP"]) if r["RTLMP"] is not None else None,
            "cong": float(r["RTCONG"]) if r["RTCONG"] is not None else None,
        })

    def last_lmp(oid: int):
        pts = series.get(oid, [])
        return next((p["lmp"] for p in reversed(pts) if p["lmp"] is not None), None)

    out = []
    for s in SITES.values():
        pts = series.get(s.node_id, [])
        last = pts[-1] if pts else {}
        site_lmp = last.get("lmp")
        hub_lmp = last_lmp(s.hub_node_id)
        out.append({
            "key": s.key, "name": s.display_name, "pnode": s.pnode_name, "fuel": s.fuel,
            "lat": s.lat, "lon": s.lon,
            "hub_name": s.hub_name,
            "rt_lmp": site_lmp,
            "rt_cong": last.get("cong"),
            "hub_lmp": hub_lmp,
            "basis": (site_lmp - hub_lmp) if (site_lmp is not None and hub_lmp is not None) else None,
            "last_time": last.get("t"),
            "series": [{"t": p["t"], "cong": p["cong"]} for p in pts],
        })
    return {"days": days, "sites": out}
