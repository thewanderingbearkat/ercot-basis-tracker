"""Authoritative PJM binding constraints (system-wide).

The node-attribution panel needs MODELED shift factors (BETA) to say how much a
constraint hits OUR node. But the binding constraints THEMSELVES -- which elements
bound, how hard (shadow price), and how often -- are AUTHORITATIVE in PJM and sit
in the same CONSTRAINTS table as ERCOT (ISO='PJMISO'). This ranks them over the
window by persistence-weighted shadow price, so you see the real congestion
landscape with no modeling involved.

IMPORTANT: the shadow price is the constraint's own marginal value ($/MWh), NOT
the impact on our node -- that conversion needs the per-node shift factor PJM
doesn't publish. So this is SYSTEM-WIDE congestion, complementary to (not a
replacement for) the modeled node-attribution panel.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from constraint_map.db import YES, query
from constraint_map.geo import facility_geometry, routed_path

from .attribution import _facility_names, last_full_day

PJM_BASEMAP = os.path.join(os.path.dirname(__file__), "..", "data", "pjm_transmission_lines.geojson")


def binding_constraints(days: int = 1, top: int = 15) -> dict[str, Any]:
    """Top binding PJM constraints over the window (last full operating day, or a
    trailing N-day window ending there), ranked by persistence-weighted shadow
    price = SUM(shadow price) / intervals in window -- a time-averaged $/MWh that
    rewards binding both often and hard."""
    as_of = last_full_day()
    if as_of is None:
        return {"as_of": None, "start": None, "days": int(days), "intervals": 0, "drivers": []}
    days = max(1, int(days))
    start = as_of - timedelta(days=days - 1)
    win = f"DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{as_of}'::DATE)"

    # Total intervals in the window (the ATC denominator). Cadence-agnostic: the
    # CONSTRAINTS grid defines the intervals, and hours are derived from the span.
    total = query(f"""
        SELECT COUNT(DISTINCT DATETIME) AS N FROM {YES}.CONSTRAINTS
        WHERE ISO='PJMISO' AND {win}
    """)[0]["N"] or 1

    # PJM rows carry NO CONSTRAINTNAME (null) and PRICE is signed negative -- so we
    # key on the monitored FACILITYID, name it from FACILITIES, and rank by |PRICE|.
    rows = query(f"""
        SELECT FACILITYID                  AS FID,
               COUNT(DISTINCT CONTINGENCYID) AS N_CTG,
               COUNT(DISTINCT DATETIME)      AS N_BIND,
               AVG(ABS(PRICE))               AS AVG_SP,
               SUM(ABS(PRICE)) / {total}     AS ATC_SP,
               MAX(ABS(PRICE))               AS MAX_SP
        FROM {YES}.CONSTRAINTS
        WHERE ISO='PJMISO' AND PRICE <> 0 AND FACILITYID IS NOT NULL AND {win}
        GROUP BY FACILITYID
        ORDER BY SUM(ABS(PRICE)) / {total} DESC
        LIMIT {int(top)}
    """)

    names = _facility_names([r["FID"] for r in rows])
    span_hours = days * 24.0
    drivers = []
    for r in rows:
        n = int(r["N_BIND"])
        drivers.append({
            "facility_id": r["FID"], "name": names.get(r["FID"], str(r["FID"])),
            "n_contingencies": int(r["N_CTG"]) if r["N_CTG"] is not None else 1,
            "atc_sp": float(r["ATC_SP"]) if r["ATC_SP"] is not None else 0.0,
            "avg_sp": float(r["AVG_SP"]) if r["AVG_SP"] is not None else 0.0,
            "max_sp": float(r["MAX_SP"]) if r["MAX_SP"] is not None else 0.0,
            "n_bind": n, "pct_time": 100.0 * n / total,
            "hours_bound": (n / total) * span_hours,
        })

    # Endpoint + voltage geometry (routed on the PJM basemap) so the list shows
    # origin/end/voltage and each row can locate itself on the map.
    geo = facility_geometry([d["facility_id"] for d in drivers])
    for d in drivers:
        g = geo.get(d["facility_id"]) or {}
        frm, to = g.get("from"), g.get("to")
        path = routed_path(frm, to, PJM_BASEMAP, tol_km=5.0) if (frm and to) else None
        d["from"], d["to"], d["voltage"] = frm, to, g.get("voltage")
        d["drawable"] = bool(frm and to)
        d["path"], d["snapped"] = path, path is not None

    return {"as_of": str(as_of), "start": str(start), "days": days,
            "intervals": total, "drivers": drivers}
