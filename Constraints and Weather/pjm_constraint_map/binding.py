"""Authoritative PJM binding constraints (system-wide).

The node-attribution panel needs MODELED shift factors (BETA) to say how much a
constraint hits OUR node. But the binding constraints THEMSELVES -- which elements
bound, how hard (shadow price), and how often -- are AUTHORITATIVE in PJM and sit
in the same CONSTRAINTS table as ERCOT (ISO='PJMISO'). This ranks them over the
window by persistence-weighted shadow price, so you see the real congestion
landscape with no modeling involved.

Reads precomputed daily aggregates (CM_PJM_CONSTRAINTS_DAILY / _INTERVALS_DAILY)
for the stale history + a live tail, so 1Y/3Y stays fast.

IMPORTANT: the shadow price is the constraint's own marginal value ($/MWh), NOT
the impact on our node -- that conversion needs the per-node shift factor PJM
doesn't publish. So this is SYSTEM-WIDE congestion, complementary to the modeled
node-attribution panel.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

from constraint_map.basis import DBO, _staged_max
from constraint_map.db import YES, query
from constraint_map.geo import facility_geometry, routed_path

from .attribution import _facility_names, last_full_day

PJM_BASEMAP = os.path.join(os.path.dirname(__file__), "..", "data", "pjm_transmission_lines.geojson")


def binding_constraints(days: int = 1, top: int = 15,
                        start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Top binding PJM constraints over the window, ranked by persistence-weighted
    shadow price = SUM(|shadow|) / intervals -- rewards binding both often and hard.
    Pass days (window ending on the last full day) or an explicit start/end."""
    if end is None:
        as_of = last_full_day()
        if as_of is None:
            return {"as_of": None, "start": None, "days": int(days), "intervals": 0, "drivers": []}
        end = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1

    sm = _staged_max("CM_PJM_CONSTRAINTS_DAILY")
    staged_hi = min(end, sm) if sm else None
    has_staged = bool(sm and start <= staged_hi)
    if sm is None:
        live_lo = start
    elif sm < end:
        live_lo = max(start, (date.fromisoformat(sm) + timedelta(days=1)).isoformat())
    else:
        live_lo = None

    # Total intervals over the window (ATC denominator), staged + live.
    total = 0
    if has_staged:
        total += query(f"SELECT COALESCE(SUM(N_INTERVALS),0) N FROM {DBO}.CM_PJM_INTERVALS_DAILY WHERE DAY BETWEEN '{start}' AND '{staged_hi}'")[0]["N"]
    if live_lo:
        total += query(f"SELECT COUNT(DISTINCT DATETIME) N FROM {YES}.CONSTRAINTS WHERE ISO='PJMISO' AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE)")[0]["N"]
    total = total or 1

    # Per-facility aggregates, staged + live. (n_contingencies can't be summed
    # across days exactly; approximate with the max daily distinct count.)
    fac: dict[Any, dict[str, Any]] = {}

    def add(rows):
        for r in rows:
            d = fac.setdefault(r["FID"], {"n_bind": 0, "abs_sum": 0.0, "n_ctg": 0})
            d["n_bind"] += int(r["NB"])
            d["abs_sum"] += float(r["APS"])
            d["n_ctg"] = max(d["n_ctg"], int(r["NC"]) if r["NC"] is not None else 0)

    if has_staged:
        add(query(f"SELECT FACILITYID FID, SUM(N_BIND) NB, SUM(ABS_PRICE_SUM) APS, MAX(N_CTG) NC FROM {DBO}.CM_PJM_CONSTRAINTS_DAILY WHERE DAY BETWEEN '{start}' AND '{staged_hi}' GROUP BY 1"))
    if live_lo:
        add(query(f"SELECT FACILITYID FID, COUNT(DISTINCT DATETIME) NB, SUM(ABS(PRICE)) APS, COUNT(DISTINCT CONTINGENCYID) NC FROM {YES}.CONSTRAINTS WHERE ISO='PJMISO' AND PRICE<>0 AND FACILITYID IS NOT NULL AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE) GROUP BY 1"))

    span_hours = days * 24.0
    items = []
    for fid, d in fac.items():
        n = d["n_bind"]
        items.append({
            "facility_id": fid, "n_bind": n, "n_contingencies": d["n_ctg"] or 1,
            "atc_sp": d["abs_sum"] / total, "avg_sp": (d["abs_sum"] / n if n else 0.0),
            "max_sp": 0.0, "pct_time": 100.0 * n / total, "hours_bound": (n / total) * span_hours,
        })
    items.sort(key=lambda x: x["atc_sp"], reverse=True)
    drivers = items[:int(top)]

    names = _facility_names([d["facility_id"] for d in drivers])
    geo = facility_geometry([d["facility_id"] for d in drivers])
    for d in drivers:
        d["name"] = names.get(d["facility_id"], str(d["facility_id"]))
        g = geo.get(d["facility_id"]) or {}
        frm, to = g.get("from"), g.get("to")
        path = routed_path(frm, to, PJM_BASEMAP, tol_km=5.0) if (frm and to) else None
        d["from"], d["to"], d["voltage"] = frm, to, g.get("voltage")
        d["drawable"] = bool(frm and to)
        d["path"], d["snapped"] = path, path is not None

    return {"as_of": end, "start": start, "days": days, "intervals": total,
            "drivers": drivers, "staged_through": sm}
