"""Basis decomposition for PJM nodes: Energy + Congestion + Losses -> basis.

PJM publishes the three LMP components (RTCONG, RTLOSS), so basis = node LMP -
hub LMP splits *exactly* into:

    energy-basis = node energy  - hub energy   (~0; system energy is uniform)
    cong-basis   = node RTCONG  - hub RTCONG    (constraint-driven)
    loss-basis   = node RTLOSS  - hub RTLOSS    (topology / loss-driven)

All authoritative -- ties to settlement. Reads precomputed daily DART aggregates
(CM_PJM_DART_DAILY) for the stale history + a live tail, so 1Y/3Y stays fast.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from constraint_map.basis import DBO, _staged_max
from constraint_map.db import YES, query

from .attribution import last_full_day
from .sites import SITES


def basis_decomposition(site_key: str, days: int = 1,
                        start: str | None = None, end: str | None = None) -> dict[str, Any]:
    site = SITES[site_key]
    node, hub = site.node_id, site.hub_node_id
    if end is None:
        as_of = last_full_day()
        if as_of is None:
            return {"site": site.key, "name": site.display_name, "as_of": None,
                    "start": None, "days": int(days)}
        end = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1

    sm = _staged_max("CM_PJM_DART_DAILY")
    staged_hi = min(end, sm) if sm else None
    has_staged = bool(sm and start <= staged_hi)
    if sm is None:
        live_lo = start
    elif sm < end:
        live_lo = max(start, (date.fromisoformat(sm) + timedelta(days=1)).isoformat())
    else:
        live_lo = None

    agg: dict[Any, list] = {}   # objectid -> [lmp_sum, cong_sum, loss_sum, n]

    def add(rows):
        for r in rows:
            a = agg.setdefault(r["O"], [0.0, 0.0, 0.0, 0])
            a[0] += float(r["LS"]); a[1] += float(r["CS"]); a[2] += float(r["OS"]); a[3] += int(r["N"])

    if has_staged:
        add(query(f"SELECT OBJECTID O, SUM(RTLMP_SUM) LS, SUM(RTCONG_SUM) CS, SUM(RTLOSS_SUM) OS, SUM(N) N FROM {DBO}.CM_PJM_DART_DAILY WHERE OBJECTID IN ({node},{hub}) AND DAY BETWEEN '{start}' AND '{staged_hi}' GROUP BY 1"))
    if live_lo:
        add(query(f"SELECT OBJECTID O, SUM(RTLMP) LS, SUM(COALESCE(RTCONG,0)) CS, SUM(COALESCE(RTLOSS,0)) OS, COUNT(*) N FROM {YES}.DART_PRICES WHERE OBJECTID IN ({node},{hub}) AND RTLMP IS NOT NULL AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE) GROUP BY 1"))

    def avg(o, i):
        a = agg.get(o)
        return (a[i] / a[3]) if (a and a[3]) else 0.0

    nl, nc, nlo = avg(node, 0), avg(node, 1), avg(node, 2)
    hl, hc, hlo = avg(hub, 0), avg(hub, 1), avg(hub, 2)
    ne, he = nl - nc - nlo, hl - hc - hlo

    return {
        "site": site.key, "name": site.display_name, "hub_name": site.hub_name,
        "as_of": end, "start": start, "days": days,
        "node_lmp": nl, "hub_lmp": hl,
        "node_energy": ne, "hub_energy": he,
        "node_cong": nc, "hub_cong": hc, "node_loss": nlo, "hub_loss": hlo,
        "energy_basis": ne - he, "congestion_basis": nc - hc, "loss_basis": nlo - hlo,
        "basis": nl - hl, "staged_through": sm,
    }
