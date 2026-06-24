"""Basis decomposition for PJM nodes: Energy + Congestion + Losses -> basis.

PJM publishes the three LMP components (RTCONG, RTLOSS), so basis = node LMP -
hub LMP splits *exactly* into:

    energy-basis = node energy  - hub energy   (~0; system energy is uniform)
    cong-basis   = node RTCONG  - hub RTCONG    (constraint-driven)
    loss-basis   = node RTLOSS  - hub RTLOSS    (topology / loss-driven)

All authoritative -- ties to settlement prices. Averaged over the same window as
the attribution (last full operating day, or a trailing N-day window ending
there). The per-constraint split of the congestion piece is the modeled BETA
attribution (see attribution.py) for the node and the hub.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from constraint_map.db import YES, query

from .attribution import daily_attribution, last_full_day
from .sites import SITES


def basis_decomposition(site_key: str, days: int = 1) -> dict[str, Any]:
    site = SITES[site_key]
    as_of = last_full_day()
    if as_of is None:
        return {"site": site.key, "name": site.display_name, "as_of": None,
                "start": None, "days": days}
    start = as_of - timedelta(days=int(days) - 1)

    rows = query(f"""
        SELECT OBJECTID,
               AVG(RTLMP)                                          AS LMP,
               AVG(RTCONG)                                         AS CONG,
               AVG(RTLOSS)                                         AS LOSS,
               AVG(RTLMP - COALESCE(RTCONG,0) - COALESCE(RTLOSS,0)) AS ENERGY
        FROM {YES}.DART_PRICES
        WHERE OBJECTID IN ({site.node_id}, {site.hub_node_id}) AND RTLMP IS NOT NULL
          AND DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{as_of}'::DATE)
        GROUP BY OBJECTID
    """)
    by = {r["OBJECTID"]: r for r in rows}
    n, h = by.get(site.node_id), by.get(site.hub_node_id)

    def c(r, k):
        return float(r[k]) if (r and r.get(k) is not None) else 0.0

    out = {
        "site": site.key, "name": site.display_name, "hub_name": site.hub_name,
        "as_of": str(as_of), "start": str(start), "days": int(days),
        "node_lmp": c(n, "LMP"), "hub_lmp": c(h, "LMP"),
        "node_energy": c(n, "ENERGY"), "hub_energy": c(h, "ENERGY"),
        "node_cong": c(n, "CONG"), "hub_cong": c(h, "CONG"),
        "node_loss": c(n, "LOSS"), "hub_loss": c(h, "LOSS"),
        "energy_basis": c(n, "ENERGY") - c(h, "ENERGY"),
        "congestion_basis": c(n, "CONG") - c(h, "CONG"),
        "loss_basis": c(n, "LOSS") - c(h, "LOSS"),
        "basis": c(n, "LMP") - c(h, "LMP"),
    }
    # Per-constraint drivers of each side's congestion (modeled BETA; reliable
    # individually). congestion_basis = sum(node drivers) - sum(hub drivers).
    out["node_drivers"] = daily_attribution(site_key, days=days)["drivers"]
    return out
