"""ERCOT basis decomposition -- which constraints drive the node away from its hub.

ERCOT runs a near-lossless energy market (no marginal loss component in LMP), so
basis = node LMP - hub LMP is almost entirely CONGESTION. And ERCOT shift factors
are AUTHORITATIVE: MARKET_SHIFT_FACTORS carries 5-minute RT shift factors +
shadow prices for nodes AND hubs. So unlike PJM, we can attribute the basis to
the actual binding constraints, and they SUM to it:

    contribution per constraint = -(shadow_price * (node_SF - hub_SF))   (RT, 5-min)
    congestion-basis = sum over constraints
    residual = basis - congestion-basis   (small: 5-min/hourly timing + reference)

Validated 2026-06-22: congestion-basis ties to actual node-hub RT LMP basis within
~0.4-2 $/MWh (AVIAT 0.40, HOLSTEIN 0.49, NBOHR 2.05).

Windows (1/7/30/90 days) are anchored to the last full operating day -- the 1D
view is that single settled day, the longer windows are trailing ranges ending on
it. Each driver also carries map geometry (CONSTRAINTID -> FACILITYID -> stations),
so the map can color the conductors by their basis contribution.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .db import YES, query
from .geo import facility_geometry, routed_path
from .sites import SITES

MSF = f"{YES}.MARKET_SHIFT_FACTORS"


def last_full_day() -> str:
    """Most recent complete operating day (the day before the latest RT data)."""
    d = query(f"SELECT DATEADD('day', -1, MAX(DATETIME)::DATE) AS D FROM {MSF} WHERE MARKET='RT'")[0]["D"]
    return d.date().isoformat() if hasattr(d, "date") else str(d)


def basis_decomposition(site_key: str, days: int = 1, top: int = 15) -> dict[str, Any]:
    site = SITES[site_key]
    days = max(1, int(days))
    end = last_full_day()
    start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    # `days` full operating days ending on `end`: [start 00:00, end+1day).
    win = f"DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{end}'::DATE)"

    # Authoritative basis from DART RT LMP (node - hub), averaged over the window.
    b = query(f"""
        SELECT AVG(n.RTLMP - h.RTLMP) AS BASIS, AVG(n.RTLMP) AS NLMP, AVG(h.RTLMP) AS HLMP
        FROM (SELECT DATETIME, RTLMP FROM {YES}.DART_PRICES
              WHERE OBJECTID={site.price_node_id}
                AND DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{end}'::DATE)
                AND RTLMP IS NOT NULL) n
        JOIN (SELECT DATETIME, RTLMP FROM {YES}.DART_PRICES
              WHERE OBJECTID={site.hub_node_id}
                AND DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{end}'::DATE)
                AND RTLMP IS NOT NULL) h
          ON n.DATETIME = h.DATETIME
    """)[0]
    basis = float(b["BASIS"]) if b["BASIS"] is not None else 0.0

    # Intervals in the window (denominator to turn interval-sums into averages).
    iv = query(f"SELECT COUNT(DISTINCT DATETIME) AS N FROM {MSF} WHERE MARKET='RT' AND {win}")[0]["N"] or 1

    # Per-constraint differential contribution to basis = -(SP*(node_SF - hub_SF)),
    # summed over the window and averaged. node/hub summed separately then differenced
    # (SP is the same for both at a given interval, so this is the differential).
    rows = query(f"""
        WITH node AS (
            SELECT CONSTRAINTID, ANY_VALUE(CONSTRAINTNAME) NM,
                   SUM(-(SHADOWPRICE * SHIFTFACTOR)) S
            FROM {MSF} WHERE PRICENODEID={site.price_node_id} AND MARKET='RT' AND {win}
            GROUP BY CONSTRAINTID),
        hub AS (
            SELECT CONSTRAINTID, ANY_VALUE(CONSTRAINTNAME) NM,
                   SUM(-(SHADOWPRICE * SHIFTFACTOR)) S
            FROM {MSF} WHERE PRICENODEID={site.hub_node_id} AND MARKET='RT' AND {win}
            GROUP BY CONSTRAINTID)
        SELECT COALESCE(n.CONSTRAINTID, h.CONSTRAINTID) CID,
               COALESCE(n.NM, h.NM) NM,
               (COALESCE(n.S, 0) - COALESCE(h.S, 0)) / {iv} AS CONTRIB
        FROM node n FULL OUTER JOIN hub h ON n.CONSTRAINTID = h.CONSTRAINTID
    """)
    drivers = [{"constraint_id": r["CID"], "name": r["NM"], "contrib": float(r["CONTRIB"])}
               for r in rows if r["CONTRIB"] is not None]
    congestion_basis = sum(d["contrib"] for d in drivers)
    drivers.sort(key=lambda d: d["contrib"])   # most negative (widens basis) first
    drivers = drivers[:top]
    _attach_geometry(drivers, start, end)

    return {
        "site": site.key, "name": site.display_name, "hub_name": site.hub_name,
        "as_of": end, "start": start, "days": days,
        "node_lmp": float(b["NLMP"]) if b["NLMP"] is not None else None,
        "hub_lmp": float(b["HLMP"]) if b["HLMP"] is not None else None,
        "basis": basis, "congestion_basis": congestion_basis,
        "residual": basis - congestion_basis,
        "drivers": drivers,
    }


def _attach_geometry(drivers: list[dict[str, Any]], start: str, end: str) -> None:
    """Resolve each driver's conductor geometry so the map can route it.

    MARKET_SHIFT_FACTORS keys on CONSTRAINTID; geometry keys on FACILITYID. The
    CONSTRAINTS table carries both, so we map CONSTRAINTID -> FACILITYID over the
    same window, then reuse the active-map geo chain (FACILITIES -> STATIONS_GEO
    -> HIFLD routing). Undrawable drivers still show in the basis list, just not
    on the map.
    """
    cids = [int(d["constraint_id"]) for d in drivers if d.get("constraint_id") is not None]
    if not cids:
        return
    in_clause = ",".join(str(c) for c in cids)
    fac: dict[Any, Any] = {}
    for r in query(f"""
        SELECT DISTINCT CONSTRAINTID, FACILITYID FROM {YES}.CONSTRAINTS
        WHERE ISO='ERCOT' AND CONSTRAINTID IN ({in_clause})
          AND DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{end}'::DATE)
          AND FACILITYID IS NOT NULL
    """):
        fac.setdefault(r["CONSTRAINTID"], r["FACILITYID"])
    geo = facility_geometry([v for v in fac.values() if v is not None])
    for d in drivers:
        g = geo.get(fac.get(d["constraint_id"]))
        if not g:
            d["geometry"] = {"from": None, "to": None, "voltage": None,
                             "drawable": False, "path": None, "snapped": False}
            continue
        drawable = bool(g["from"] and g["to"])
        path = routed_path(g["from"], g["to"]) if drawable else None
        d["geometry"] = {**g, "drawable": drawable, "path": path, "snapped": path is not None}
