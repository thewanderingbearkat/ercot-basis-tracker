"""Price-impact analytics: tie binding constraints to actual node prices.

Two views, both built on the same identity we validated against ERCOT:

    RT LMP(node) = Energy(hub) + Congestion + Loss,  where
    Congestion   = SUM over binding constraints of  -(shadow_price x shift_factor)

price_bridge(at)
    Point-in-time waterfall for each site: start at the hub LMP, walk down each
    binding constraint's congestion contribution, land near the site's RT LMP.
    A residual term absorbs losses + hub-side congestion + SCED-vs-settled timing.

historical_attribution(days)
    Over a window, rank the constraints that drove the most around-the-clock
    (ATC) congestion at each site. ATC contribution = total impact / all SCED
    intervals in the window -- i.e. the $/MWh this constraint added to the node's
    average price, summed to the site's average congestion.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .constraints import active_constraints
from .db import YES, query
from .geo import attach_geometry_list
from .sites import SITES


def _last_full_day() -> str:
    """Most recent complete operating day in the shift-factor feed (day before
    the latest interval), so attribution windows line up with the basis panel."""
    d = query(f"""
        SELECT DATEADD('day', -1, MAX(DATETIME)::DATE) AS D
        FROM {YES}.ERCOT_SCED_SHIFT_FACTORS
        WHERE DATETIME >= DATEADD('day', -2, CURRENT_TIMESTAMP)
    """)[0]["D"]
    return d.date().isoformat() if hasattr(d, "date") else str(d)


def _latest_lmp(node_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Most recent settled RT LMP per price-node OBJECTID."""
    ids = ",".join(str(i) for i in dict.fromkeys(node_ids) if i)
    if not ids:
        return {}
    rows = query(f"""
        SELECT OBJECTID, DATETIME, RTLMP
        FROM {YES}.DART_PRICES
        WHERE OBJECTID IN ({ids}) AND RTLMP IS NOT NULL
          AND DATETIME >= DATEADD('day', -2, CURRENT_TIMESTAMP)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY OBJECTID ORDER BY DATETIME DESC) = 1
    """)
    return {r["OBJECTID"]: {"datetime": r["DATETIME"].isoformat() if r["DATETIME"] else None,
                            "rtlmp": float(r["RTLMP"])} for r in rows}


def price_bridge(at: str | None = None) -> dict[str, Any]:
    res = active_constraints(at)
    # Per settlement point: each constraint's congestion contribution ($/MWh).
    contrib: dict[str, list[dict[str, Any]]] = {}
    for c in res["constraints"]:
        for sp, imp in c["impacts"].items():
            contrib.setdefault(sp, []).append({"name": c["name"], "impact": imp["impact"]})

    # Fetch every site node + every (possibly distinct) site hub.
    px = _latest_lmp([s.price_node_id for s in SITES.values()]
                     + [s.hub_node_id for s in SITES.values()])

    sites = []
    for s in SITES.values():
        cs = sorted(contrib.get(s.settlement_point, []), key=lambda x: x["impact"])
        modeled = sum(x["impact"] for x in cs)
        slmp = px.get(s.price_node_id)
        hlmp = px.get(s.hub_node_id)
        site_lmp = slmp["rtlmp"] if slmp else None
        hub_lmp = hlmp["rtlmp"] if hlmp else None
        basis = (site_lmp - hub_lmp) if (site_lmp is not None and hub_lmp is not None) else None
        sites.append({
            "key": s.key, "name": s.display_name, "settlement_point": s.settlement_point,
            "fuel": s.fuel,
            "hub_name": s.hub_name, "hub_lmp": hub_lmp, "site_lmp": site_lmp, "basis": basis,
            "modeled_congestion": modeled,
            "residual": (basis - modeled) if basis is not None else None,
            "constraints": cs,                       # most-negative first
            "price_time": slmp["datetime"] if slmp else None,
        })
    return {"interval": res["interval"], "sites": sites}


def historical_attribution(days: int = 30, top: int = 10) -> dict[str, Any]:
    sps = ",".join(f"'{sp}'" for sp in {s.settlement_point for s in SITES.values()})
    days = max(1, int(days))
    end = _last_full_day()
    start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    # `days` full operating days ending on `end`, matching the basis panel window.
    win = f"DATETIME >= '{start}' AND DATETIME < DATEADD('day', 1, '{end}'::DATE)"

    intervals = query(f"""
        SELECT COUNT(DISTINCT DATETIME) AS N FROM {YES}.ERCOT_SCED_SHIFT_FACTORS
        WHERE {win}
    """)[0]["N"] or 1

    rows = query(f"""
        SELECT sf.SETTLEMENTPOINT AS SP, c.CONSTRAINTNAME AS NAME,
               ANY_VALUE(c.CONSTRAINTID)  AS CID,
               ANY_VALUE(c.FACILITYID)    AS FID,
               SUM(-(c.PRICE * sf.SHIFTFACTOR)) / {intervals} AS ATC,
               AVG(-(c.PRICE * sf.SHIFTFACTOR))               AS WHEN_BIND,
               COUNT(*)                                       AS N_BIND
        FROM {YES}.CONSTRAINTS c
        JOIN {YES}.ERCOT_SCED_SHIFT_FACTORS sf
          ON sf.DATETIME = c.DATETIME AND sf.CONSTRAINTID = c.CONSTRAINTID
        WHERE c.ISO = 'ERCOT' AND c.PRICE <> 0
          AND sf.SETTLEMENTPOINT IN ({sps})
          AND c.DATETIME >= '{start}' AND c.DATETIME < DATEADD('day', 1, '{end}'::DATE)
        GROUP BY 1, 2
    """)

    by_sp: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_sp.setdefault(r["SP"], []).append({
            "constraint_id": r["CID"],
            "facility_id": r["FID"],
            "name": r["NAME"],
            "atc": float(r["ATC"]),
            "when_bind": float(r["WHEN_BIND"]),
            "hours_bound": float(r["N_BIND"]) / 12.0,    # 5-min intervals -> hours
            "pct_time": 100.0 * float(r["N_BIND"]) / intervals,
        })
    out = {}
    for sp, lst in by_sp.items():
        lst.sort(key=lambda x: x["atc"])                 # most negative (worst) first
        out[sp] = {"total_atc": sum(x["atc"] for x in lst), "drivers": lst[:top]}

    # Endpoint + voltage geometry for the displayed drivers (for the list
    # descriptions and click-to-zoom). Only the top-N per site, so it's cheap.
    attach_geometry_list([d for v in out.values() for d in v["drivers"]])
    return {"days": days, "as_of": end, "start": start, "intervals": intervals, "by_sp": out}
