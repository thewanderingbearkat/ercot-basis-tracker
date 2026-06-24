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
from .geo import attach_geometry_list
from .sites import SITES

MSF = f"{YES}.MARKET_SHIFT_FACTORS"
DBO = "SKYVEST.DBO"   # precomputed daily aggregates (see staging/stage.py)
# All monitored ERCOT price nodes (sites + hubs) -- matches the staged interval grid.
E_ALL_NODES = ",".join(str(i) for i in sorted(
    {s.price_node_id for s in SITES.values()} | {s.hub_node_id for s in SITES.values()}))


def last_full_day() -> str:
    """Most recent complete operating day (the day before the latest RT data)."""
    d = query(f"SELECT DATEADD('day', -1, MAX(DATETIME)::DATE) AS D FROM {MSF} WHERE MARKET='RT'")[0]["D"]
    return d.date().isoformat() if hasattr(d, "date") else str(d)


def _staged_max(table: str) -> str | None:
    """Latest DAY in a staging table, or None if staging is unavailable/empty
    (in which case callers fall back to the full live path)."""
    try:
        d = query(f"SELECT MAX(DAY) AS D FROM {DBO}.{table}")[0]["D"]
        return d.isoformat() if hasattr(d, "isoformat") else (str(d) if d else None)
    except Exception:
        return None


def basis_decomposition(site_key: str, days: int = 1, top: int = 15,
                        start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Decompose node-hub basis over a window. Reads precomputed daily aggregates
    from SKYVEST.DBO for the stale history and computes only the live tail (days
    after the staged max) from the heavy source tables -- so 1Y/3Y stays fast.
    Pass days (window ending on the last full day) or an explicit start/end."""
    site = SITES[site_key]
    node, hub = site.price_node_id, site.hub_node_id
    if end is None:
        end = last_full_day()
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1

    # Split the window: STAGED [start .. staged_hi] + LIVE tail (staged_max .. end].
    sm = _staged_max("CM_ERCOT_SF_DAILY")
    staged_hi = min(end, sm) if sm else None
    has_staged = bool(sm and start <= staged_hi)
    if sm is None:
        live_lo = start                                     # no staging -> all live
    elif sm < end:
        live_lo = max(start, (date.fromisoformat(sm) + timedelta(days=1)).isoformat())
    else:
        live_lo = None                                      # staged covers the window

    nodeS: dict[Any, float] = {}
    hubS: dict[Any, float] = {}
    names: dict[Any, Any] = {}
    facs: dict[Any, Any] = {}

    def add_sf(rows):
        for r in rows:
            d = nodeS if r["PN"] == node else hubS
            d[r["CID"]] = d.get(r["CID"], 0.0) + float(r["S"])
            names.setdefault(r["CID"], r["NM"]); facs.setdefault(r["CID"], r["FID"])

    if has_staged:
        add_sf(query(f"""
            SELECT PRICENODEID PN, CONSTRAINTID CID, ANY_VALUE(CONSTRAINTNAME) NM,
                   ANY_VALUE(FACILITYID) FID, SUM(SF_SUM) S
            FROM {DBO}.CM_ERCOT_SF_DAILY
            WHERE PRICENODEID IN ({node},{hub}) AND DAY BETWEEN '{start}' AND '{staged_hi}'
            GROUP BY 1, 2"""))
    if live_lo:
        add_sf(query(f"""
            SELECT PRICENODEID PN, CONSTRAINTID CID, ANY_VALUE(CONSTRAINTNAME) NM,
                   ANY_VALUE(FACILITYID) FID, SUM(-(SHADOWPRICE*SHIFTFACTOR)) S
            FROM {MSF} WHERE MARKET='RT' AND PRICENODEID IN ({node},{hub})
              AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE)
            GROUP BY 1, 2"""))

    # Interval denominator (over the monitored-node grid), staged + live.
    iv = 0
    if has_staged:
        iv += query(f"SELECT COALESCE(SUM(N_INTERVALS),0) N FROM {DBO}.CM_ERCOT_INTERVALS_DAILY WHERE DAY BETWEEN '{start}' AND '{staged_hi}'")[0]["N"]
    if live_lo:
        iv += query(f"SELECT COUNT(DISTINCT DATETIME) N FROM {MSF} WHERE MARKET='RT' AND PRICENODEID IN ({E_ALL_NODES}) AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE)")[0]["N"]
    iv = iv or 1

    # Basis from RT LMP sums (node - hub), staged + live.
    nsum = hsum = 0.0
    nn = hn = 0

    def add_lmp(rows):
        nonlocal nsum, hsum, nn, hn
        for r in rows:
            if r["O"] == node:
                nsum += float(r["S"]); nn += int(r["N"])
            else:
                hsum += float(r["S"]); hn += int(r["N"])

    if has_staged:
        add_lmp(query(f"SELECT OBJECTID O, SUM(RTLMP_SUM) S, SUM(N) N FROM {DBO}.CM_ERCOT_LMP_DAILY WHERE OBJECTID IN ({node},{hub}) AND DAY BETWEEN '{start}' AND '{staged_hi}' GROUP BY 1"))
    if live_lo:
        add_lmp(query(f"SELECT OBJECTID O, SUM(RTLMP) S, COUNT(*) N FROM {YES}.DART_PRICES WHERE OBJECTID IN ({node},{hub}) AND RTLMP IS NOT NULL AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE) GROUP BY 1"))
    node_lmp = (nsum / nn) if nn else None
    hub_lmp = (hsum / hn) if hn else None
    basis = (node_lmp - hub_lmp) if (node_lmp is not None and hub_lmp is not None) else 0.0

    cids = set(nodeS) | set(hubS)
    drivers = [{"constraint_id": c, "constraint_name": names.get(c), "facility_id": facs.get(c),
                "name": names.get(c), "contrib": (nodeS.get(c, 0.0) - hubS.get(c, 0.0)) / iv}
               for c in cids]
    congestion_basis = sum(d["contrib"] for d in drivers)   # over ALL constraints
    n_constraints = len(drivers)
    # Rank by magnitude (both signs); roll the rest into an explicit "other" row
    # so the displayed bars + other == congestion_basis exactly.
    drivers.sort(key=lambda d: abs(d["contrib"]), reverse=True)
    shown = drivers[:top]
    other_contrib = congestion_basis - sum(d["contrib"] for d in shown)
    _name_and_locate(shown)

    return {
        "site": site.key, "name": site.display_name, "hub_name": site.hub_name,
        "as_of": end, "start": start, "days": days,
        "node_lmp": node_lmp, "hub_lmp": hub_lmp,
        "basis": basis, "congestion_basis": congestion_basis,
        "residual": basis - congestion_basis,
        "drivers": shown,
        "other_contrib": other_contrib,
        "other_count": n_constraints - len(shown),
        "n_constraints": n_constraints,
        "staged_through": sm,
    }


def _name_and_locate(drivers: list[dict[str, Any]]) -> None:
    """Label each driver with its FACILITY name (from FACILITIES) and attach map
    geometry, both keyed off the FACILITYID that MARKET_SHIFT_FACTORS already
    carries per constraint. Falls back to the constraint name when a facility has
    no FACILITIES row. Reuses the active-map geo chain (FACILITIES -> STATIONS_GEO
    -> HIFLD routing)."""
    fids = [int(d["facility_id"]) for d in drivers if d.get("facility_id") is not None]
    names: dict[Any, str] = {}
    if fids:
        in_clause = ",".join(str(f) for f in dict.fromkeys(fids))
        for r in query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({in_clause})"):
            names[r["OBJECTID"]] = r["FACILITYNAME"]
    for d in drivers:
        d["name"] = names.get(d["facility_id"]) or d.get("constraint_name") or str(d.get("facility_id"))
    attach_geometry_list(drivers)
