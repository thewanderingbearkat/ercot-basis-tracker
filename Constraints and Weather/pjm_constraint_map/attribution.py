"""Single-day constraint attribution for PJM nodes.

PJM shift factors are modeled + daily (`YES_ENERGY_SHIFT_FACTOR_BETA`), and
validated to track actual congestion in DIRECTION (r=0.70) but overstate
MAGNITUDE ~6x. So we never present the modeled dollars directly. Instead, for the
most recent COMPLETE operating day:

    1. From BETA, each constraint's modeled impact = -(SHADOW_PRICE * SHIFT_FACTOR)
       for that day -> a relative SHARE of congestion.
    2. The authoritative magnitude is the node's average RTCONG that same day.
    3. Attributed $/MWh per constraint = avg_RTCONG * (share of total modeled).

One clean calendar day in market time -- no rolling windows. Today is excluded
because it's still filling in; we use the last fully-settled day.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from constraint_map.basis import DBO, _staged_max
from constraint_map.db import YES, query

from .sites import SITES

BETA = f"{YES}.YES_ENERGY_SHIFT_FACTOR_BETA"


def _avg_node_cong(node_id: int, start: str, end: str) -> float:
    """Average node RTCONG over [start, end], from staged daily DART + live tail."""
    sm = _staged_max("CM_PJM_DART_DAILY")
    staged_hi = min(end, sm) if sm else None
    has_staged = bool(sm and start <= staged_hi)
    if sm is None:
        live_lo = start
    elif sm < end:
        live_lo = max(start, (date.fromisoformat(sm) + timedelta(days=1)).isoformat())
    else:
        live_lo = None
    csum, n = 0.0, 0
    if has_staged:
        r = query(f"SELECT SUM(RTCONG_SUM) S, SUM(N) N FROM {DBO}.CM_PJM_DART_DAILY WHERE OBJECTID={node_id} AND DAY BETWEEN '{start}' AND '{staged_hi}'")[0]
        if r["S"] is not None:
            csum += float(r["S"]); n += int(r["N"])
    if live_lo:
        r = query(f"SELECT SUM(COALESCE(RTCONG,0)) S, COUNT(*) N FROM {YES}.DART_PRICES WHERE OBJECTID={node_id} AND RTLMP IS NOT NULL AND DATETIME >= '{live_lo}' AND DATETIME < DATEADD('day',1,'{end}'::DATE)")[0]
        if r["S"] is not None:
            csum += float(r["S"]); n += int(r["N"])
    return (csum / n) if n else 0.0


def last_full_day():
    """Most recent COMPLETE PJM operating day (excludes today, still in progress)."""
    d = query(f"""SELECT MAX(CONSTRAINT_DAY) AS D FROM {BETA}
                  WHERE ISO = 'PJMISO' AND CONSTRAINT_DAY < CURRENT_DATE""")[0]["D"]
    return d.date() if (d is not None and hasattr(d, "date")) else d


def _apportion(pnode_id: int, avg_cong: float, start: str, end: str) -> dict[Any, dict[str, Any]]:
    """Apportion a pnode's authoritative avg RTCONG across constraints by modeled
    BETA shares (grouped by FACILITY). Returns {facility_id: {name, attributed}}."""
    beta = query(f"""
        SELECT FACILITYID,
               SUM(-(SHADOW_PRICE * SHIFT_FACTOR)) AS MODELED,
               COUNT(DISTINCT CONSTRAINT_DAY)      AS DAYS_BOUND
        FROM {BETA}
        WHERE ISO = 'PJMISO' AND PNODEID = {pnode_id}
          AND CONSTRAINT_DAY BETWEEN '{start}' AND '{end}'
        GROUP BY FACILITYID
    """)
    modeled = [r for r in beta if r["MODELED"] is not None]
    total = sum(float(r["MODELED"]) for r in modeled) or 1.0
    names = _facility_names([r["FACILITYID"] for r in modeled])
    return {r["FACILITYID"]: {"name": names.get(r["FACILITYID"], str(r["FACILITYID"])),
                              "attributed": avg_cong * (float(r["MODELED"]) / total),
                              "days_bound": r["DAYS_BOUND"]}
            for r in modeled}


def daily_attribution(site_key: str, days: int = 1, top: int = 14,
                      start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Per-constraint contribution to the BASIS congestion (node - hub), modeled.
    Each side is apportioned from authoritative RTCONG by BETA shares, then
    differenced per facility, so the drivers SUM to the congestion component of
    basis (= node avg - hub avg). Biggest basis movers first + an 'other' row."""
    site = SITES[site_key]
    if end is None:
        as_of = last_full_day()
        if as_of is None:
            return {"site": site.key, "name": site.display_name, "as_of": None, "start": None,
                    "days": days, "avg_congestion": 0.0, "hub_name": site.hub_name,
                    "hub_avg_congestion": 0.0, "congestion_basis": 0.0, "drivers": [],
                    "other_contrib": 0.0, "other_count": 0, "n_constraints": 0}
        end = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()

    node_avg = _avg_node_cong(site.node_id, start, end)
    hub_avg = _avg_node_cong(site.hub_node_id, start, end)
    nmap = _apportion(site.node_id, node_avg, start, end)
    hmap = _apportion(site.hub_node_id, hub_avg, start, end)

    drivers = []
    for fid in set(nmap) | set(hmap):
        n, h = nmap.get(fid), hmap.get(fid)
        node_part = n["attributed"] if n else 0.0
        hub_part = h["attributed"] if h else 0.0
        drivers.append({
            "facility_id": fid, "name": (n or h)["name"],
            "node_part": node_part, "hub_part": hub_part,
            "attributed": node_part - hub_part,   # contribution to BASIS congestion
            "days_bound": (n or h)["days_bound"],
        })
    congestion_basis = sum(d["attributed"] for d in drivers)   # == node_avg - hub_avg
    n_constraints = len(drivers)
    drivers.sort(key=lambda d: abs(d["attributed"]), reverse=True)
    shown = drivers[:top]
    other = congestion_basis - sum(d["attributed"] for d in shown)
    return {
        "site": site.key, "name": site.display_name,
        "as_of": end, "start": start,
        "days": (date.fromisoformat(end) - date.fromisoformat(start)).days + 1,
        "avg_congestion": node_avg, "hub_name": site.hub_name, "hub_avg_congestion": hub_avg,
        "congestion_basis": congestion_basis, "drivers": shown,
        "other_contrib": other, "other_count": n_constraints - len(shown),
        "n_constraints": n_constraints,
    }


def _facility_names(fac_ids: list[Any]) -> dict[Any, str]:
    ids = ",".join(str(int(f)) for f in dict.fromkeys(fac_ids) if f is not None)
    if not ids:
        return {}
    rows = query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({ids})")
    return {r["OBJECTID"]: r["FACILITYNAME"] for r in rows}
