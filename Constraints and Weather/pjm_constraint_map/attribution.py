"""Daily constraint attribution for PJM nodes.

PJM shift factors are modeled + daily (`YES_ENERGY_SHIFT_FACTOR_BETA`), and
validated to track actual congestion in DIRECTION (r=0.70) but overstate
MAGNITUDE ~6x. So we never present the modeled dollars directly. Instead:

    1. From BETA, each constraint's modeled impact = -(SHADOW_PRICE * SHIFT_FACTOR),
       summed over the window per constraint -> a relative SHARE of congestion.
    2. The authoritative magnitude is the node's average RTCONG over the window.
    3. Attributed $/MWh per constraint = avg_RTCONG * (share of total modeled).

This yields real congestion dollars, split across constraints by the modeled
shares -- using each source for what it's good at.
"""
from __future__ import annotations

from typing import Any

from constraint_map.db import YES, query

from .sites import SITES

BETA = f"{YES}.YES_ENERGY_SHIFT_FACTOR_BETA"


def daily_attribution(site_key: str, days: int = 30, top: int = 12) -> dict[str, Any]:
    site = SITES[site_key]

    # 1. Modeled congestion share per constraint over the window (BETA, daily).
    beta = query(f"""
        SELECT FACILITYID, CONTINGENCYID,
               ANY_VALUE(PNODENAME) AS PNODE,
               SUM(-(SHADOW_PRICE * SHIFT_FACTOR)) AS MODELED,
               AVG(QUALITY_METRIC)                 AS QUALITY,
               COUNT(DISTINCT CONSTRAINT_DAY)      AS DAYS_BOUND
        FROM {BETA}
        WHERE ISO = 'PJMISO' AND PNODEID = {site.node_id}
          AND CONSTRAINT_DAY >= DATEADD('day', -{int(days)}, CURRENT_DATE)
        GROUP BY FACILITYID, CONTINGENCYID
    """)
    modeled = [r for r in beta if r["MODELED"] is not None]
    total_modeled = sum(float(r["MODELED"]) for r in modeled) or 1.0

    # 2. Authoritative magnitude: average RTCONG over the window.
    avg = query(f"""
        SELECT AVG(RTCONG) AS C FROM {YES}.DART_PRICES
        WHERE OBJECTID = {site.node_id} AND RTCONG IS NOT NULL
          AND DATETIME >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP)
    """)[0]["C"]
    avg_cong = float(avg) if avg is not None else 0.0

    # 3. Apportion the real congestion by modeled share. Resolve constraint name
    #    + endpoint geometry via the shared facility chain.
    fac_ids = [r["FACILITYID"] for r in modeled]
    names = _facility_names(fac_ids)
    drivers = []
    for r in modeled:
        share = float(r["MODELED"]) / total_modeled
        drivers.append({
            "facility_id": r["FACILITYID"],
            "name": names.get(r["FACILITYID"], str(r["FACILITYID"])),
            "share": share,
            "attributed": avg_cong * share,           # $/MWh of real congestion
            "days_bound": r["DAYS_BOUND"],
            "pct_days": 100.0 * r["DAYS_BOUND"] / max(1, days),
            "quality": float(r["QUALITY"]) if r["QUALITY"] is not None else None,
        })
    drivers.sort(key=lambda d: d["attributed"])        # most negative (worst) first
    return {
        "site": site.key, "name": site.display_name, "days": days,
        "avg_congestion": avg_cong, "drivers": drivers[:top],
    }


def _facility_names(fac_ids: list[Any]) -> dict[Any, str]:
    ids = ",".join(str(int(f)) for f in dict.fromkeys(fac_ids) if f is not None)
    if not ids:
        return {}
    rows = query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({ids})")
    return {r["OBJECTID"]: r["FACILITYNAME"] for r in rows}
