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

from typing import Any

from constraint_map.db import YES, query

from .sites import SITES

BETA = f"{YES}.YES_ENERGY_SHIFT_FACTOR_BETA"


def last_full_day() -> Any:
    """Most recent COMPLETE PJM operating day (excludes today, still in progress)."""
    return query(f"""SELECT MAX(CONSTRAINT_DAY) AS D FROM {BETA}
                     WHERE ISO = 'PJMISO' AND CONSTRAINT_DAY < CURRENT_DATE""")[0]["D"]


def daily_attribution(site_key: str, top: int = 12) -> dict[str, Any]:
    site = SITES[site_key]
    as_of = last_full_day()
    if as_of is None:
        return {"site": site.key, "name": site.display_name, "as_of": None,
                "avg_congestion": 0.0, "drivers": []}
    day = as_of.date().isoformat() if hasattr(as_of, "date") else str(as_of)

    # 1. Modeled congestion share per constraint for that single day.
    beta = query(f"""
        SELECT FACILITYID, CONTINGENCYID,
               ANY_VALUE(PNODENAME) AS PNODE,
               SUM(-(SHADOW_PRICE * SHIFT_FACTOR)) AS MODELED,
               AVG(QUALITY_METRIC)                 AS QUALITY
        FROM {BETA}
        WHERE ISO = 'PJMISO' AND PNODEID = {site.node_id}
          AND CONSTRAINT_DAY = '{day}'
        GROUP BY FACILITYID, CONTINGENCYID
    """)
    modeled = [r for r in beta if r["MODELED"] is not None]
    total_modeled = sum(float(r["MODELED"]) for r in modeled) or 1.0

    # 2. Authoritative magnitude: average RTCONG over that same calendar day
    #    (market-time NTZ boundaries -- no CURRENT_TIMESTAMP / timezone drift).
    avg = query(f"""
        SELECT AVG(RTCONG) AS C FROM {YES}.DART_PRICES
        WHERE OBJECTID = {site.node_id} AND RTCONG IS NOT NULL
          AND DATETIME >= '{day}' AND DATETIME < DATEADD('day', 1, '{day}'::DATE)
    """)[0]["C"]
    avg_cong = float(avg) if avg is not None else 0.0

    # 3. Apportion the real congestion by modeled share.
    names = _facility_names([r["FACILITYID"] for r in modeled])
    drivers = []
    for r in modeled:
        share = float(r["MODELED"]) / total_modeled
        drivers.append({
            "facility_id": r["FACILITYID"],
            "name": names.get(r["FACILITYID"], str(r["FACILITYID"])),
            "share": share,
            "attributed": avg_cong * share,           # $/MWh of real congestion
            "quality": float(r["QUALITY"]) if r["QUALITY"] is not None else None,
        })
    drivers.sort(key=lambda d: d["attributed"])        # most negative (worst) first
    return {
        "site": site.key, "name": site.display_name, "as_of": day,
        "avg_congestion": avg_cong, "drivers": drivers[:top],
    }


def _facility_names(fac_ids: list[Any]) -> dict[Any, str]:
    ids = ",".join(str(int(f)) for f in dict.fromkeys(fac_ids) if f is not None)
    if not ids:
        return {}
    rows = query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({ids})")
    return {r["OBJECTID"]: r["FACILITYNAME"] for r in rows}
