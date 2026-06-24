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


def daily_attribution(site_key: str, days: int = 1, top: int = 12,
                      start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Attribution over a window ending at the last full operating day (or an
    explicit start/end). Modeled BETA shares x authoritative RTCONG magnitude."""
    site = SITES[site_key]
    if end is None:
        as_of = last_full_day()
        if as_of is None:
            return {"site": site.key, "name": site.display_name, "as_of": None,
                    "start": None, "days": days, "avg_congestion": 0.0, "drivers": []}
        end = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()

    # 1. Modeled congestion share per constraint over the window (BETA, daily/cheap).
    beta = query(f"""
        SELECT FACILITYID, CONTINGENCYID,
               ANY_VALUE(PNODENAME) AS PNODE,
               SUM(-(SHADOW_PRICE * SHIFT_FACTOR)) AS MODELED,
               AVG(QUALITY_METRIC)                 AS QUALITY,
               COUNT(DISTINCT CONSTRAINT_DAY)      AS DAYS_BOUND
        FROM {BETA}
        WHERE ISO = 'PJMISO' AND PNODEID = {site.node_id}
          AND CONSTRAINT_DAY BETWEEN '{start}' AND '{end}'
        GROUP BY FACILITYID, CONTINGENCYID
    """)
    modeled = [r for r in beta if r["MODELED"] is not None]
    total_modeled = sum(float(r["MODELED"]) for r in modeled) or 1.0

    # 2. Authoritative magnitude: average node RTCONG over the window, from staged
    #    daily aggregates + a live tail (fast for long windows).
    avg_cong = _avg_node_cong(site.node_id, start, end)

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
            "days_bound": r["DAYS_BOUND"],
            "quality": float(r["QUALITY"]) if r["QUALITY"] is not None else None,
        })
    drivers.sort(key=lambda d: d["attributed"])        # most negative (worst) first
    return {
        "site": site.key, "name": site.display_name,
        "as_of": end, "start": start,
        "days": (date.fromisoformat(end) - date.fromisoformat(start)).days + 1,
        "avg_congestion": avg_cong, "drivers": drivers[:top],
    }


def _facility_names(fac_ids: list[Any]) -> dict[Any, str]:
    ids = ",".join(str(int(f)) for f in dict.fromkeys(fac_ids) if f is not None)
    if not ids:
        return {}
    rows = query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({ids})")
    return {r["OBJECTID"]: r["FACILITYNAME"] for r in rows}
