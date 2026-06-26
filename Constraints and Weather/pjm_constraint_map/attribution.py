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


def _modeled(pnode_id: int, start: str, end: str, days: int) -> dict[Any, dict[str, Any]]:
    """Raw modeled congestion per facility at a pnode -- the colleague's exact method:
    join the BINDING constraints (ALL_CONSTRAINTS_PMV.price) to the BETA shift factors
    on facility + contingency + day, take -(price * shift_factor) summed over the
    window's hours, and divide by the hours in the window (24/day) -> a daily-average
    $/MWh. NOT rescaled to authoritative RTCONG -- the gap is surfaced as model noise
    (BETA shift factors are daily + modeled; the residual also absorbs loss/topology
    effects the shift-factor model doesn't carry). Returns {facility_id:{name,modeled}}.

    Sign note: -(price * shift_factor) gives the LMP congestion-component convention
    (negative = constraint depressing the node), so node - hub ties to basis. The
    colleague's raw cong_d omits the leading minus (so their sign is flipped)."""
    # Operating day on FIXED EST (Etc/GMT+5, no DST) to match the colleague's slice:
    # the data is America/New_York (observes DST), so their EST day starts an hour
    # earlier in summer -> one boundary interval differs. The raw DATETIME guard keeps
    # partition pruning; the converted date picks the exact operating-day intervals.
    # (sf still joins on the data-date, same as the colleague.)
    rows = query(f"""
        SELECT con.FACILITYID,
               SUM(-(con.PRICE * sf.SHIFT_FACTOR)) AS MODELED_SUM,
               COUNT(DISTINCT con.DATETIME)        AS HOURS_BOUND
        FROM {YES}.ALL_CONSTRAINTS_PMV con
        JOIN {BETA} sf
          ON con.FACILITYID = sf.FACILITYID AND con.CONTINGENCYID = sf.CONTINGENCYID
             AND sf.CONSTRAINT_DAY = CAST(con.DATETIME AS DATE)
        WHERE con.ISO = 'PJMISO' AND sf.PNODEID = {pnode_id}
          AND con.DATETIME >= DATEADD('day', -1, '{start}') AND con.DATETIME < DATEADD('day', 2, '{end}')
          AND CONVERT_TIMEZONE('America/New_York', 'Etc/GMT+5', con.DATETIME)::DATE
                BETWEEN '{start}' AND '{end}'
        GROUP BY con.FACILITYID
    """)
    rows = [r for r in rows if r["MODELED_SUM"] is not None]
    names = _facility_names([r["FACILITYID"] for r in rows])
    denom = 24.0 * max(1, days)
    return {r["FACILITYID"]: {"name": names.get(r["FACILITYID"], str(r["FACILITYID"])),
                              "modeled": float(r["MODELED_SUM"]) / denom,
                              "days_bound": r["HOURS_BOUND"]}
            for r in rows}


def daily_attribution(site_key: str, days: int = 1, top: int = 14,
                      start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Per-constraint contribution to the BASIS congestion (node - hub), using the
    RAW shift-factor model (the colleague's methodology): each side is the direct
    window-average of -(shadow_price * shift_factor), differenced per facility -- NOT
    rescaled to authoritative RTCONG. The drivers sum to the MODELED basis congestion;
    the gap to the authoritative RTCONG basis (model error + loss/topology effects) is
    returned as `model_noise`, so drivers + other + noise tie to the Congestion line."""
    site = SITES[site_key]
    if end is None:
        as_of = last_full_day()
        if as_of is None:
            return {"site": site.key, "name": site.display_name, "as_of": None, "start": None,
                    "days": days, "avg_congestion": 0.0, "hub_name": site.hub_name,
                    "hub_avg_congestion": 0.0, "congestion_basis": 0.0, "modeled_basis": 0.0,
                    "model_noise": 0.0, "drivers": [],
                    "other_contrib": 0.0, "other_count": 0, "n_constraints": 0}
        end = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
    if start is None:
        days = max(1, int(days))
        start = (date.fromisoformat(end) - timedelta(days=days - 1)).isoformat()
    days_n = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1

    node_avg = _avg_node_cong(site.node_id, start, end)
    hub_avg = _avg_node_cong(site.hub_node_id, start, end)
    nmap = _modeled(site.node_id, start, end, days_n)
    hmap = _modeled(site.hub_node_id, start, end, days_n)

    drivers = []
    for fid in set(nmap) | set(hmap):
        n, h = nmap.get(fid), hmap.get(fid)
        node_part = n["modeled"] if n else 0.0
        hub_part = h["modeled"] if h else 0.0
        drivers.append({
            "facility_id": fid, "name": (n or h)["name"],
            "node_part": node_part, "hub_part": hub_part,
            "attributed": node_part - hub_part,   # raw modeled contribution to BASIS congestion
            "days_bound": (n or h)["days_bound"],
        })
    modeled_basis = sum(d["attributed"] for d in drivers)   # raw shift-factor model total
    authoritative_basis = node_avg - hub_avg                # RTCONG -- the real congestion in basis
    model_noise = authoritative_basis - modeled_basis       # residual: model error + loss/topology
    n_constraints = len(drivers)
    drivers.sort(key=lambda d: abs(d["attributed"]), reverse=True)
    shown = drivers[:top]
    other = modeled_basis - sum(d["attributed"] for d in shown)
    return {
        "site": site.key, "name": site.display_name,
        "as_of": end, "start": start, "days": days_n,
        "avg_congestion": node_avg, "hub_name": site.hub_name, "hub_avg_congestion": hub_avg,
        "congestion_basis": authoritative_basis,   # authoritative -- ties to the Congestion line
        "modeled_basis": modeled_basis, "model_noise": model_noise,
        "drivers": shown, "other_contrib": other, "other_count": n_constraints - len(shown),
        "n_constraints": n_constraints,
    }


def _facility_names(fac_ids: list[Any]) -> dict[Any, str]:
    ids = ",".join(str(int(f)) for f in dict.fromkeys(fac_ids) if f is not None)
    if not ids:
        return {}
    rows = query(f"SELECT OBJECTID, FACILITYNAME FROM {YES}.FACILITIES WHERE OBJECTID IN ({ids})")
    return {r["OBJECTID"]: r["FACILITYNAME"] for r in rows}
