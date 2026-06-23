"""Active-constraint analytics: what's binding now and how it hits our sites.

The two source tables:
  CONSTRAINTS              -- system-wide binding constraints per SCED interval,
                              with shadow PRICE ($/MWh), flow (VALUEMW), LIMITMW,
                              and VIOLATEDMW (>0 => over the limit).
  ERCOT_SCED_SHIFT_FACTORS -- per (constraint, settlement point) sensitivity:
                              how 1 MW injected at the node changes flow on the
                              constrained element.

Per-site congestion impact = -(PRICE * SHIFTFACTOR). The leading minus is the
ERCOT LMP decomposition convention: the marginal congestion component at a node
is MCC = -sum_k(shadow_price_k * shift_factor_k). A positive shift factor means
injecting at the node worsens the constrained flow, so the constraint pushes
that node's price DOWN. Hence IMPACT is the $/MWh the constraint adds to the
node's LMP: negative => depressing the node's price (worse basis for us),
positive => lifting it.

Sign convention validated 2026-06-23 against NBOHR_RN RTLMP net of HB_HUBAVG /
HB_WEST over 69 hours: the negated impact tracks realized basis at r = +0.77/
+0.78. The un-negated PRICE*SHIFTFACTOR correlated -0.77 (backwards), which is
why the minus sign is required.
"""
from __future__ import annotations

from typing import Any

from .db import YES, query
from .sites import SETTLEMENT_POINTS, SITES_BY_SP


def _sp_in_clause() -> str:
    return ", ".join(f"'{sp}'" for sp in SETTLEMENT_POINTS)

# The shift-factor table holds ~14 years / 2.6B rows; an unbounded MAX(DATETIME)
# scans all of it (~14s). The latest SCED interval is always minutes old, so we
# bound the search to recent partitions. The 2-day window is deliberately wide
# enough to absorb any session-vs-data timezone offset while still pruning to a
# sliver of partitions.
_RECENT = "DATETIME >= DATEADD('day', -2, CURRENT_TIMESTAMP)"
_LATEST_IV = f"(SELECT MAX(DATETIME) FROM {YES}.ERCOT_SCED_SHIFT_FACTORS WHERE {_RECENT})"


def latest_interval() -> str | None:
    """Most recent SCED interval that has shift factors (and thus impacts we can compute)."""
    rows = query(f"SELECT {_LATEST_IV} AS T")
    return rows[0]["T"] if rows and rows[0]["T"] else None


def active_constraints(at: str | None = None) -> dict[str, Any]:
    """Binding constraints at the latest (or given) interval that touch our sites.

    Returns {interval, constraints: [...]}, where each constraint carries its
    shadow price / flow / limit and a per-settlement-point impact map.
    """
    sql = f"""
    WITH iv AS (
        SELECT COALESCE(%(at)s::TIMESTAMP_NTZ, {_LATEST_IV}) AS T
    ),
    binding AS (
        SELECT c.CONSTRAINTID, c.FACILITYID, c.CONSTRAINTNAME, c.CONTINGENCY,
               c.CONTROLLINGACTION, c.REPORTED_NAME, c.PRICE, c.LIMITMW,
               c.VALUEMW, c.VIOLATEDMW, c.DATETIME
        FROM {YES}.CONSTRAINTS c, iv
        WHERE c.ISO = 'ERCOT' AND c.DATETIME = iv.T AND c.PRICE <> 0
    )
    SELECT b.CONSTRAINTID, b.FACILITYID, b.CONSTRAINTNAME, b.CONTINGENCY,
           b.CONTROLLINGACTION, b.REPORTED_NAME, b.PRICE, b.LIMITMW,
           b.VALUEMW, b.VIOLATEDMW, b.DATETIME,
           sf.SETTLEMENTPOINT, sf.SHIFTFACTOR,
           -(b.PRICE * sf.SHIFTFACTOR) AS IMPACT
    FROM binding b
    JOIN {YES}.ERCOT_SCED_SHIFT_FACTORS sf
      ON sf.DATETIME = b.DATETIME AND sf.CONSTRAINTID = b.CONSTRAINTID
    WHERE sf.SETTLEMENTPOINT IN ({_sp_in_clause()})
    ORDER BY ABS(b.PRICE * sf.SHIFTFACTOR) DESC
    """
    rows = query(sql, {"at": at})

    by_constraint: dict[Any, dict[str, Any]] = {}
    interval = None
    for r in rows:
        interval = interval or (r["DATETIME"].isoformat() if r["DATETIME"] else None)
        cid = r["CONSTRAINTID"]
        c = by_constraint.get(cid)
        if c is None:
            limit = float(r["LIMITMW"]) if r["LIMITMW"] is not None else None
            flow = float(r["VALUEMW"]) if r["VALUEMW"] is not None else None
            c = by_constraint[cid] = {
                "constraint_id": cid,
                "facility_id": r["FACILITYID"],
                "name": r["CONSTRAINTNAME"],
                "reported_name": r["REPORTED_NAME"],
                "contingency": r["CONTINGENCY"],
                "controlling_action": r["CONTROLLINGACTION"],
                "shadow_price": float(r["PRICE"]) if r["PRICE"] is not None else None,
                "flow_mw": flow,
                "limit_mw": limit,
                "violated_mw": float(r["VIOLATEDMW"]) if r["VIOLATEDMW"] is not None else None,
                "utilization": (flow / limit) if (limit and flow is not None) else None,
                "impacts": {},   # settlement_point -> {shift_factor, impact, sites}
            }
        sp = r["SETTLEMENTPOINT"]
        c["impacts"][sp] = {
            "settlement_point": sp,
            "shift_factor": float(r["SHIFTFACTOR"]) if r["SHIFTFACTOR"] is not None else None,
            "impact": float(r["IMPACT"]) if r["IMPACT"] is not None else None,
            "sites": SITES_BY_SP.get(sp, []),
        }

    return {"interval": interval, "constraints": list(by_constraint.values())}
