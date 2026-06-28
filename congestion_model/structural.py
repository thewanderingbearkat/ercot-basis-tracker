"""Structural component: NBOHR basis from West-Texas export-constraint (GTC) binding.

WHY THIS IS SEPARATE FROM THE STATISTICAL MODEL. ERCOT basis = sum over binding
constraints of  -(shadow_price * (node_SF - hub_SF)). So a constraint's *live* shadow
price IS basis -- feeding it to the statistical model would be leakage (predicting basis
from basis). What's genuinely useful, and NOT leakage, is the *relative shift factor*
(node_SF - hub_SF): it's a near-constant property of the grid's wiring (topology), so it
lets us answer forward/structural what-ifs:

    Delta_basis_NBOHR  ~=  -(shadow_price * rel_SF)        per constraint

Measured over the last 12 months (constraint_map MARKET_SHIFT_FACTORS, RT), NBOHR's
relative shift factors on the dominant export constraints are remarkably stable:

    6965__A : rel_SF 0.267  (sd 0.017)   avg shadow $77   p90 $155
    6056__A : rel_SF 0.226  (sd 0.006)   avg shadow $84   p90 $202
    16050__B: rel_SF 0.206  (sd 0.210)   avg shadow $350  p90 $669   (severe, less stable)
    NELRIO  : rel_SF ~0                  -- binds often but NBOHR is immune (sanity check)

This is the hand-off target for the OOD rungs of the statistical scenario ladder: when a
what-if steps outside the historical envelope (new load behind the constraint, a binding
severity not seen), we price it here with physics instead of extrapolating statistics.

    python structural.py            # show the GTC stress ladder + validate vs realized
    python structural.py --refresh  # re-derive the sensitivities from Snowflake
"""
import os
import sys

# Dominant West-TX export constraints on NBOHR: relative shift factor (node-hub) + the
# binding-severity shadow-price anchors (avg / p90 $/MWh). Refresh with --refresh.
GTC = {
    "6965__A":  {"rel_sf": 0.267, "avg_shadow": 77.0,  "p90_shadow": 155.0},
    "6056__A":  {"rel_sf": 0.226, "avg_shadow": 84.0,  "p90_shadow": 202.0},
    "16050__B": {"rel_sf": 0.206, "avg_shadow": 350.0, "p90_shadow": 669.0},
}
NBOHR, HB_WEST = 10004202409, 10000697080


def basis_from_shadow(shadow_by_constraint: dict) -> float:
    """NBOHR basis ($/MWh) implied by a set of binding shadow prices: -(shadow * rel_SF)."""
    return sum(-shadow * GTC[c]["rel_sf"] for c, shadow in shadow_by_constraint.items() if c in GTC)


def stress(level: str = "p90", constraints=None) -> float:
    """NBOHR basis if the named export constraints all bind at the given severity anchor.
    level: 'typical' (avg shadow) or 'p90' (heavy). constraints: subset of GTC (default all)."""
    key = "avg_shadow" if level == "typical" else "p90_shadow"
    cons = constraints or list(GTC)
    return basis_from_shadow({c: GTC[c][key] for c in cons})


def new_load_behind(delta_mw: float, constraint: str = "6965__A", shadow_per_mw: float = 0.5) -> float:
    """First-order Delta-basis from a NEW load landing behind `constraint` that pushes it
    deeper into bind. Delta_shadow ~= shadow_per_mw * delta_mw (a tunable congestion-cost
    slope), then Delta_basis = -(Delta_shadow * rel_SF). shadow_per_mw is the rough $/MWh
    of shadow-price rise per extra MW of overload -- calibrate per constraint before relying
    on the magnitude; the SIGN and the rel_SF scaling are the structural part."""
    d_shadow = shadow_per_mw * delta_mw
    return -d_shadow * GTC[constraint]["rel_sf"]


def refresh():
    """Re-derive rel_SF + shadow anchors from Snowflake and print drop-in GTC dict."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    from constraint_map.db import query, YES
    names = "','".join(GTC)
    rows = query(f"""
      WITH msf AS (
        SELECT ANY_VALUE(CONSTRAINTNAME) NM, CONSTRAINTID, DATETIME, ANY_VALUE(SHADOWPRICE) SP,
          MAX(IFF(PRICENODEID={NBOHR}, SHIFTFACTOR, NULL)) nsf,
          MAX(IFF(PRICENODEID={HB_WEST}, SHIFTFACTOR, NULL)) hsf
        FROM {YES}.MARKET_SHIFT_FACTORS
        WHERE MARKET='RT' AND PRICENODEID IN ({NBOHR},{HB_WEST})
          AND CONSTRAINTNAME IN ('{names}') AND DATETIME >= DATEADD('year',-1,CURRENT_DATE)
        GROUP BY CONSTRAINTID, DATETIME)
      SELECT NM, ROUND(AVG(nsf-hsf),4) rel_sf, ROUND(STDDEV(nsf-hsf),4) sd,
             ROUND(AVG(ABS(SP)),1) avg_shadow,
             ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ABS(SP)),0) p90_shadow
      FROM msf WHERE nsf IS NOT NULL AND hsf IS NOT NULL AND SP IS NOT NULL AND SP!=0
      GROUP BY NM ORDER BY NM""")
    print("refreshed sensitivities:")
    for r in rows:
        print(f'    "{r["NM"]}": {{"rel_sf": {r["REL_SF"]}, "avg_shadow": {r["AVG_SHADOW"]}, '
              f'"p90_shadow": {r["P90_SHADOW"]}}},   # sd {r["SD"]}')


def _ladder():
    print("NBOHR GTC structural stress ladder ($/MWh basis):")
    print(f"  typical bind (avg shadow), all export constraints : {stress('typical'):+7.1f}")
    print(f"  heavy bind  (p90 shadow),  all export constraints : {stress('p90'):+7.1f}")
    print(f"  dominant 6965__A binds heavy (p90 $155)           : {basis_from_shadow({'6965__A': 155}):+7.1f}")
    print(f"  severe 16050__B binds at p90 ($669)               : {basis_from_shadow({'16050__B': 669}):+7.1f}")
    print(f"  new 200 MW load behind 6965__A (slope $0.5/MW)    : {new_load_behind(200):+7.1f}  (Delta on top of ambient)")


if __name__ == "__main__":
    if "--refresh" in sys.argv:
        refresh()
    else:
        _ladder()
