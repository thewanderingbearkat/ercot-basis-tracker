"""PJM Constraint Map: congestion + constraint attribution for our PJM assets.

Sibling to the ERCOT `constraint_map` package; reuses its Snowflake access
(`constraint_map.db`) and geometry layer (`constraint_map.geo`, which is
ISO-agnostic). PJM differs from ERCOT in two ways that shape this package:

  1. Node congestion is AUTHORITATIVE and finer-grained -- PJM publishes the
     congestion component of LMP (`DART_PRICES.RTCONG`), unlike ERCOT.
  2. Shift factors are MODELED + DAILY -- PJM doesn't publish nodal shift
     factors, so we use Yes Energy's `YES_ENERGY_SHIFT_FACTOR_BETA` (one snapshot
     per constraint-node per day, with a quality metric).

Validated 2026-06-23 at HAVILAND: modeled daily congestion tracks actual RTCONG
at r=+0.70 (direction good) but is ~6x overstated (magnitude bad). So we use
RTCONG for the dollar magnitude and BETA only to apportion it across constraints.

Exposed as the `pjm_constraints_bp` blueprint; see web.py.
"""
