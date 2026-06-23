"""Our West Texas sites and how they map into the ERCOT shift-factor data.

Each site settles at an ERCOT settlement point; the binding-constraint impact
math keys off that settlement point (CONSTRAINTS x ERCOT_SCED_SHIFT_FACTORS).
Holstein, McCrae (BKII), and BKI are distinct businesses but McCrae and BKI
both settle at NBOHR_RN, so the per-node congestion impact is shared between
them.

Coordinates are approximate (county-level) and flagged for refinement -- they
position the map pins only; line geometry comes from HIFLD, not from here.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# Default basis reference hub (site LMP - hub LMP) -- our West Texas sites settle
# against HB_WEST. Sites can override per-asset (e.g. Aviator -> HB_NORTH).
HUB_NAME = "HB_WEST"
HUB_NODE_ID = 10000697080


@dataclass(frozen=True)
class Site:
    key: str
    display_name: str
    settlement_point: str          # ERCOT settlement point (join key into shift factors)
    resources: tuple[str, ...]     # ERCOT resource node names at this site
    fuel: str                      # solar | wind
    lat: float                     # approximate, county-level -- TODO refine
    lon: float
    coords_approx: bool = True
    county: str = ""
    price_node_id: int = 0         # PRICE_NODES.OBJECTID -> DART_PRICES (RT/DA LMP)
    hub_name: str = HUB_NAME       # basis reference hub (per-site override)
    hub_node_id: int = HUB_NODE_ID


# Keyed by site key. Note NBOHR_RN is shared by McCrae (BKII) and BKI.
SITES: dict[str, Site] = {
    "HOLSTEIN": Site(
        key="HOLSTEIN",
        display_name="Holstein Solar",
        settlement_point="HOLSTEIN_ALL",
        resources=("HOLSTEIN_SOLAR1", "HOLSTEIN_SOLAR2"),
        fuel="solar",
        # EIA-860: "Holstein 1 Solar Farm", Nolan County (200 MW).
        lat=32.1041, lon=-100.1624, county="Nolan", coords_approx=False,
        price_node_id=10016076881,
    ),
    "MCCRAE": Site(
        key="MCCRAE",
        display_name="McCrae (BKII)",
        settlement_point="NBOHR_RN",
        resources=("NBOHR_UNIT1",),
        fuel="wind",
        # EIA-860: "Bearkat", Glasscock County. BKI/BKII share the complex.
        lat=31.7272, lon=-101.5820, county="Glasscock", coords_approx=False,
        price_node_id=10004202409,
    ),
    "BKI": Site(
        key="BKI",
        display_name="Bearkat I",
        settlement_point="NBOHR_RN",
        resources=("NBOHR_UNIT1",),
        fuel="wind",
        # Co-located with McCrae/BKII at the Bearkat complex; tiny offset so the
        # two pins don't fully overlap on the map.
        lat=31.7322, lon=-101.5870, county="Glasscock", coords_approx=False,
        price_node_id=10004202409,
    ),
    "STANTON": Site(
        key="STANTON",
        display_name="Stanton (SWEC)",
        settlement_point="SWEC_G1",
        resources=("SWEC_G1",),
        fuel="wind",
        # EIA-860: "Stanton Wind Energy LLC", Martin County (120 MW).
        lat=32.2353, lon=-101.8367, county="Martin", coords_approx=False,
        price_node_id=10000698819,
    ),
    "AVIATOR": Site(
        key="AVIATOR",
        display_name="Aviator Wind",
        settlement_point="AVIAT_ALL",
        resources=(),                # aggregate settlement point; unit RNs not enumerated
        fuel="wind",
        # EIA-860: "Aviator Wind", Coke County (525 MW).
        lat=31.7926, lon=-100.6973, county="Coke", coords_approx=False,
        price_node_id=10016246152,
        # Settles against the North hub, not West/HubAvg.
        hub_name="HB_NORTH", hub_node_id=10000697078,
    ),
}

# Distinct settlement points we monitor (join key into the shift-factor table).
SETTLEMENT_POINTS: tuple[str, ...] = tuple(
    dict.fromkeys(s.settlement_point for s in SITES.values())
)

# settlement point -> list of site keys that settle there (NBOHR_RN -> McCrae, BKI)
SITES_BY_SP: dict[str, list[str]] = {}
for _s in SITES.values():
    SITES_BY_SP.setdefault(_s.settlement_point, []).append(_s.key)
