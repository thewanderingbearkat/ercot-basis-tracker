"""Our PJM assets and how they map into Yes Energy / the BETA shift factors.

Each site settles at a PJM pnode (`PRICE_NODES.OBJECTID`, which is also the
`DART_PRICES.OBJECTID` and the `YES_ENERGY_SHIFT_FACTOR_BETA.PNODEID`). Basis is
node vs the AEP-Dayton hub, which is what matters commercially -- NWOH's PPA
settles at the hub, so the desk keeps node-minus-hub.

Coordinates are approximate (town-level) and flagged for refinement.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Site:
    key: str
    display_name: str
    pnode_name: str                # Yes Energy PRICE_NODES.PNODENAME
    node_id: int                   # OBJECTID == DART_PRICES.OBJECTID == BETA.PNODEID
    fuel: str                      # wind | solar | ...
    lat: float
    lon: float
    coords_approx: bool = True
    county: str = ""
    state: str = ""


# PJM system hub used as the basis reference (node LMP - hub LMP).
HUB_NAME = "AEP-DAYTON HUB"
HUB_NODE_ID = 34497127

SITES: dict[str, Site] = {
    "NWOH": Site(
        key="NWOH",
        display_name="NWOH (Northwest Ohio)",
        pnode_name="HAVILAND 34.5 KV NTHWSTWF",
        node_id=1318144721,
        fuel="wind",
        # Haviland, Paulding County, NW Ohio -- approximate; refine from STATIONS_GEO.
        lat=41.022, lon=-84.585, county="Paulding", state="OH",
    ),
}
