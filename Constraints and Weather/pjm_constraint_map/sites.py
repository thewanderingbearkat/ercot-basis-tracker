"""Our PJM assets and how they map into Yes Energy / the BETA shift factors.

Each site settles at a PJM pnode (`PRICE_NODES.OBJECTID`, which is also the
`DART_PRICES.OBJECTID` and the `YES_ENERGY_SHIFT_FACTOR_BETA.PNODEID`). Basis is
node vs the site's reference hub/zone -- which differs by asset (NWOH and
Lordstown reference the AEP-Dayton hub; Lackawanna references the PSEG zone), so
the hub is stored per-site rather than globally.

Coordinates are approximate (plant/town-level) and flagged for refinement.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Site:
    key: str
    display_name: str
    pnode_name: str                # Yes Energy PRICE_NODES.PNODENAME
    node_id: int                   # OBJECTID == DART_PRICES.OBJECTID == BETA.PNODEID
    hub_name: str                  # basis reference (hub or zone)
    hub_node_id: int
    fuel: str                      # wind | gas | solar | ...
    lat: float
    lon: float
    coords_approx: bool = True
    county: str = ""
    state: str = ""


SITES: dict[str, Site] = {
    "NWOH": Site(
        key="NWOH", display_name="NWOH (Northwest Ohio)",
        pnode_name="HAVILAND 34.5 KV NTHWSTWF", node_id=1318144721,
        hub_name="AEP-DAYTON HUB", hub_node_id=34497127,
        fuel="wind", lat=41.022, lon=-84.585, county="Paulding", state="OH",
    ),
    "LORDSTOWN": Site(
        key="LORDSTOWN", display_name="Lordstown Energy Center",
        pnode_name="LRDTWNEC 19 KV CT11", node_id=1369012529,
        hub_name="AEP-DAYTON HUB", hub_node_id=34497127,
        fuel="gas", lat=41.155, lon=-80.855, county="Trumbull", state="OH",
    ),
    "LACKAWANNA": Site(
        key="LACKAWANNA", display_name="Lackawanna Energy Center",
        pnode_name="LACKAENG 24 KV CTG2", node_id=1369011076,
        hub_name="PSEG", hub_node_id=51301,
        fuel="gas", lat=41.466, lon=-75.561, county="Lackawanna", state="PA",
    ),
}
