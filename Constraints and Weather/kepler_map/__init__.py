"""Kepler/deck.gl experimental visualization tab.

A separate, self-contained blueprint that renders a deck.gl scene (transmission
arcs, facility hexagons, and a time-animated ERCOT congestion layer). Reuses
constraint_map's Snowflake helpers + the shared infra/transmission data; it does
not touch the existing constraint-map or PnL surfaces.
"""
