"""Constraints & Weather: West Texas transmission-constraint map.

Pulls binding-constraint and shift-factor data from the Yes Energy dataset in
Snowflake, computes how each active constraint affects our West Texas sites
(Holstein, McCrae/BKII, BKI, Stanton), and renders an interactive Leaflet map
with transmission-line geometry from HIFLD.

Exposed as a Flask blueprint (`constraints_bp`) mounted on the host app; see
web.py for the route surface.
"""
