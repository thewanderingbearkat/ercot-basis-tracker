"""Hail-monitor configuration: the site, XWeather endpoints, and threat thresholds.

Standalone from shadow_trader -- the only thing shared is the Holstein location,
duplicated here on purpose so this tab has no import dependency on the trading code.
"""

XWEATHER_BASE_URL = "https://data.api.xweather.com"

# The monitored site. Holstein Solar -- Wingate / Nolan County, TX.
SITE = {
    "key": "HOLSTEIN",
    "name": "Holstein Solar",
    "lat": 32.03,
    "lon": -100.45,
}

# How far out to pull storm cells from the site (radius for stormcells/closest).
CELL_SEARCH_RADIUS = "400mi"
CELL_SEARCH_LIMIT = 40

# Threat-assessment thresholds (tunable). Distances in miles, time in minutes.
THRESHOLDS = {
    # A cell counts as "hail-bearing" if either of these is met.
    "min_hail_prob": 25,        # ob.hail.prob (%)
    "min_hail_size_in": 0.25,   # ob.hail.maxSizeIN (pea-size and up)

    # Corridor: how close a moving cell's track must pass to the site to be "inbound".
    "inbound_corridor_mi": 20.0,
    # Only treat a cell as inbound if it would reach the site within this horizon.
    "inbound_eta_min": 120.0,
    # A hail-bearing cell sitting within this radius is at least a "watch", even if
    # its track isn't aimed straight at us (storms wobble).
    "watch_radius_mi": 60.0,
    # Cells slower than this are treated as ~stationary (ETA undefined, judged on distance).
    "min_movement_mph": 2.0,
}

# Status levels, worst-first. The UI maps these to colors.
STATUS_INBOUND = "INBOUND"   # hail-bearing cell on track to reach the site soon
STATUS_WATCH = "WATCH"       # hail-bearing cell nearby, or a point nowcast threat
STATUS_CLEAR = "CLEAR"       # nothing of concern

# Cache TTL for the live status fetch (seconds). Each status build costs 2 XWeather
# calls (storm cells + point threats), and the trial allows only 1000 calls/day, shared
# with everything else. At 300s, a continuously-open dashboard costs at most
# 2 calls / 5 min = 576/day -- the authoritative server-side throttle, independent of
# how many browsers are polling. Raise it further to spend even less of the daily cap.
STATUS_CACHE_TTL = 300
