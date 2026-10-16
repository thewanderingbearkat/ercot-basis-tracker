"""Configuration: API endpoints, settlement points, asset definitions, backtest window."""
import os

# Tenaska / PTP Energy API
TENASKA_TOKEN_URL = "https://api.ptp.energy/v1/token"
TENASKA_MARKET_PRICES_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Market-Prices/data"
TENASKA_DART_DETAILS_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/DART-Energy-Details/data"
TENASKA_DART_FORECAST_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Optimization-Renewable-Forecast/data"

# Backtest window (override via CLI flags in scripts/)
BACKTEST_START_DATE = "2026-01-01"

# P&L tracking reset: the dashboard's default window and aggregates start here, so
# performance is measured only under the current strategy parameters (scarcity gate
# + PowerUp forecast source, both deployed 2026-09-01). Data before this date stays
# in the cache -- forecast-error stats (21d) and the wind share factor (30d) need
# that history -- and older dates remain viewable by moving
# the date pickers manually. Do NOT change BACKTEST_START_DATE for a P&L reset.
PNL_TRACKING_START = "2026-09-01"

# Trailing-average forecast lookback
TRAILING_AVG_DAYS = 7

# Persistence. On Render, SHADOW_DATA_DIR points at the persistent disk -- the free
# plan's filesystem is ephemeral, so every deploy used to wipe the cache, the bid ledger
# and the forward-only PowerUp/STWPF forecast history with it.
DATA_DIR = os.getenv("SHADOW_DATA_DIR") or os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
SHADOW_HISTORY_FILE = os.path.join(DATA_DIR, "shadow_history.json")

# Settlement points and price keys used by the DA/RT fetch
DART_NODES = ["NBOHR_RN", "HOLSTEIN_ALL", "HB_WEST"]
DART_PRICE_KEYS = ["DASPP", "RTSPP"]
HUB_NODE = "HB_WEST"

# ERCOT assets in scope for shadow DA strategy (NWOH lives in PJM, handled separately)
DART_ASSETS = ["BKI", "BKII", "HOLSTEIN"]

# Tenaska forecast feed labels each plant's forecast under a specific element name;
# map known element labels to our asset keys
DART_FORECAST_ELEMENT_MAP = {
    "Holstein Solar - DART Optimization": "HOLSTEIN",
    "226HC 8me LLC (Holstein Solar)": "HOLSTEIN",
    "Holstein Solar - PTP Optimization": "HOLSTEIN",
}

#
# PPA settlement (fixed-for-floating swap, applied per actual MWh generated):
#   ppa_fixed_payment    = actual_gen × ppa_price                         (we receive)
#   floating_leg_price   = basis_exposure × hub_rt + (1 - basis_exposure) × node_rt
#   ppa_floating_payment = actual_gen × floating_leg_price                (we pay)
#   net_ppa              = ppa_fixed_payment - ppa_floating_payment
#
#   basis_exposure = 1.0   -> floating leg priced at HUB only; we keep all (node - hub) basis
#   basis_exposure = 0.5   -> floating leg priced at (hub + node) / 2; we keep half the basis
#   basis_exposure = 0.0   -> floating leg priced at NODE; PPA is a flat $/MWh trade, no basis
#
# Plant coordinates are used only for weather lookups (Open-Meteo). Approximate site
# centroids are fine -- weather gradients at this scale are << forecast error. Adjust if
# you have exact substation coordinates.
ASSET_CONFIG = {
    "BKII": {
        "display_name": "McCrae (BKII)",
        "element_patterns": ["Bearkat Wind Energy II, LLC - Gen"],
        "settlement_point": "NBOHR_RN",
        "latitude": 31.86,   # Glasscock County, TX (Bearkat site, approx.)
        "longitude": -101.52,
        "ppa_percent": 100,
        "merchant_percent": 0,
        "ppa_price": 34.00,
        "ppa_basis_exposure": 0.5,
        "iso": "ERCOT",
        "nameplate_mw": 162.15,   # 47 x Vestas V126-3.45 (McCrae MOR)
        # Plant-level Vaisala PowerUp ensemble forecast, overlaid on the ERCOT
        # STWPF-share base (which backfills history and is the fallback whenever
        # PowerUp has no data -- missing creds, entitlement, or outage).
        "forecast_source": "powerup",
        "ercot_region": "WEST",
        "tech": "wind",
    },
    "BKI": {
        "display_name": "Bearkat I",
        "element_patterns": ["Bearkat Wind Energy I, LLC - Gen"],
        "settlement_point": "NBOHR_RN",
        "latitude": 31.86,   # Glasscock County, TX (Bearkat site, approx.)
        "longitude": -101.52,
        "ppa_percent": 100,
        "merchant_percent": 0,
        "ppa_price": 40.50,
        "ppa_basis_exposure": 1.0,
        "iso": "ERCOT",
        "nameplate_mw": 196.65,   # 57 x Vestas V126-3.45
        "forecast_source": "powerup",
        "ercot_region": "WEST",
        "tech": "wind",
    },
    "HOLSTEIN": {
        "display_name": "Holstein",
        "element_patterns": ["Holstein Solar - Generation"],
        "settlement_point": "HOLSTEIN_ALL",
        "latitude": 32.03,   # Wingate / Nolan County, TX (Holstein Solar, approx.)
        "longitude": -100.45,
        "ppa_percent": 87.5,
        "merchant_percent": 12.5,
        "ppa_price": 35.00,
        "ppa_basis_exposure": 1.0,
        "iso": "ERCOT",
        "nameplate_mw": 200.0,    # MWac
        "forecast_source": "tenaska",
        "tech": "solar",
    },
}

# Hub node for PPA floating-leg basis calculations (ERCOT)
PPA_HUB_NODE = "HB_WEST"

#
# Trader decision engine (shadow_trader/decision.py) thresholds.
#
# Per hour, shadow-DA uplift = bid_mw x (DA - RT), so the engine (a) stands down or
# half-sizes ahead of conditions that historically produced RT blowouts / forecast
# busts, and (b) haircuts bid size by trailing forecast-error sigma. There is no
# trailing-DART edge filter: participation is condition-dependent only.
# Thresholds are starting points chosen for West Texas sites; tune against backtests.
#
TRADER_CONFIG = {
    # Forecast-error tracking (bias/sigma by hour-of-day, trailing window)
    "error_lookback_days": 21,
    "min_error_samples": 5,
    "sigma_haircut_k": 0.5,          # bid = corrected_forecast - k*sigma
    "bias_correction_clamp": 0.5,    # cap |bias adjustment| at this fraction of forecast

    # Scarcity-condition stand-downs (any tech): extreme plant-site temperature is a
    # proxy for system stress, when an undelivered DA MW is bought back near the cap
    "extreme_heat_f": 100.0,
    "extreme_cold_f": 20.0,

    # Dusk system-wind screens (any tech), summer only: driver analysis over 94
    # summer evenings found spike evenings run ~8.5 GW ERCOT-wide wind at dusk vs
    # ~14 GW normal while ~25 GW of solar rolls off into a still-high load. LEVEL
    # triggers the stand-down; a big forecast SWING across the window is a
    # half-size caution (ramp magnitude alone didn't separate spike evenings, but
    # ramp-timing misses drive DART volatility). Winter dusk hours PAY (Jan-Jun
    # blanket test: -$107k) -- hence the seasonal switch.
    "dusk_hours": (19, 23),                   # hour-ending window screened
    "dusk_months": (6, 9),                    # active Jun-Sep inclusive
    "dusk_system_wind_standdown_mw": 8000.0,  # D-1 system STWPF at/below -> stand down
    "dusk_system_wind_elevated_mw": 11000.0,  # ...at/below -> half size
    "dusk_system_ramp_elevated_mw": 6000.0,   # max-min STWPF swing across window -> half size

    # Calm-wind screen (any tech): forecast site wind at the hour. Calm West Texas
    # hours carry negative mean DART and the fat RT-spike tail; windy hours earn
    # steadily. Swept 5-12 / 8-16 mph across all/am+eve/eve/dusk windows on Jan-Oct
    # 2026 (smooth surface); all hours at 12/16 had the best P&L per unit of tail.
    "calm_wind_hours": tuple(range(1, 25)),
    "calm_wind_standdown_mph": 12.0,  # forecast site wind at/below -> stand down
    "calm_wind_elevated_mph": 16.0,   # ...at/below -> half size

    # Wind-specific gates (80m wind speed / 10m gusts, mph)
    "wind_ramp_elevated_mph": 14.0,  # max hour-over-hour forecast swing -> half size all day
    "wind_cutout_mph": 50.0,         # approaching turbine cut-out (~56 mph at hub)
    "gust_standdown_mph": 58.0,
    # Removed after the Jan-Oct 2026 backtest (each cost P&L with no tail benefit):
    # solar cloud-volatility gate (-$64k), wind/cloud model-spread gates (-$6k/-$4k).
}
