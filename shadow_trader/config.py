"""Configuration: API endpoints, settlement points, asset definitions, backtest window."""
import os

# Tenaska / PTP Energy API
TENASKA_TOKEN_URL = "https://api.ptp.energy/v1/token"
TENASKA_MARKET_PRICES_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Market-Prices/data"
TENASKA_DART_DETAILS_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/DART-Energy-Details/data"
TENASKA_DART_FORECAST_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Optimization-Renewable-Forecast/data"

# Backtest window (override via CLI flags in scripts/)
BACKTEST_START_DATE = "2026-01-01"

# Trailing-average forecast lookback
TRAILING_AVG_DAYS = 7

# Persistence
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
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
        "nameplate_mw": None,
        # Tenaska doesn't publish wind forecasts; use ERCOT STWPF scaled by historical share
        "forecast_source": "ercot_regional",
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
        "nameplate_mw": None,
        "forecast_source": "ercot_regional",
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
        "nameplate_mw": None,
        "forecast_source": "tenaska",
        "tech": "solar",
    },
}

# Hub node for PPA floating-leg basis calculations (ERCOT)
PPA_HUB_NODE = "HB_WEST"

#
# Trader decision engine (shadow_trader/decision.py) thresholds.
#
# Per hour, shadow-DA uplift = bid_mw x (DA - RT), so the engine (a) requires a trailing
# DA premium before participating, (b) stands down ahead of weather that produces RT
# blowouts / forecast busts, and (c) haircuts bid size by trailing forecast-error sigma.
# Thresholds are starting points chosen for West Texas sites; tune against backtests.
#
TRADER_CONFIG = {
    # Edge filter: trailing DART (DA - RT) by node and hour-of-day
    "dart_lookback_days": 14,
    "min_dart_samples": 7,
    "dart_min_edge": 0.0,            # $/MWh trailing DART required to bid the hour

    # Forecast-error tracking (bias/sigma by hour-of-day, trailing window)
    "error_lookback_days": 21,
    "min_error_samples": 5,
    "sigma_haircut_k": 0.5,          # bid = corrected_forecast - k*sigma
    "bias_correction_clamp": 0.5,    # cap |bias adjustment| at this fraction of forecast

    # Scarcity-condition stand-downs (any tech): extreme plant-site temperature is a
    # proxy for system stress, when an undelivered DA MW is bought back near the cap
    "extreme_heat_f": 100.0,
    "extreme_cold_f": 20.0,

    # Wind-specific gates (80m wind speed / 10m gusts, mph)
    "wind_ramp_elevated_mph": 9.0,   # max hour-over-hour forecast swing -> half size
    "wind_ramp_standdown_mph": 14.0, # frontal passage -> stay out all day
    "wind_cutout_mph": 50.0,         # approaching turbine cut-out (~56 mph at hub)
    "gust_standdown_mph": 58.0,
    "wind_model_spread_mph": 7.0,    # cross-model disagreement -> half size

    # Solar-specific gates (cloud cover, %)
    "cloud_volatility_elevated_pct": 25.0,   # stdev across daylight hours -> half size
    "cloud_volatility_standdown_pct": 38.0,  # chaotic sky -> stay out of daylight hours
    "cloud_model_spread_pct": 30.0,          # cross-model disagreement -> half size
}
