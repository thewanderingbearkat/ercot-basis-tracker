from flask import Flask, jsonify, request, redirect, session
from flask_cors import CORS
from gridstatus import Ercot
import requests
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from functools import wraps
from collections import defaultdict
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# TENASKA API CONFIGURATION
# ============================================================================
TENASKA_API_AUTH = (
    os.getenv("TENASKA_API_USER", "tmartin@skyvest.com"),
    os.getenv("TENASKA_API_PASSWORD", "Rowhard2024!!!")
)
TENASKA_TOKEN_URL = "https://api.ptp.energy/v1/token"
TENASKA_ENERGY_IMBALANCE_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/EnergySettlement/data"

# Storage file for PnL data
PNL_HISTORY_FILE = 'pnl_history.json'

# Local Excel file for testing (Predictive Real-Time Energy Imbalance report)
ENERGY_IMBALANCE_EXCEL = r"C:\Users\TylerMartin\Downloads\Real-TimeEnergyImbalance_2026-01-26_2026-01-28.xlsx"

# API fetch configuration
TENASKA_AUTO_FETCH = True  # Set to True to fetch from API, False for Excel-only
TENASKA_FETCH_INTERVAL = 1800  # Fetch new data every 30 minutes (in seconds)
TENASKA_FETCH_DAYS_BACK = 30   # How many days of history to fetch (used as fallback)
TENASKA_FETCH_START_DATE = "2026-01-01"  # Start date for YTD historical data
TENASKA_MARKET_PRICES_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Market-Prices/data"
HUB_SETTLEMENT_POINT = "HB_WEST"  # Hub for basis calculation

# ============================================================================
# PHAROS AMS API CONFIGURATION (for NWOH - PJM asset)
# ============================================================================
PHAROS_API_TOKEN = os.getenv("PHAROS_API_TOKEN", "57f04df5c470974f7f50bz9JDaUsvLTi_qDR2iJso")
PHAROS_BASE_URL = "https://ams.pharos-ei.com/api"
PHAROS_ORGANIZATION_KEY = "skyv-nwo"  # Northwest Ohio Wind
PHAROS_AUTO_FETCH = True  # Set to True to fetch from Pharos API
PHAROS_FETCH_INTERVAL = 1800  # Fetch new data every 30 minutes (in seconds)
PHAROS_FETCH_START_DATE = "2026-02-05"  # Start date for NWOH data (Pharos access started Feb 5)

# Storage file for Pharos/NWOH data
PHAROS_HISTORY_FILE = 'pharos_nwoh_history.json'

# ============================================================================
# ASSET CONFIGURATION - PPA/Merchant Split & PnL Formulas
# ============================================================================
# Each asset has:
#   - element_patterns: list of strings to match in the 'Element' column
#   - settlement_point: the node where this asset settles
#   - ppa_percent: percentage under PPA (0-100)
#   - merchant_percent: percentage merchant (0-100), should sum to 100 with ppa_percent
#   - ppa_price: $/MWh PPA strike price
#   - ppa_settlement: how PPA is settled - "node", "hub", or "split" (50/50 node/hub)
#   - ppa_basis_exposure: percentage of basis risk borne (100 = full basis, 50 = half)
#
# PnL Calculation Logic:
#   - Merchant PnL = merchant_percent × Volume × RTSPP (or NodePrice)
#   - PPA PnL depends on settlement:
#       - "node": ppa_percent × Volume × (NodePrice - ppa_price) [full basis exposure]
#       - "hub": ppa_percent × Volume × (HubPrice - ppa_price) [no basis exposure]
#       - "split": ppa_percent × Volume × ((NodePrice + HubPrice)/2 - ppa_price) [50% basis]

ASSET_CONFIG = {
    "BKII": {
        "display_name": "McCrae (BKII)",
        # Match generation elements for BKII/McCrae
        # API returns: "Bearkat Wind Energy II, LLC - Gen" for generation data
        # Note: "McCrae Wind Energy II - Main" has netting data (buy+sell), not generation
        "element_patterns": [
            "Bearkat Wind Energy II, LLC - Gen",
        ],
        "settlement_point": "NBOHR_RN",
        "ppa_percent": 100,
        "merchant_percent": 0,
        "ppa_price": 34.00,
        "ppa_settlement": "split",  # 50% node, 50% hub
        "ppa_basis_exposure": 50,   # Bears 50% of basis risk
    },
    "BKI": {
        "display_name": "Bearkat I",
        # Match generation elements for BKI
        # API returns: "Bearkat Wind Energy I, LLC - Gen"
        "element_patterns": [
            "Bearkat Wind Energy I, LLC - Gen",
        ],
        "settlement_point": "NBOHR_RN",
        "ppa_percent": 0,
        "merchant_percent": 100,
        "ppa_price": 0,
        "ppa_settlement": "node",
        "ppa_basis_exposure": 0,
    },
    "HOLSTEIN": {
        "display_name": "Holstein",
        # Match generation elements for Holstein
        # API returns: "Holstein Solar - Generation"
        "element_patterns": [
            "Holstein Solar - Generation",
        ],
        "settlement_point": "HOLSTEIN_ALL",
        "ppa_percent": 87.5,
        "merchant_percent": 12.5,
        "ppa_price": 35.00,
        "ppa_settlement": "node",   # 100% settled at node = 100% basis exposure
        "ppa_basis_exposure": 100,
        "iso": "ERCOT",
    },
    "NWOH": {
        "display_name": "Northwest Ohio Wind",
        # Match Pharos API data for NWOH
        # Pharos returns: "HAVILAND 34.5 KV NTHWSTWF GEN"
        "element_patterns": [
            "HAVILAND 34.5 KV NTHWSTWF GEN",
            "Northwest Ohio Wind",
        ],
        "settlement_point": "HAVILAND34.5 KV NTHWSTWF",
        "pnode_id": "1318144721",
        "hub": "AEP-DAYTON HUB",
        "hub_id": "34497127",
        "zone": "AEP ZONE",
        "nameplate_mw": 105,  # 105 MW nameplate capacity
        # PPA Structure with General Motors:
        # - Fixed PPA Price: $33.31/MWh
        # - Settlement: Generation sold to GM at floating hub price
        # - GM pays: Generation × Hub Price (floating)
        # - NWOH receives: Generation × Fixed PPA Price
        # - Basis exposure: Hub - Node (NWOH keeps node revenue, pays hub to GM)
        "ppa_percent": 100,
        "merchant_percent": 0,
        "ppa_price": 33.31,  # Fixed PPA price $/MWh
        "ppa_settlement": "hub",  # Settles at hub (AEP-Dayton)
        "ppa_basis_exposure": 100,  # 100% exposed to hub-node basis
        "iso": "PJM",
        "data_source": "pharos",  # Indicates data comes from Pharos API
    },
}

# Number of worst basis intervals to track (for PPA exclusion clause)
# Per contract, exclusions can only be made from the prior day
# Formula: Gen × Basis = Volume × (Node Price - Hub Price)
WORST_BASIS_INTERVALS_TO_TRACK = 96  # Max 96 intervals in a day (15-min intervals)
WORST_BASIS_DISPLAY_COUNT = 10  # Show top 10 in dashboard

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-change-this')

# Configuration - ERCOT
NODE_1 = "NBOHR_RN"
NODE_2 = "HOLSTEIN_ALL"
HUB = "HB_WEST"

# Configuration - PJM
PJM_NODE = "HAVILAND34.5 KV NTHWSTWF"
PJM_HUB = "AEP-DAYTON HUB"
PJM_NODE_ID = 1318144721
PJM_HUB_ID = 34497127

# Thresholds
ALERT_THRESHOLD = 100
GREEN_THRESHOLD = -100
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD', 'SV2026!!!')

# Storage file for PJM historical data
PJM_HISTORY_FILE = 'pjm_history.json'

# Global state
data_lock = threading.Lock()
latest_data = {
    # ERCOT data
    "node1_price": None,
    "node2_price": None,
    "hub_price": None,
    "basis1": None,  # NODE_1 vs HUB
    "basis2": None,  # NODE_2 vs HUB
    "status1": "initializing",
    "status2": "initializing",
    "history": [],

    # PJM data
    "pjm_node_price": None,
    "pjm_hub_price": None,
    "pjm_basis": None,  # PJM_NODE vs PJM_HUB
    "pjm_status": "initializing",
    "pjm_history": [],

    # Metadata
    "last_update": None,
    "data_time": None,
}

# PnL data storage - now includes per-asset breakdown and worst basis tracking
pnl_data = {
    "energy_imbalance_history": [],  # Raw energy imbalance data from Tenaska
    "pnl_history": [],               # Calculated PnL points

    # Aggregated totals (all assets combined)
    "daily_pnl": {},                 # Aggregated by day
    "monthly_pnl": {},               # Aggregated by month
    "annual_pnl": {},                # Aggregated by year
    "total_pnl": 0,
    "total_volume": 0,

    # Per-asset breakdown
    "assets": {
        # Each asset will have: daily_pnl, monthly_pnl, annual_pnl, total_pnl, total_volume
    },

    # Worst basis intervals for PPA exclusion tracking
    # Sorted list of worst basis intervals (most negative first)
    "worst_basis_intervals": [],     # [{interval, basis, volume, pnl_impact, asset}]

    "last_tenaska_update": None,
    "record_count": 0,
}

last_basis_time = None
last_pjm_time = None

# Login decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated_function

# PJM History Storage Functions
def save_pjm_history(history):
    """Save PJM history to JSON file."""
    try:
        with open(PJM_HISTORY_FILE, 'w') as f:
            json.dump(history, f)
        logger.info(f"Saved {len(history)} PJM historical points to {PJM_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving PJM history: {e}")

def load_pjm_history():
    """Load PJM history from JSON file."""
    try:
        if os.path.exists(PJM_HISTORY_FILE):
            with open(PJM_HISTORY_FILE, 'r') as f:
                history = json.load(f)
            logger.info(f"Loaded {len(history)} PJM historical points from {PJM_HISTORY_FILE}")
            return history
        else:
            logger.info(f"No existing PJM history file found at {PJM_HISTORY_FILE}")
            return []
    except Exception as e:
        logger.error(f"Error loading PJM history: {e}")
        return []

# PJM API Helper Functions
def get_pjm_subscription_key():
    """Get the public PJM API subscription key."""
    try:
        response = requests.get("http://dataminer2.pjm.com/config/settings.json", timeout=10)
        settings = response.json()
        return settings.get("subscriptionKey")
    except Exception as e:
        logger.error(f"Error getting PJM subscription key: {e}")
        return None

def get_pjm_lmp_data(hours_back=4):
    """Fetch PJM LMP data from the unverified 5-minute feed."""
    try:
        # Get subscription key
        key = get_pjm_subscription_key()
        if not key:
            logger.error("Could not get PJM subscription key")
            return []

        headers = {"Ocp-Apim-Subscription-Key": key}

        # Use direct feed URL (avoids fetching 142KB feed list which can timeout on Render)
        feed_url = "https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps"

        # Fetch data from the last N hours
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        start_time = (now - timedelta(hours=hours_back)).strftime('%Y-%m-%dT%H:%M:%S')
        end_time = now.strftime('%Y-%m-%dT%H:%M:%S')

        params = {
            'datetime_beginning_utc': f'{start_time} to {end_time}',
            'startRow': 1,
            'rowCount': 10000
        }

        response = requests.get(feed_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"PJM API returned status {response.status_code}")
            return []

        data = response.json()
        all_items = data.get('items', [])

        if not all_items:
            logger.warning("No PJM data available")
            return []

        # Filter for our specific nodes
        node_items = [item for item in all_items if item.get('pnode_id') == PJM_NODE_ID]
        hub_items = [item for item in all_items if item.get('pnode_id') == PJM_HUB_ID]

        # Create merged data
        history = []
        for node_item in node_items:
            time_utc = node_item.get('datetime_beginning_utc')
            # Find matching hub item
            hub_item = next((h for h in hub_items if h.get('datetime_beginning_utc') == time_utc), None)

            if hub_item:
                node_lmp = node_item.get('total_lmp_rt', 0)
                hub_lmp = hub_item.get('total_lmp_rt', 0)
                basis = node_lmp - hub_lmp
                status = "safe" if basis > 0 else ("caution" if basis >= -30 else "alert")

                history.append({
                    'time': time_utc,
                    'node_price': round(float(node_lmp), 2),
                    'hub_price': round(float(hub_lmp), 2),
                    'basis': round(float(basis), 2),
                    'status': status
                })

        # Sort by time
        history.sort(key=lambda x: x['time'])

        logger.info(f"Fetched {len(history)} PJM historical data points")
        return history

    except Exception as e:
        logger.error(f"Error fetching PJM data: {e}")
        return []

# Cache for PJM hub prices (for Pharos basis calculation)
PJM_HUB_CACHE_FILE = 'pjm_hub_prices.json'
pjm_hub_price_cache = {}  # {timestamp_str: hub_lmp}

def load_pjm_hub_cache():
    """Load cached PJM hub prices from file."""
    global pjm_hub_price_cache
    try:
        if os.path.exists(PJM_HUB_CACHE_FILE):
            with open(PJM_HUB_CACHE_FILE, 'r') as f:
                pjm_hub_price_cache = json.load(f)
            logger.info(f"Loaded {len(pjm_hub_price_cache)} cached PJM hub prices")
    except Exception as e:
        logger.error(f"Error loading PJM hub cache: {e}")
        pjm_hub_price_cache = {}

def save_pjm_hub_cache():
    """Save PJM hub price cache to file."""
    try:
        with open(PJM_HUB_CACHE_FILE, 'w') as f:
            json.dump(pjm_hub_price_cache, f)
        logger.info(f"Saved {len(pjm_hub_price_cache)} PJM hub prices to cache")
    except Exception as e:
        logger.error(f"Error saving PJM hub cache: {e}")

def fetch_pjm_hub_prices_for_date(date_str):
    """
    Fetch PJM hub (AEP-DAYTON) 5-minute prices for a specific date.
    Returns dict of {timestamp_str: hub_lmp}.
    """
    try:
        key = get_pjm_subscription_key()
        if not key:
            logger.warning("[PJM HUB] Could not get PJM subscription key")
            return {}

        headers = {"Ocp-Apim-Subscription-Key": key}

        # Use direct feed URL (avoids fetching 142KB feed list which can timeout)
        feed_url = "https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps"

        start_time = f"{date_str}T00:00:00"
        end_time = f"{date_str}T23:59:59"

        params = {
            'datetime_beginning_utc': f'{start_time} to {end_time}',
            'pnode_id': PJM_HUB_ID,  # Only fetch hub prices (AEP-Dayton Hub)
            'startRow': 1,
            'rowCount': 500  # 288 intervals per day + buffer
        }

        response = requests.get(feed_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"[PJM HUB] API returned status {response.status_code} for {date_str}")
            return {}
        if not response.text.strip():
            logger.error(f"[PJM HUB] API returned empty response for {date_str}")
            return {}

        data = response.json()
        items = data.get('items', [])

        logger.info(f"[PJM HUB] Got {len(items)} items from PJM API for {date_str}")

        # Build lookup dict
        hub_prices = {}
        for item in items:
            time_utc = item.get('datetime_beginning_utc', '')
            hub_lmp = item.get('total_lmp_rt', 0)
            if time_utc:
                hub_prices[time_utc] = float(hub_lmp)

        logger.info(f"[PJM HUB] Fetched {len(hub_prices)} hub prices for {date_str}")
        return hub_prices

    except Exception as e:
        logger.error(f"[PJM HUB] Error fetching hub prices for {date_str}: {e}")
        return {}

def get_hub_price_for_timestamp(timestamp_str):
    """
    Get hub price for a Pharos timestamp.
    Converts Pharos timestamp (EST) to UTC and looks up in cache.
    Returns hub LMP or None if not found.
    """
    try:
        # Pharos timestamps are like "2026-02-10T00:00:00.000-05:00"
        # PJM timestamps are like "2026-02-10T05:00:00"
        if not timestamp_str:
            return None

        # Parse Pharos timestamp
        dt = datetime.fromisoformat(timestamp_str.replace(".000", ""))
        # Convert to UTC
        dt_utc = dt.astimezone(ZoneInfo("UTC"))
        # Format for PJM lookup (they store as UTC without timezone)
        pjm_time_key = dt_utc.strftime("%Y-%m-%dT%H:%M:%S")

        return pjm_hub_price_cache.get(pjm_time_key)

    except Exception as e:
        return None

def ensure_hub_prices_cached(start_date, end_date):
    """
    Ensure we have hub prices cached for the given date range.
    Fetches missing dates from PJM API.
    """
    global pjm_hub_price_cache

    # Load existing cache
    if not pjm_hub_price_cache:
        load_pjm_hub_cache()

    # Get list of dates we need
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    dates_to_fetch = []
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        # Check if we have any prices for this date
        date_prefix = f"{date_str}T"
        has_data = any(k.startswith(date_prefix) for k in pjm_hub_price_cache.keys())
        if not has_data:
            dates_to_fetch.append(date_str)
        current += timedelta(days=1)

    if dates_to_fetch:
        logger.info(f"Fetching hub prices for {len(dates_to_fetch)} missing dates...")
        for date_str in dates_to_fetch:
            prices = fetch_pjm_hub_prices_for_date(date_str)
            pjm_hub_price_cache.update(prices)

        # Save updated cache
        save_pjm_hub_cache()

# ============================================================================
# TENASKA API FUNCTIONS
# ============================================================================
def get_tenaska_token():
    """Get authentication token from Tenaska API."""
    try:
        response = requests.get(TENASKA_TOKEN_URL, auth=TENASKA_API_AUTH, timeout=10)
        if response.status_code == 200:
            token = response.json().get('data')
            logger.info("Successfully obtained Tenaska API token")
            return token
        else:
            logger.error(f"Failed to get Tenaska token: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting Tenaska token: {e}")
        return None

def fetch_energy_imbalance_data(start_date=None, days_back=30):
    """
    Fetch energy imbalance data from Tenaska API.
    Returns data in the same format as load_energy_imbalance_from_excel() for compatibility.

    Args:
        start_date: Start date string (YYYY-MM-DD) or None to use days_back
        days_back: Number of days back if start_date not specified

    Expected API fields:
    - Real_Time_Energy_Imbalance_Volume: MWh volume
    - RTEIAMT: Real-Time Energy Imbalance Amount ($)
    - Energy_Imbalance_Average_Price: $/MWh price
    - RTSPP: Real-Time Settlement Point Price
    """
    try:
        token = get_tenaska_token()
        if not token:
            logger.error("Could not obtain Tenaska API token")
            return []

        headers = {"Authorization": f"Bearer {token}"}

        # Calculate date range
        end_date = datetime.now(ZoneInfo("UTC"))
        if start_date:
            begin_str = f"{start_date}T00:00:00Z"
        else:
            begin_dt = end_date - timedelta(days=days_back)
            begin_str = begin_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "begin": begin_str,
            "end": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        logger.info(f"Fetching Tenaska energy imbalance data from {params['begin']} to {params['end']}")
        response = requests.get(TENASKA_ENERGY_IMBALANCE_URL, headers=headers, params=params, timeout=120)

        if response.status_code != 200:
            logger.error(f"Tenaska API returned status {response.status_code}: {response.text[:200]}")
            return []

        data = response.json()
        cst_tz = ZoneInfo("America/Chicago")

        # Debug: save raw API response to file
        try:
            with open("C:/Users/TylerMartin/tenaska_api_debug.json", "w") as f:
                import json as json_module
                json_module.dump(data, f, indent=2)
            logger.info("Saved raw Tenaska API response to tenaska_api_debug.json")

            # Log structure of first few items
            items = data.get("data", [])
            logger.info(f"API returned {len(items)} top-level data items")
            for i, item in enumerate(items[:3]):
                logger.info(f"Item {i} keys: {list(item.keys())}")
                logger.info(f"  parent={item.get('parent')}, element={item.get('element')}, name={item.get('name')}")
        except Exception as e:
            logger.warning(f"Could not save debug API response: {e}")

        # Fields we want to extract
        target_fields = {
            "Real_Time_Energy_Imbalance_Volume": "volume",
            "RTEIAMT": "amount",
            "Energy_Imbalance_Average_Price": "price",
            "RTSPP": "rtspp",
        }

        # Build a dictionary keyed by (element, interval, settlement_point) to combine all fields
        interval_data = defaultdict(lambda: {
            "element": "",
            "settlement_point": "",
            "interval": "",
            "volume_mwh": 0,
            "pnl": 0,
            "price": 0,
            "rtspp": 0,
        })

        for item in data.get("data", []):
            # Use 'element' for asset name (e.g., "Bearkat Wind Energy II, LLC - Gen")
            # Fall back to 'parent' only if element is not available
            element_name = item.get("element") or item.get("parent", "Unknown")

            for data_point in item.get("dataPoints", []):
                key_name = data_point.get("keyName", "")

                # Skip fields we don't need
                if key_name not in target_fields:
                    continue

                field_name = target_fields[key_name]

                for value_entry in data_point.get("values", []):
                    interval_start_utc = value_entry.get("intervalStartUtc")

                    for nested_data in value_entry.get("data", []):
                        value = nested_data.get("value", 0)
                        settlement_point = nested_data.get("coords", {}).get("settlementPoint", "")

                        # Convert to CST for interval key
                        try:
                            interval_dt = datetime.strptime(interval_start_utc, "%Y-%m-%dT%H:%M:%SZ")
                            interval_dt = interval_dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(cst_tz)
                            interval_str = interval_dt.isoformat()
                        except:
                            continue

                        # Create unique key for this interval
                        key = (element_name, interval_str, settlement_point)
                        interval_data[key]["element"] = element_name
                        interval_data[key]["settlement_point"] = settlement_point
                        interval_data[key]["interval"] = interval_str

                        # Parse value safely
                        try:
                            float_value = float(value) if value else 0
                        except (ValueError, TypeError):
                            float_value = 0

                        # Assign to appropriate field
                        if field_name == "volume":
                            interval_data[key]["volume_mwh"] = float_value
                        elif field_name == "amount":
                            interval_data[key]["pnl"] = float_value
                        elif field_name == "price":
                            interval_data[key]["price"] = float_value
                        elif field_name == "rtspp":
                            interval_data[key]["rtspp"] = float_value

        # Convert to list format compatible with Excel loader
        records = list(interval_data.values())

        # Filter out records with no volume (empty intervals)
        records = [r for r in records if r["volume_mwh"] != 0 or r["pnl"] != 0]

        # Derive RTSPP from reported amount when not provided by API
        # RTEIAMT (pnl field) = -Volume × RTSPP (has opposite sign)
        # So RTSPP = -pnl / volume
        rtspp_derived_count = 0
        for r in records:
            if r["rtspp"] == 0 and r["pnl"] != 0 and r["volume_mwh"] != 0:
                r["rtspp"] = -r["pnl"] / r["volume_mwh"]
                rtspp_derived_count += 1
        if rtspp_derived_count > 0:
            logger.info(f"Derived RTSPP from RTEIAMT for {rtspp_derived_count} records")

        logger.info(f"Fetched {len(records)} energy imbalance records from Tenaska API")

        # Log sample of data for debugging
        if records:
            elements = set(r["element"] for r in records[:100])
            logger.info(f"Elements found: {elements}")
            # Log sample record to verify RTSPP values
            sample = records[0]
            logger.info(f"Sample record - Element: {sample['element']}, Volume: {sample['volume_mwh']}, RTSPP: {sample['rtspp']}, PnL: {sample['pnl']}")

        return records

    except requests.Timeout:
        logger.error("Tenaska API request timed out")
        return []
    except Exception as e:
        logger.error(f"Error fetching Tenaska energy imbalance data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def fetch_hub_prices(start_date=None, end_date=None):
    """
    Fetch HB_WEST hub prices from Tenaska Market-Prices endpoint.
    Returns a dictionary keyed by interval timestamp (CST) with hub RTSPP price.

    Args:
        start_date: Start date string (YYYY-MM-DD) or None for TENASKA_FETCH_START_DATE
        end_date: End date string (YYYY-MM-DD) or None for today
    """
    try:
        token = get_tenaska_token()
        if not token:
            logger.error("Could not obtain Tenaska API token for hub prices")
            return {}

        headers = {"Authorization": f"Bearer {token}"}

        # Calculate date range
        if start_date is None:
            start_date = TENASKA_FETCH_START_DATE
        if end_date is None:
            end_date = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")

        cst_tz = ZoneInfo("America/Chicago")
        hub_prices = {}

        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Fetch day by day to avoid API limit (1,000,000 records max)
        logger.info(f"Fetching hub prices ({HUB_SETTLEMENT_POINT}) from {start_date} to {end_date} (day by day)")
        current_dt = start_dt
        days_fetched = 0

        while current_dt <= end_dt:
            day_str = current_dt.strftime("%Y-%m-%d")
            params = {
                "begin": f"{day_str}T00:00:00Z",
                "end": f"{day_str}T23:59:59Z"
            }

            try:
                response = requests.get(TENASKA_MARKET_PRICES_URL, headers=headers, params=params, timeout=60)

                if response.status_code == 200:
                    data = response.json()

                    for item in data.get("data", []):
                        if item.get("element") != HUB_SETTLEMENT_POINT:
                            continue

                        for data_point in item.get("dataPoints", []):
                            if data_point.get("keyName") != "RTSPP":
                                continue

                            for value_entry in data_point.get("values", []):
                                interval_start_utc = value_entry.get("intervalStartUtc")

                                for nested_data in value_entry.get("data", []):
                                    price = nested_data.get("value", 0)

                                    try:
                                        interval_dt = datetime.strptime(interval_start_utc, "%Y-%m-%dT%H:%M:%SZ")
                                        interval_dt = interval_dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(cst_tz)
                                        interval_str = interval_dt.isoformat()

                                        hub_prices[interval_str] = float(price) if price else 0
                                    except:
                                        continue
                    days_fetched += 1
                else:
                    logger.warning(f"Hub prices API returned {response.status_code} for {day_str}")

            except requests.Timeout:
                logger.warning(f"Hub prices request timed out for {day_str}")
            except Exception as e:
                logger.warning(f"Error fetching hub prices for {day_str}: {e}")

            current_dt += timedelta(days=1)

        logger.info(f"Fetched {len(hub_prices)} hub price intervals from {days_fetched} days")
        return hub_prices

    except requests.Timeout:
        logger.error("Market-Prices API request timed out")
        return {}
    except Exception as e:
        logger.error(f"Error fetching hub prices: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

def calculate_pnl(energy_imbalance_records, lmp_history):
    """
    Calculate PnL by matching energy imbalance volumes with LMP prices.
    PnL = Volume × (Node Price - Hub Price) = Volume × Basis

    For energy imbalance, if you're selling at node and buying at hub:
    - Positive basis = profit (selling high, buying low)
    - Negative basis = loss (selling low, buying high)
    """
    pnl_records = []

    # Group energy imbalance by interval and settlement point
    imbalance_by_interval = defaultdict(lambda: {"volume": 0, "amount": 0, "settlement_point": ""})

    for record in energy_imbalance_records:
        interval = record["interval_start"]
        sp = record["settlement_point"]
        key = f"{interval}_{sp}"

        if record["key_name"] == "Real_Time_Energy_Imbalance_Volume":
            imbalance_by_interval[key]["volume"] = record["value"]
            imbalance_by_interval[key]["settlement_point"] = sp
            imbalance_by_interval[key]["interval"] = interval
        elif record["key_name"] == "RTEIAMT":
            imbalance_by_interval[key]["amount"] = record["value"]

    # Create a lookup for LMP basis by time
    basis_lookup = {}
    for point in lmp_history:
        time_str = str(point.get("time", ""))
        basis_lookup[time_str] = {
            "basis1": point.get("basis1", 0),
            "basis2": point.get("basis2", 0),
            "node1_price": point.get("node1_price", 0),
            "node2_price": point.get("node2_price", 0),
            "hub_price": point.get("hub_price", 0)
        }

    # Calculate PnL for each interval
    for key, data in imbalance_by_interval.items():
        if data["volume"] == 0:
            continue

        interval = data.get("interval", "")
        volume = data["volume"]  # MWh
        settlement_point = data["settlement_point"]
        reported_amount = data["amount"]  # This is the actual settlement amount from Tenaska

        # Determine which basis to use based on settlement point
        if settlement_point == NODE_1 or "NBOHR" in settlement_point:
            basis_field = "basis1"
        elif settlement_point == NODE_2 or "HOLSTEIN" in settlement_point:
            basis_field = "basis2"
        else:
            basis_field = "basis1"  # Default

        # Try to find matching LMP data
        basis = 0
        for time_key, lmp_data in basis_lookup.items():
            if interval[:16] in time_key[:16]:  # Match by minute
                basis = lmp_data.get(basis_field, 0)
                break

        # Calculate PnL: Volume × Basis
        # Use reported amount if available, otherwise calculate
        if reported_amount != 0:
            calculated_pnl = reported_amount
        else:
            calculated_pnl = volume * basis

        pnl_records.append({
            "interval": interval,
            "settlement_point": settlement_point,
            "volume_mwh": round(volume, 4),
            "basis": round(basis, 2),
            "pnl": round(calculated_pnl, 2),
            "reported_amount": round(reported_amount, 2)
        })

    return pnl_records

def aggregate_pnl(pnl_records):
    """Aggregate PnL by daily, monthly, and annual periods."""
    daily = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0})
    monthly = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0})
    annual = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0})

    for record in pnl_records:
        try:
            interval = record["interval"]
            # Parse the interval timestamp
            if "T" in interval:
                dt = datetime.fromisoformat(interval.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(interval[:19], "%Y-%m-%d %H:%M:%S")

            day_key = dt.strftime("%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")
            year_key = dt.strftime("%Y")

            pnl = record["pnl"]
            volume = record["volume_mwh"]

            daily[day_key]["pnl"] += pnl
            daily[day_key]["volume"] += volume
            daily[day_key]["count"] += 1

            monthly[month_key]["pnl"] += pnl
            monthly[month_key]["volume"] += volume
            monthly[month_key]["count"] += 1

            annual[year_key]["pnl"] += pnl
            annual[year_key]["volume"] += volume
            annual[year_key]["count"] += 1

        except Exception as e:
            logger.error(f"Error aggregating PnL record: {e}")
            continue

    # Round the aggregated values
    for d in daily.values():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)
    for d in monthly.values():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)
    for d in annual.values():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)

    return dict(daily), dict(monthly), dict(annual)

def save_pnl_data(data):
    """Save PnL data to JSON file."""
    try:
        with open(PNL_HISTORY_FILE, 'w') as f:
            json.dump(data, f, default=str)
        logger.info(f"Saved PnL data to {PNL_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving PnL data: {e}")

def load_pnl_data():
    """Load PnL data from JSON file."""
    try:
        if os.path.exists(PNL_HISTORY_FILE):
            with open(PNL_HISTORY_FILE, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded PnL data from {PNL_HISTORY_FILE}")
            return data
        return None
    except Exception as e:
        logger.error(f"Error loading PnL data: {e}")
        return None

def load_energy_imbalance_from_excel(file_path=None):
    """
    Load energy imbalance data from Excel file.
    Supports both report formats:

    1. Predictive Real-Time Energy Imbalance report:
       - Energy Imbalance Volume, Energy Imbalance Amount, Energy Imbalance Average Price, RTSPP

    2. Real-Time Energy Imbalance report:
       - Real-Time Energy Imbalance Volume (MWh), Real-Time Energy Imbalance Amount ($)
       - Real-Time Settlement Point Price ($/MWh), Weighted Average Price ($/MWh)

    NOTE: The "Amount" in both reports has OPPOSITE sign convention.
          Report shows negative amounts, but Volume × RTSPP = positive revenue.
          We calculate PnL as Volume × RTSPP (positive for generation revenue).
    """
    try:
        import pandas as pd

        if file_path is None:
            file_path = ENERGY_IMBALANCE_EXCEL

        if not os.path.exists(file_path):
            logger.warning(f"Energy imbalance Excel file not found: {file_path}")
            return []

        logger.info(f"Loading energy imbalance data from Excel: {file_path}")
        df = pd.read_excel(file_path)

        logger.info(f"Excel columns found: {df.columns.tolist()}")

        # Combine Flowday and Interval to create timestamp
        # Handle '24:00' interval specially - it should stay on the same flowday
        df['Flowday (Central)'] = pd.to_datetime(df['Flowday (Central)'])

        def parse_interval(row):
            interval_str = str(row['Interval'])
            flowday = row['Flowday (Central)']
            # Handle 24:00 as 23:59:59 to keep it on the same day
            if interval_str == '24:00':
                return flowday + pd.Timedelta(hours=23, minutes=59, seconds=59)
            else:
                return flowday + pd.to_timedelta(interval_str + ':00')

        df['DateTime'] = df.apply(parse_interval, axis=1)

        # Detect column format and normalize column names
        # Format 1: Predictive report
        if 'Energy Imbalance Volume' in df.columns:
            vol_col = 'Energy Imbalance Volume'
            amt_col = 'Energy Imbalance Amount'
            price_col = 'Energy Imbalance Average Price'
            rtspp_col = 'RTSPP'
        # Format 2: Real-Time report
        elif 'Real-Time Energy Imbalance Volume (MWh)' in df.columns:
            vol_col = 'Real-Time Energy Imbalance Volume (MWh)'
            amt_col = 'Real-Time Energy Imbalance Amount ($)'
            price_col = 'Weighted Average Price ($/MWh)'
            rtspp_col = 'Real-Time Settlement Point Price ($/MWh)'
        else:
            logger.error(f"Unknown Excel format. Columns: {df.columns.tolist()}")
            return []

        # Convert to records for PnL calculation
        records = []
        for _, row in df.iterrows():
            volume = float(row[vol_col]) if pd.notna(row[vol_col]) else 0
            rtspp = float(row[rtspp_col]) if rtspp_col in df.columns and pd.notna(row[rtspp_col]) else 0
            price = float(row[price_col]) if price_col in df.columns and pd.notna(row[price_col]) else rtspp

            # Calculate PnL as Volume × RTSPP (positive = revenue for generation)
            # Don't use the report's "Amount" column as it has opposite sign convention
            calculated_pnl = volume * rtspp

            records.append({
                "interval": row['DateTime'].isoformat(),
                "element": row['Element'],
                "settlement_point": row['Settlement Point'],
                "volume_mwh": volume,
                "pnl": calculated_pnl,  # Use calculated value, not report's Amount
                "price": price,
                "rtspp": rtspp,
            })

        logger.info(f"Loaded {len(records)} energy imbalance records from Excel")

        # Log summary by element
        from collections import defaultdict
        element_summary = defaultdict(lambda: {"volume": 0, "pnl": 0, "count": 0})
        for r in records:
            element_summary[r["element"]]["volume"] += r["volume_mwh"]
            element_summary[r["element"]]["pnl"] += r["pnl"]
            element_summary[r["element"]]["count"] += 1
        for elem, data in element_summary.items():
            logger.info(f"  {elem}: {data['count']} records, {data['volume']:.2f} MWh, ${data['pnl']:.2f}")

        return records

    except ImportError:
        logger.error("pandas is required to load Excel files. Install with: pip install pandas openpyxl")
        return []
    except Exception as e:
        logger.error(f"Error loading energy imbalance from Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def identify_asset(element_name):
    """
    Identify which asset an element belongs to based on ASSET_CONFIG patterns.
    Returns the asset key (e.g., 'BKII', 'BKI', 'HOLSTEIN') or 'UNKNOWN'.

    The API returns data at multiple hierarchy levels (Main, Hedge/Gen, Gen).
    We ONLY use "- Gen" elements to avoid double-counting the same generation data.
    - BKI: "Bearkat Wind Energy I, LLC - Gen"
    - BKII: "Bearkat Wind Energy II, LLC - Gen"
    - Holstein: "Holstein Solar - Generation"
    """
    element_lower = element_name.lower() if element_name else ""

    # Only process "- Gen" or "- Generation" elements to avoid double-counting
    # The API reports same data at Main, Hedge/Gen, and Gen levels - we only want Gen
    is_generation = "- generation" in element_lower
    is_gen = element_lower.endswith("- gen")  # Exact match to avoid matching "hedge/gen"

    if not is_generation and not is_gen:
        return "UNKNOWN"

    for asset_key, config in ASSET_CONFIG.items():
        for pattern in config.get("element_patterns", []):
            if pattern.lower() in element_lower:
                return asset_key

    return "UNKNOWN"

def calculate_asset_pnl(record, asset_key, hub_price=None):
    """
    Calculate PnL for a record based on asset-specific configuration.

    PnL Formulas (per user specification):
    - BKI (100% Merchant): Volume × RTSPP
    - McCrae/BKII (100% PPA at $34, 50% basis): Volume × $34 + 50% × Volume × basis
    - Holstein (87.5% PPA at $35 + 100% basis, 12.5% Merchant):
        PPA: 87.5% × Volume × ($35 + basis)
        Merchant: 12.5% × Volume × RTSPP

    Where basis = Node Price (RTSPP) - Hub Price
    """
    config = ASSET_CONFIG.get(asset_key)
    if not config:
        # Unknown asset - just use Volume × RTSPP
        volume = record.get("volume_mwh", 0)
        rtspp = record.get("rtspp", 0)
        return volume * rtspp, 0  # (pnl, basis)

    volume = record.get("volume_mwh", 0)
    node_price = record.get("rtspp", 0)  # RTSPP is the node price

    # If no hub price provided, assume 0 basis (hub = node)
    # In production, we'd match with actual hub LMP data from ERCOT
    if hub_price is None:
        hub_price = node_price

    # Calculate basis (node - hub)
    basis = node_price - hub_price

    ppa_pct = config["ppa_percent"] / 100
    merchant_pct = config["merchant_percent"] / 100
    ppa_price = config["ppa_price"]
    basis_exposure = config.get("ppa_basis_exposure", 100) / 100  # % of basis risk

    # Calculate PnL components
    merchant_pnl = 0
    ppa_pnl = 0

    # Merchant portion: Revenue = Volume × RTSPP
    if merchant_pct > 0:
        merchant_pnl = merchant_pct * volume * node_price

    # PPA portion: Revenue = Volume × PPA_Price + basis_exposure × Volume × basis
    # This gives: PPA revenue + gain/loss from basis
    if ppa_pct > 0:
        ppa_revenue = ppa_pct * volume * ppa_price
        basis_adjustment = ppa_pct * volume * basis * basis_exposure
        ppa_pnl = ppa_revenue + basis_adjustment

    total_pnl = merchant_pnl + ppa_pnl

    return total_pnl, basis

def aggregate_excel_pnl(records, hub_prices=None):
    """
    Aggregate PnL data from Excel/API records by daily, monthly, and annual periods.
    Supports per-asset breakdown and worst basis interval tracking.

    Args:
        records: List of energy imbalance records
        hub_prices: Dictionary of hub prices keyed by interval timestamp (CST ISO format)
                   e.g., {"2026-01-26T00:00:00-06:00": 100.50, ...}
    """
    # Hub price lookup - directly use the provided dictionary
    hub_price_lookup = hub_prices or {}
    if hub_price_lookup:
        logger.info(f"Using hub price lookup with {len(hub_price_lookup)} entries")
        # Log sample hub price keys for debugging
        sample_keys = list(hub_price_lookup.keys())[:3]
        for key in sample_keys:
            logger.info(f"Hub price sample: {key} = ${hub_price_lookup[key]:.2f}")

    # Total aggregations
    daily = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0, "records": [], "volume_basis_product": 0})
    monthly = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0, "volume_basis_product": 0})
    annual = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0, "volume_basis_product": 0})

    # Per-asset aggregations
    asset_daily = defaultdict(lambda: defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0}))
    asset_monthly = defaultdict(lambda: defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0}))
    asset_annual = defaultdict(lambda: defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0}))
    asset_totals = defaultdict(lambda: {"pnl": 0, "volume": 0, "count": 0})

    # Realized price tracking at multiple time levels
    # For BKI (100% merchant): realized = total_revenue / total_volume
    # For BKII (100% PPA, 50% basis): realized = PPA_price + 50% × GWA_basis
    # For Holstein: PPA = PPA_price + 100% × GWA_basis, Merchant = total_merchant_revenue / total_merchant_volume
    def make_realized_tracker():
        return {
            "total_revenue": 0,           # Sum of volume × RTSPP (for merchant pricing)
            "total_volume": 0,            # Sum of volume
            "volume_basis_product": 0,    # Sum of volume × basis (for GWA basis)
            "merchant_revenue": 0,        # For Holstein merchant portion
            "merchant_volume": 0,         # For Holstein merchant portion
        }

    # YTD totals
    asset_realized = defaultdict(make_realized_tracker)
    # Daily tracking: asset_realized_daily[asset_key][day_key]
    asset_realized_daily = defaultdict(lambda: defaultdict(make_realized_tracker))
    # Monthly tracking: asset_realized_monthly[asset_key][month_key]
    asset_realized_monthly = defaultdict(lambda: defaultdict(make_realized_tracker))
    # Annual tracking: asset_realized_annual[asset_key][year_key]
    asset_realized_annual = defaultdict(lambda: defaultdict(make_realized_tracker))

    # Worst basis intervals tracking - ONLY for prior day (yesterday) per contractual requirements
    all_intervals = []
    hub_matches = 0
    hub_misses = 0

    # Calculate yesterday's date (in CST) for PPA exclusion interval filtering
    cst_tz = ZoneInfo("America/Chicago")
    now_cst = datetime.now(cst_tz)
    yesterday_cst = (now_cst - timedelta(days=1)).strftime("%Y-%m-%d")

    for record in records:
        try:
            interval = record["interval"]
            # Parse the interval timestamp
            if "T" in interval:
                dt = datetime.fromisoformat(interval.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(interval[:19], "%Y-%m-%d %H:%M:%S")

            day_key = dt.strftime("%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")
            year_key = dt.strftime("%Y")

            # Identify asset based on element name pattern
            element = record.get("element", "")
            asset_key = identify_asset(element)

            # Look up hub price from Tenaska hub prices
            hub_price = None
            if hub_price_lookup:
                # Hub prices are keyed by CST ISO format (e.g., "2026-01-26T00:00:00-06:00")
                # Try the exact interval timestamp first, then truncated versions
                hub_price = hub_price_lookup.get(interval)
                if not hub_price:
                    # Try without timezone offset
                    time_key_no_tz = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    for key in hub_price_lookup:
                        if key.startswith(time_key_no_tz[:16]):  # Match to minute
                            hub_price = hub_price_lookup[key]
                            break
                if hub_price:
                    hub_matches += 1
                else:
                    hub_misses += 1

            # Debug logging for Holstein intervals on current day
            today_cst = now_cst.strftime("%Y-%m-%d")
            if asset_key == "HOLSTEIN" and day_key == today_cst and hub_matches + hub_misses <= 10:
                node_price_debug = record.get("rtspp", 0)
                logger.info(f"HOLSTEIN DEBUG [{day_key}]: interval={interval}, node=${node_price_debug:.2f}, hub=${hub_price:.2f if hub_price else 'None'}, basis=${(node_price_debug - hub_price) if hub_price else 'N/A':.2f if hub_price else 'N/A'}")

            # Calculate PnL (using asset-specific formula)
            pnl, basis = calculate_asset_pnl(record, asset_key, hub_price=hub_price)
            volume = record.get("volume_mwh", 0)
            node_price = record.get("rtspp", 0)

            # Track interval for worst basis calculation (HOLSTEIN ONLY, PRIOR DAY ONLY)
            # Holstein is the only site with PPA exclusion clause
            # Per contractual requirements, can only exclude intervals from the prior day
            # Formula: Basis Revenue = Gen (Volume) × Basis (Node Price - Hub Price)
            # Most negative = worst intervals (candidates for exclusion)
            if asset_key == "HOLSTEIN" and volume != 0 and day_key == yesterday_cst:
                # Basis revenue = Volume × Basis (where Basis = Node - Hub)
                basis_revenue = volume * basis

                all_intervals.append({
                    "interval": interval,
                    "datetime": dt.isoformat(),
                    "asset": asset_key,
                    "element": element,
                    "basis": round(basis, 2),
                    "volume": round(volume, 4),
                    "basis_pnl_impact": round(basis_revenue, 2),  # Gen × Basis
                    "node_price": round(node_price, 2),
                    "hub_price": round(hub_price, 2) if hub_price else None,
                })

            # Total aggregations - only include known assets (exclude UNKNOWN to avoid double-counting)
            if asset_key != "UNKNOWN":
                daily[day_key]["pnl"] += pnl
                daily[day_key]["volume"] += volume
                daily[day_key]["count"] += 1
                if volume != 0:
                    daily[day_key]["volume_basis_product"] += volume * basis
                daily[day_key]["records"].append({
                    "time": dt.strftime("%H:%M"),
                    "pnl": round(pnl, 2),
                    "volume": round(volume, 4),
                    "asset": asset_key,
                    "settlement_point": record.get("settlement_point", ""),
                    "price": node_price
                })

                monthly[month_key]["pnl"] += pnl
                monthly[month_key]["volume"] += volume
                monthly[month_key]["count"] += 1
                if volume != 0:
                    monthly[month_key]["volume_basis_product"] += volume * basis

                annual[year_key]["pnl"] += pnl
                annual[year_key]["volume"] += volume
                annual[year_key]["count"] += 1
                if volume != 0:
                    annual[year_key]["volume_basis_product"] += volume * basis

            # Per-asset aggregations
            asset_daily[asset_key][day_key]["pnl"] += pnl
            asset_daily[asset_key][day_key]["volume"] += volume
            asset_daily[asset_key][day_key]["count"] += 1

            asset_monthly[asset_key][month_key]["pnl"] += pnl
            asset_monthly[asset_key][month_key]["volume"] += volume
            asset_monthly[asset_key][month_key]["count"] += 1

            asset_annual[asset_key][year_key]["pnl"] += pnl
            asset_annual[asset_key][year_key]["volume"] += volume
            asset_annual[asset_key][year_key]["count"] += 1

            asset_totals[asset_key]["pnl"] += pnl
            asset_totals[asset_key]["volume"] += volume
            asset_totals[asset_key]["count"] += 1

            # Track data for realized price calculations at all time levels
            if volume != 0:
                # YTD totals
                asset_realized[asset_key]["total_volume"] += volume
                asset_realized[asset_key]["total_revenue"] += volume * node_price
                asset_realized[asset_key]["volume_basis_product"] += volume * basis

                # Daily tracking
                asset_realized_daily[asset_key][day_key]["total_volume"] += volume
                asset_realized_daily[asset_key][day_key]["total_revenue"] += volume * node_price
                asset_realized_daily[asset_key][day_key]["volume_basis_product"] += volume * basis

                # Monthly tracking
                asset_realized_monthly[asset_key][month_key]["total_volume"] += volume
                asset_realized_monthly[asset_key][month_key]["total_revenue"] += volume * node_price
                asset_realized_monthly[asset_key][month_key]["volume_basis_product"] += volume * basis

                # Annual tracking
                asset_realized_annual[asset_key][year_key]["total_volume"] += volume
                asset_realized_annual[asset_key][year_key]["total_revenue"] += volume * node_price
                asset_realized_annual[asset_key][year_key]["volume_basis_product"] += volume * basis

                # For Holstein, track merchant portion separately (12.5% merchant)
                if asset_key == "HOLSTEIN":
                    merchant_pct = ASSET_CONFIG.get("HOLSTEIN", {}).get("merchant_percent", 12.5) / 100
                    # YTD
                    asset_realized[asset_key]["merchant_volume"] += volume * merchant_pct
                    asset_realized[asset_key]["merchant_revenue"] += volume * merchant_pct * node_price
                    # Daily
                    asset_realized_daily[asset_key][day_key]["merchant_volume"] += volume * merchant_pct
                    asset_realized_daily[asset_key][day_key]["merchant_revenue"] += volume * merchant_pct * node_price
                    # Monthly
                    asset_realized_monthly[asset_key][month_key]["merchant_volume"] += volume * merchant_pct
                    asset_realized_monthly[asset_key][month_key]["merchant_revenue"] += volume * merchant_pct * node_price
                    # Annual
                    asset_realized_annual[asset_key][year_key]["merchant_volume"] += volume * merchant_pct
                    asset_realized_annual[asset_key][year_key]["merchant_revenue"] += volume * merchant_pct * node_price

        except Exception as e:
            logger.error(f"Error aggregating Excel PnL record: {e}")
            continue

    # Round the aggregated values and calculate GWA basis
    for key, d in daily.items():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)
        d["avg_pnl_per_interval"] = round(d["pnl"] / d["count"], 2) if d["count"] > 0 else 0
        # Calculate GWA basis = Sum(Volume × Basis) / Sum(Volume)
        if d["volume"] > 0 and "volume_basis_product" in d:
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["gwa_basis"] = None
        d["records"] = d["records"][-20:]  # Keep last 20 records

    for d in monthly.values():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)
        # Calculate GWA basis
        if d["volume"] > 0 and "volume_basis_product" in d:
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["gwa_basis"] = None

    for d in annual.values():
        d["pnl"] = round(d["pnl"], 2)
        d["volume"] = round(d["volume"], 4)
        # Calculate GWA basis
        if d["volume"] > 0 and "volume_basis_product" in d:
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["gwa_basis"] = None

    # Helper function to calculate realized prices from tracking data
    def calc_realized_prices(asset_key, realized_data):
        """Calculate realized prices for an asset from tracking data."""
        config = ASSET_CONFIG.get(asset_key, {})
        result = {
            "realized_price": None,
            "realized_ppa_price": None,
            "realized_merchant_price": None,
            "gwa_basis": None,
        }

        total_vol = realized_data["total_volume"]
        if total_vol > 0:
            # GWA Basis = Sum(Volume × Basis) / Sum(Volume)
            gwa_basis = realized_data["volume_basis_product"] / total_vol
            result["gwa_basis"] = round(gwa_basis, 2)

            if asset_key == "BKI":
                # BKI (100% Merchant): Realized = Total Revenue / Total Volume
                result["realized_price"] = round(realized_data["total_revenue"] / total_vol, 2)

            elif asset_key == "BKII":
                # BKII (100% PPA at $34, 50% basis exposure):
                # Realized = PPA Price + (50% × GWA Basis)
                ppa_price = config.get("ppa_price", 34.0)
                basis_exposure = config.get("ppa_basis_exposure", 50) / 100
                result["realized_price"] = round(ppa_price + (basis_exposure * gwa_basis), 2)

            elif asset_key == "HOLSTEIN":
                # Holstein (87.5% PPA at $35 + 100% basis, 12.5% Merchant)
                ppa_price = config.get("ppa_price", 35.0)
                basis_exposure = config.get("ppa_basis_exposure", 100) / 100
                result["realized_ppa_price"] = round(ppa_price + (basis_exposure * gwa_basis), 2)

                merchant_vol = realized_data["merchant_volume"]
                if merchant_vol > 0:
                    result["realized_merchant_price"] = round(realized_data["merchant_revenue"] / merchant_vol, 2)

        return result

    # Round per-asset aggregations and calculate realized prices at all time levels
    assets_result = {}
    for asset_key in asset_totals.keys():
        config = ASSET_CONFIG.get(asset_key, {})

        # Calculate YTD realized prices
        ytd_realized = calc_realized_prices(asset_key, asset_realized[asset_key])

        # Calculate daily realized prices
        daily_realized = {}
        for day_key, day_data in asset_realized_daily[asset_key].items():
            daily_realized[day_key] = calc_realized_prices(asset_key, day_data)

        # Calculate monthly realized prices
        monthly_realized = {}
        for month_key, month_data in asset_realized_monthly[asset_key].items():
            monthly_realized[month_key] = calc_realized_prices(asset_key, month_data)

        # Calculate annual realized prices
        annual_realized = {}
        for year_key, year_data in asset_realized_annual[asset_key].items():
            annual_realized[year_key] = calc_realized_prices(asset_key, year_data)

        assets_result[asset_key] = {
            "display_name": config.get("display_name", asset_key),
            "ppa_percent": config.get("ppa_percent", 0),
            "merchant_percent": config.get("merchant_percent", 0),
            "ppa_price": config.get("ppa_price", 0),
            "total_pnl": round(asset_totals[asset_key]["pnl"], 2),
            "total_volume": round(asset_totals[asset_key]["volume"], 4),
            "record_count": asset_totals[asset_key]["count"],
            # YTD Realized price fields
            "realized_price": ytd_realized["realized_price"],
            "realized_ppa_price": ytd_realized["realized_ppa_price"],
            "realized_merchant_price": ytd_realized["realized_merchant_price"],
            "gwa_basis": ytd_realized["gwa_basis"],
            # Daily data with PnL, volume, and realized prices
            "daily_pnl": {k: {
                "pnl": round(v["pnl"], 2),
                "volume": round(v["volume"], 4),
                "count": v["count"],
                **daily_realized.get(k, {})
            } for k, v in asset_daily[asset_key].items()},
            # Monthly data with PnL, volume, and realized prices
            "monthly_pnl": {k: {
                "pnl": round(v["pnl"], 2),
                "volume": round(v["volume"], 4),
                "count": v["count"],
                **monthly_realized.get(k, {})
            } for k, v in asset_monthly[asset_key].items()},
            # Annual data with PnL, volume, and realized prices
            "annual_pnl": {k: {"pnl": round(v["pnl"], 2), "volume": round(v["volume"], 4), "count": v["count"], **annual_realized.get(k, {})}
                          for k, v in asset_annual[asset_key].items()},
        }

    # Calculate worst basis intervals (sorted by basis_pnl_impact, most negative first)
    # These are the intervals that hurt PnL the most due to basis
    worst_intervals = sorted(all_intervals, key=lambda x: x["basis_pnl_impact"])[:WORST_BASIS_INTERVALS_TO_TRACK]

    # Calculate total PnL
    total_pnl = sum(d["pnl"] for d in daily.values())
    total_volume = sum(d["volume"] for d in daily.values())

    # Log hub price matching stats
    if hub_price_lookup:
        logger.info(f"Hub price matches: {hub_matches}, misses: {hub_misses}")
    logger.info(f"Holstein worst basis intervals (yesterday only): {len(all_intervals)}")

    # Debug: Log Holstein today's GWA basis calculation
    today_cst = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    if "HOLSTEIN" in asset_realized_daily:
        holstein_today = asset_realized_daily["HOLSTEIN"].get(today_cst, {})
        vol = holstein_today.get("total_volume", 0)
        vbp = holstein_today.get("volume_basis_product", 0)
        gwa = vbp / vol if vol > 0 else 0
        logger.info(f"HOLSTEIN TODAY ({today_cst}): volume={vol:.2f} MWh, volume_basis_product={vbp:.2f}, GWA_basis=${gwa:.2f}")

    # Log asset distribution for debugging
    asset_summary = {k: {"pnl": round(v["pnl"], 2), "volume": round(v["volume"], 2), "count": v["count"]}
                     for k, v in asset_totals.items()}
    logger.info(f"Asset distribution: {asset_summary}")
    logger.info(f"Total PnL: ${round(total_pnl, 2)}, Total Volume: {round(total_volume, 2)} MWh")

    return {
        "daily": dict(daily),
        "monthly": dict(monthly),
        "annual": dict(annual),
        "total_pnl": round(total_pnl, 2),
        "total_volume": round(total_volume, 4),
        "record_count": len(records),
        "assets": assets_result,
        "worst_basis_intervals": worst_intervals[:WORST_BASIS_DISPLAY_COUNT],  # Top 10 for display
        "all_worst_intervals": worst_intervals,  # Full list for API
    }

# ============================================================================
# PHAROS AMS API FUNCTIONS (for NWOH - PJM asset)
# ============================================================================
from requests.auth import HTTPBasicAuth

def get_pharos_auth():
    """Get HTTP Basic Auth for Pharos API (token as username, empty password)."""
    return HTTPBasicAuth(PHAROS_API_TOKEN, '')

def fetch_pharos_locations():
    """Fetch asset locations from Pharos API."""
    try:
        url = f"{PHAROS_BASE_URL}/pjm/locations"
        params = {"organization_key": PHAROS_ORGANIZATION_KEY}

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            locations = data.get("locations", [])
            logger.info(f"Fetched {len(locations)} locations from Pharos API")
            return locations
        else:
            logger.error(f"Pharos locations API returned {response.status_code}: {response.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"Error fetching Pharos locations: {e}")
        return []

def fetch_pharos_da_awards(start_date=None, end_date=None):
    """
    Fetch Day-Ahead market results (awards) from Pharos API.
    Returns hourly DA awards with energy_mw, energy_price, and price_capped status.

    Args:
        start_date: Start date string (YYYY-MM-DD) or None for PHAROS_FETCH_START_DATE
        end_date: End date string (YYYY-MM-DD) or None for today
    """
    try:
        url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"

        if start_date is None:
            start_date = PHAROS_FETCH_START_DATE
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": start_date,
            "end_date": end_date,
        }

        logger.info(f"Fetching Pharos DA awards from {start_date} to {end_date}")
        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=120)

        if response.status_code == 200:
            data = response.json()
            awards = data.get("market_results", [])
            logger.info(f"Fetched {len(awards)} DA award records from Pharos API")

            # Log sample for debugging
            if awards:
                sample = awards[0]
                logger.info(f"Sample DA award: timestamp={sample.get('timestamp')}, energy_mw={sample.get('energy_mw')}, price={sample.get('energy_price')}, capped={sample.get('price_capped')}")

            return awards
        else:
            logger.error(f"Pharos DA awards API returned {response.status_code}: {response.text[:200]}")
            return []
    except requests.Timeout:
        logger.error("Pharos DA awards API request timed out")
        return []
    except Exception as e:
        logger.error(f"Error fetching Pharos DA awards: {e}")
        return []

def aggregate_pharos_da_data(awards):
    """
    Aggregate Pharos DA awards data by daily, monthly, and annual periods.
    Also tracks price-capped intervals.

    Args:
        awards: List of DA award records from Pharos API
    """
    est_tz = ZoneInfo("America/New_York")  # PJM uses Eastern time

    daily = defaultdict(lambda: {
        "da_mwh": 0, "da_revenue": 0, "count": 0,
        "capped_count": 0, "avg_price": 0, "hours": []
    })
    monthly = defaultdict(lambda: {
        "da_mwh": 0, "da_revenue": 0, "count": 0, "capped_count": 0, "avg_price": 0
    })
    annual = defaultdict(lambda: {
        "da_mwh": 0, "da_revenue": 0, "count": 0, "capped_count": 0, "avg_price": 0
    })

    # Track capped intervals for alerting
    capped_intervals = []

    for award in awards:
        try:
            timestamp = award.get("timestamp", "")
            energy_mw = float(award.get("energy_mw", 0) or 0)
            energy_price = float(award.get("energy_price", 0) or 0)
            price_capped = award.get("price_capped", False)

            # Parse timestamp (format: "2026-02-04T00:00:00.000-05:00")
            if "T" in timestamp:
                dt = datetime.fromisoformat(timestamp.replace(".000", ""))
            else:
                continue

            day_key = dt.strftime("%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")
            year_key = dt.strftime("%Y")
            hour = dt.strftime("%H:%M")

            # DA awards are hourly MWh values
            da_revenue = energy_mw * energy_price

            # Daily aggregation
            daily[day_key]["da_mwh"] += energy_mw
            daily[day_key]["da_revenue"] += da_revenue
            daily[day_key]["count"] += 1
            if price_capped:
                daily[day_key]["capped_count"] += 1
                capped_intervals.append({
                    "timestamp": timestamp,
                    "hour": hour,
                    "day": day_key,
                    "energy_mw": energy_mw,
                    "energy_price": energy_price,
                })
            daily[day_key]["hours"].append({
                "hour": hour,
                "mw": energy_mw,
                "price": energy_price,
                "capped": price_capped,
            })

            # Monthly aggregation
            monthly[month_key]["da_mwh"] += energy_mw
            monthly[month_key]["da_revenue"] += da_revenue
            monthly[month_key]["count"] += 1
            if price_capped:
                monthly[month_key]["capped_count"] += 1

            # Annual aggregation
            annual[year_key]["da_mwh"] += energy_mw
            annual[year_key]["da_revenue"] += da_revenue
            annual[year_key]["count"] += 1
            if price_capped:
                annual[year_key]["capped_count"] += 1

        except Exception as e:
            logger.error(f"Error processing Pharos DA award: {e}")
            continue

    # Calculate average prices
    for d in daily.values():
        if d["da_mwh"] > 0:
            d["avg_price"] = round(d["da_revenue"] / d["da_mwh"], 2)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)
        # Keep only last 24 hours for detail display
        d["hours"] = d["hours"][-24:]

    for d in monthly.values():
        if d["da_mwh"] > 0:
            d["avg_price"] = round(d["da_revenue"] / d["da_mwh"], 2)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)

    for d in annual.values():
        if d["da_mwh"] > 0:
            d["avg_price"] = round(d["da_revenue"] / d["da_mwh"], 2)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)

    total_da_mwh = sum(d["da_mwh"] for d in daily.values())
    total_da_revenue = sum(d["da_revenue"] for d in daily.values())
    total_capped = sum(d["capped_count"] for d in daily.values())

    logger.info(f"Pharos DA aggregation: {total_da_mwh:.2f} MWh, ${total_da_revenue:.2f}, {total_capped} capped intervals")

    return {
        "daily": dict(daily),
        "monthly": dict(monthly),
        "annual": dict(annual),
        "total_da_mwh": round(total_da_mwh, 2),
        "total_da_revenue": round(total_da_revenue, 2),
        "total_capped_count": total_capped,
        "capped_intervals": capped_intervals[-50:],  # Keep last 50 for display
        "record_count": len(awards),
    }

def fetch_pharos_unit_operations(start_date=None, end_date=None):
    """
    Fetch unit operations data from Pharos API (v1 endpoint).
    Fetches in 7-day chunks to avoid API timeouts on longer date ranges.
    This provides combined DA and RT data for PnL calculation:
    - da_award, da_lmp: Day-ahead awards and prices
    - gen: Actual generation
    - rt_lmp: Real-time prices
    - deviation_mw: DA vs RT deviations
    """
    try:
        url = f"{PHAROS_BASE_URL}/pjm/unit_operations/historic"

        if start_date is None:
            start_date = PHAROS_FETCH_START_DATE
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Parse dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Fetch in 7-day chunks to avoid API timeouts
        CHUNK_DAYS = 7
        all_ops = []
        current_start = start_dt

        logger.info(f"Fetching Pharos unit operations from {start_date} to {end_date} in {CHUNK_DAYS}-day chunks")

        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=CHUNK_DAYS), end_dt)

            params = {
                "organization_key": PHAROS_ORGANIZATION_KEY,
                "start_date": current_start.strftime("%Y-%m-%d"),
                "end_date": current_end.strftime("%Y-%m-%d"),
            }

            logger.info(f"  Fetching chunk: {params['start_date']} to {params['end_date']}")
            response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=120)

            if response.status_code == 200:
                data = response.json()
                ops = data.get("unit_operations", [])
                all_ops.extend(ops)
                logger.info(f"    Got {len(ops)} records (total: {len(all_ops)})")
            else:
                logger.error(f"Pharos API returned {response.status_code} for {params['start_date']}-{params['end_date']}: {response.text[:100]}")

            current_start = current_end

        logger.info(f"Fetched {len(all_ops)} total unit operations records from Pharos API")
        return all_ops

    except requests.Timeout:
        logger.error("Pharos unit operations API request timed out")
        return []
    except Exception as e:
        logger.error(f"Error fetching Pharos unit operations: {e}")
        return []

def fetch_pharos_hourly_revenue(start_date=None, end_date=None):
    """
    Fetch pre-calculated hourly revenue data from Pharos hourly_revenue_estimate endpoint.
    This endpoint provides all the values we need in one call with consistent calculations.

    Fields available:
    - gen_mw: Actual generation MW
    - dam_mw: DA award MW
    - dam_lmp: DA price
    - rt_mw: RT deviation (gen - dam)
    - rt_lmp: RT price
    - dam_revenue: Pre-calculated DA revenue
    - rt_revenue: Pre-calculated RT revenue
    - net_revenue: Total PJM revenue (dam + rt)
    """
    try:
        if start_date is None:
            start_date = PHAROS_FETCH_START_DATE
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching Pharos hourly revenue estimate from {start_date} to {end_date}")

        url = f"{PHAROS_BASE_URL}/pjm/hourly_revenue_estimate"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": start_date,
            "end_date": end_date,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=120)

        if response.status_code == 200:
            data = response.json()
            records = data.get("hourly_revenue_estimate", [])
            logger.info(f"Fetched {len(records)} hourly revenue records from Pharos")

            # Log first record to discover all available fields (including potential hub LMP)
            if records:
                logger.info(f"[HOURLY_REV] First record keys: {list(records[0].keys())}")
                logger.info(f"[HOURLY_REV] First record: {records[0]}")

            # Convert to format expected by aggregate function
            converted = []
            for r in records:
                gen_mw = r.get("gen_mw")  # Can be None for future hours
                dam_mw = r.get("dam_mw") or 0
                rt_mw = r.get("rt_mw") or 0  # gen - dam

                converted.append({
                    "timestamp": r.get("hour", ""),
                    "date": r.get("date", ""),
                    "he": r.get("he", 0),
                    "gen": gen_mw if gen_mw is not None else 0,
                    "dam_mw": dam_mw,
                    "da_lmp": r.get("dam_lmp") or 0,
                    "rt_mw": rt_mw,
                    "rt_lmp": r.get("rt_lmp") or 0,
                    "dam_revenue": r.get("dam_revenue") or 0,
                    "rt_revenue": r.get("rt_revenue") or 0,
                    "net_revenue": r.get("net_revenue") or 0,
                    "has_gen_data": gen_mw is not None,
                    "is_hourly": True,
                    "source": "hourly_revenue_estimate",
                })

            return converted
        else:
            logger.warning(f"Pharos hourly_revenue_estimate returned {response.status_code}")
            return []

    except Exception as e:
        logger.error(f"Error fetching Pharos hourly revenue: {e}")
        return []


def fetch_pharos_combined_pnl_data(start_date=None, end_date=None):
    """
    Fetch and combine data from multiple Pharos endpoints for PnL calculation.
    This is used as a fallback when unit_operations/historic returns empty.

    Combines:
    - DA awards from market_results/historic (energy_mw, energy_price)
    - Actual generation from power_meter/submissions (hourly MWh)
    - RT LMP from lmp/historic (rt_lmp)

    Returns list of hourly records matching the format expected by aggregate_pharos_unit_operations.
    """
    try:
        if start_date is None:
            start_date = PHAROS_FETCH_START_DATE
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching combined Pharos PnL data from {start_date} to {end_date}")

        # Parse date range
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        all_records = []
        current_date = start_dt

        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")

            # 1. Fetch DA awards from market_results
            da_url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"
            da_params = {
                "organization_key": PHAROS_ORGANIZATION_KEY,
                "start_date": date_str,
                "end_date": date_str,
            }
            da_response = requests.get(da_url, auth=get_pharos_auth(), params=da_params, timeout=60)

            da_by_hour = {}
            if da_response.status_code == 200:
                da_data = da_response.json()
                da_results = da_data.get("market_results", da_data) if isinstance(da_data, dict) else da_data
                for r in da_results:
                    ts = r.get("timestamp", "")
                    if " " in ts:
                        hour = int(ts.split(" ")[1].split(":")[0])
                    elif "T" in ts:
                        hour = int(ts.split("T")[1].split(":")[0])
                    else:
                        continue
                    da_by_hour[hour] = {
                        "da_mw": r.get("energy_mw", 0) or 0,
                        "da_lmp": r.get("energy_price", 0) or 0,
                        "price_capped": r.get("price_capped", False),
                    }

            # 2. Fetch actual generation from power_meter/submissions
            meter_url = f"{PHAROS_BASE_URL}/pjm/power_meter/submissions"
            meter_params = {
                "organization_key": PHAROS_ORGANIZATION_KEY,
                "start_date": date_str,
                "end_date": date_str,
            }
            meter_response = requests.get(meter_url, auth=get_pharos_auth(), params=meter_params, timeout=60)

            gen_by_hour = {}
            gen_source = "meter"
            if meter_response.status_code == 200:
                meter_data = meter_response.json()
                submissions = meter_data.get("submissions", [])
                if submissions:
                    meter_values = submissions[0].get("meter_values", [])
                    for i, mv in enumerate(meter_values):
                        # Parse hour from start_date field (e.g., "2026-02-10T00:00:00.000-05:00")
                        hour = None
                        start_date_str = mv.get("start_date", "")
                        if start_date_str:
                            if "T" in start_date_str:
                                hour = int(start_date_str.split("T")[1].split(":")[0])
                            elif " " in start_date_str:
                                hour = int(start_date_str.split(" ")[1].split(":")[0])

                        # Fall back to other hour fields if start_date parsing failed
                        if hour is None:
                            hour = mv.get("hour_beginning")
                        if hour is None:
                            hour = mv.get("hour")
                        if hour is None:
                            hour = i  # Last resort: array index

                        mw_val = float(mv.get("mw", 0) or 0)
                        gen_by_hour[hour] = mw_val

                    logger.debug(f"Meter values for {date_str}: {len(meter_values)} entries, hours: {sorted(gen_by_hour.keys())}, total: {sum(gen_by_hour.values())}")

            # 2b. If no meter data, fall back to dispatches/historic for real-time generation
            if not gen_by_hour:
                dispatch_url = f"{PHAROS_BASE_URL}/pjm/dispatches/historic"
                dispatch_params = {
                    "organization_key": PHAROS_ORGANIZATION_KEY,
                    "start_date": date_str,
                    "end_date": date_str,
                }
                dispatch_response = requests.get(dispatch_url, auth=get_pharos_auth(), params=dispatch_params, timeout=60)

                if dispatch_response.status_code == 200:
                    dispatch_data = dispatch_response.json()
                    dispatches = dispatch_data.get("dispatches", [])
                    if dispatches:
                        gen_source = "dispatches"
                        # Group by hour and sum gen_send_out (5-minute intervals -> hourly MWh)
                        hourly_gen = defaultdict(float)
                        for d in dispatches:
                            ts = d.get("timestamp", "")
                            if "T" in ts:
                                hour = int(ts.split("T")[1].split(":")[0])
                            elif " " in ts:
                                hour = int(ts.split(" ")[1].split(":")[0])
                            else:
                                continue
                            # gen_send_out is MW, each interval is 5 min = 5/60 hours
                            gen_mw = d.get("gen_send_out", 0) or 0
                            hourly_gen[hour] += gen_mw * (5/60)  # Convert to MWh

                        gen_by_hour = dict(hourly_gen)
                        logger.debug(f"Dispatch values for {date_str}: {len(dispatches)} intervals, {len(gen_by_hour)} hours, total: {sum(gen_by_hour.values()):.2f} MWh")

            # 3. Fetch RT LMP from lmp/historic
            lmp_url = f"{PHAROS_BASE_URL}/pjm/lmp/historic"
            lmp_params = {
                "organization_key": PHAROS_ORGANIZATION_KEY,
                "start_date": date_str,
                "end_date": date_str,
            }
            lmp_response = requests.get(lmp_url, auth=get_pharos_auth(), params=lmp_params, timeout=60)

            rt_lmp_by_hour = {}
            if lmp_response.status_code == 200:
                lmp_data = lmp_response.json()
                lmps = lmp_data.get("lmp", [])
                for l in lmps:
                    hour = l.get("hour_beginning", 0)
                    rt_lmp_by_hour[hour] = l.get("rt_lmp", 0) or 0

            # 4. Combine into hourly records (mimicking unit_operations format)
            # Log data counts for debugging
            if da_by_hour or gen_by_hour or rt_lmp_by_hour:
                logger.debug(f"Data for {date_str}: DA hours={len(da_by_hour)}, Meter hours={len(gen_by_hour)}, LMP hours={len(rt_lmp_by_hour)}")

            for hour in range(24):
                da_data = da_by_hour.get(hour, {})
                da_mw = da_data.get("da_mw", 0)
                da_lmp = da_data.get("da_lmp", 0)
                gen_mwh = gen_by_hour.get(hour, 0)  # Meter reading (MWh for the hour)
                rt_lmp = rt_lmp_by_hour.get(hour, 0)

                # Skip hours with no data
                if da_mw == 0 and gen_mwh == 0:
                    continue

                # Create hourly record (in MWh, not MW)
                # Note: Since this is hourly data, MW = MWh for the hour
                record = {
                    "timestamp": f"{date_str}T{hour:02d}:00:00",
                    "dam_mw": da_mw,  # DA award in MWh (hourly)
                    "da_lmp": da_lmp,
                    "gen": gen_mwh,  # Actual generation in MWh (hourly)
                    "rt_lmp": rt_lmp,
                    "price_capped": da_data.get("price_capped", False),
                    "is_hourly": True,  # Flag to indicate this is hourly data, not 5-min
                }
                all_records.append(record)

            current_date += timedelta(days=1)

        logger.info(f"Fetched {len(all_records)} combined hourly records from Pharos API")
        return all_records

    except Exception as e:
        logger.error(f"Error fetching combined Pharos PnL data: {e}")
        return []


def fetch_pharos_price_caps():
    """
    Fetch current price cap status from Pharos market_results.
    Returns price cap info for any hours that are currently capped.
    Uses the price_capped field from /pjm/market_results/historic endpoint.
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": today,
            "end_date": today,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            results = data.get("market_results", data) if isinstance(data, dict) else data

            price_caps = []
            for r in results:
                if r.get("price_capped"):
                    # Extract hour from timestamp
                    # Handles both formats: "2026-02-11T15:00:00.000-05:00" and "2026-02-11 15:00:00 -0500"
                    ts = r.get("timestamp", "")
                    hour_ending = None
                    if "T" in ts:
                        time_part = ts.split("T")[1]
                        hour = int(time_part.split(":")[0])
                        hour_ending = hour + 1  # Convert to hour ending
                    elif " " in ts:
                        time_part = ts.split(" ")[1]
                        hour = int(time_part.split(":")[0])
                        hour_ending = hour + 1

                    price_caps.append({
                        "hour_ending": hour_ending,
                        "energy_price": r.get("energy_price"),
                        "energy_mw": r.get("energy_mw"),
                        "timestamp": ts,
                    })

            return {
                "is_capped": len(price_caps) > 0,
                "caps": price_caps,
                "total_hours": len(results),
                "capped_count": len(price_caps),
                "fetched_at": datetime.now().isoformat(),
            }
        else:
            logger.error(f"Pharos market_results API returned {response.status_code}")
            return {"is_capped": False, "caps": [], "error": response.status_code}

    except Exception as e:
        logger.error(f"Error fetching Pharos price caps: {e}")
        return {"is_capped": False, "caps": [], "error": str(e)}

def fetch_pharos_next_day_awards():
    """
    Fetch next-day (tomorrow) DA awards from Pharos market_results.
    Returns hourly DA awards for tomorrow to show what we've been awarded.
    Uses the energy_mw field from /pjm/market_results/historic endpoint.
    """
    try:
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": tomorrow,
            "end_date": tomorrow,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            results = data.get("market_results", data) if isinstance(data, dict) else data

            # Extract hourly DA awards for tomorrow
            hourly_awards = []
            total_da_mwh = 0
            total_da_revenue = 0
            capped_hours = 0

            for r in results:
                energy_mw = float(r.get("energy_mw", 0) or 0)
                energy_price = float(r.get("energy_price", 0) or 0)
                timestamp = r.get("timestamp", "")
                is_capped = r.get("price_capped", False)

                da_revenue = energy_mw * energy_price
                hourly_awards.append({
                    "timestamp": timestamp,
                    "da_award_mw": energy_mw,
                    "da_lmp": energy_price,
                    "da_revenue": da_revenue,
                    "price_capped": is_capped,
                })
                total_da_mwh += energy_mw
                total_da_revenue += da_revenue
                if is_capped:
                    capped_hours += 1

            # Calculate volume-weighted average DA price
            avg_da_price = total_da_revenue / total_da_mwh if total_da_mwh > 0 else 0

            return {
                "date": tomorrow,
                "total_da_mwh": round(total_da_mwh, 2),
                "total_da_revenue": round(total_da_revenue, 2),
                "avg_da_price": round(avg_da_price, 2),
                "hourly": hourly_awards,
                "hours_awarded": len([h for h in hourly_awards if h["da_award_mw"] > 0]),
                "capped_hours": capped_hours,
                "fetched_at": datetime.now().isoformat(),
            }
        else:
            logger.warning(f"No next-day awards available yet: {response.status_code}")
            return {"date": tomorrow, "total_da_mwh": 0, "hourly": [], "hours_awarded": 0, "capped_hours": 0}

    except Exception as e:
        logger.error(f"Error fetching next-day DA awards: {e}")
        return {"date": None, "total_da_mwh": 0, "hourly": [], "error": str(e)}

def fetch_pharos_today_da_awards():
    """
    Fetch today's DA awards from Pharos market_results.
    This gives the full 24-hour commitment (what we were awarded yesterday for today).
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": today,
            "end_date": today,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            results = data.get("market_results", data) if isinstance(data, dict) else data

            hourly_awards = []
            total_da_mwh = 0
            total_da_revenue = 0

            for r in results:
                energy_mw = float(r.get("energy_mw", 0) or 0)
                energy_price = float(r.get("energy_price", 0) or 0)
                timestamp = r.get("timestamp", "")
                is_capped = r.get("price_capped", False)

                # Extract hour ending from timestamp
                hour_ending = None
                if "T" in timestamp:
                    hour = int(timestamp.split("T")[1].split(":")[0])
                    hour_ending = hour + 1 if hour < 23 else 24
                elif " " in timestamp:
                    hour = int(timestamp.split(" ")[1].split(":")[0])
                    hour_ending = hour + 1 if hour < 23 else 24

                da_revenue = energy_mw * energy_price
                hourly_awards.append({
                    "hour_ending": hour_ending,
                    "timestamp": timestamp,
                    "da_award_mw": energy_mw,
                    "da_lmp": energy_price,
                    "da_revenue": da_revenue,
                    "price_capped": is_capped,
                })
                total_da_mwh += energy_mw
                total_da_revenue += da_revenue

            avg_da_price = total_da_revenue / total_da_mwh if total_da_mwh > 0 else 0

            return {
                "date": today,
                "total_da_mwh": round(total_da_mwh, 2),
                "total_da_revenue": round(total_da_revenue, 2),
                "avg_da_price": round(avg_da_price, 2),
                "hourly": sorted(hourly_awards, key=lambda x: x["hour_ending"] or 0),
                "hours_with_awards": len([h for h in hourly_awards if h["da_award_mw"] > 0]),
                "fetched_at": datetime.now().isoformat(),
            }
        else:
            logger.warning(f"Could not fetch today's DA awards: {response.status_code}")
            return {"date": today, "total_da_mwh": 0, "hourly": [], "hours_with_awards": 0}

    except Exception as e:
        logger.error(f"Error fetching today's DA awards: {e}")
        return {"date": None, "total_da_mwh": 0, "hourly": [], "error": str(e)}

def fetch_pharos_current_dispatch():
    """
    Fetch current dispatch status from Pharos to show real-time performance.
    """
    try:
        url = f"{PHAROS_BASE_URL}/pjm/dispatches/current"
        params = {"organization_key": PHAROS_ORGANIZATION_KEY}

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            dispatches = data.get("dispatches", [])

            if dispatches:
                d = dispatches[0]
                return {
                    "timestamp": d.get("timestamp"),
                    "gen_send_out": d.get("gen_send_out", 0),
                    "lambda_mw": d.get("lambda_mw", 0),
                    "deviation_mw": d.get("deviation_mw", 0),
                    "energy_max": d.get("energy_max", 0),
                    "capacity_max": d.get("capacity_max", 0),
                    "dispatch_rate": d.get("lambda_dispatch_rate", 0),
                    "status": d.get("status"),
                }
            return None
        else:
            return None

    except Exception as e:
        logger.error(f"Error fetching current dispatch: {e}")
        return None

def fetch_pharos_today_generation():
    """
    Fetch today's actual generation from dispatches/historic endpoint.
    Returns both total MWh and per-hour breakdown (MWh per hour ending).
    Uses gen_send_out field which shows actual MW output at each 5-minute interval.
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"{PHAROS_BASE_URL}/pjm/dispatches/historic"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": today,
            "end_date": today,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            dispatches = data.get("dispatches", [])

            if dispatches:
                # Calculate total MWh and per-hour breakdown from gen_send_out
                # Each 5-minute interval: MW * (5/60) = MWh
                total_mwh = 0
                hourly_gen = defaultdict(float)
                hourly_count = defaultdict(int)

                for d in dispatches:
                    gen_mw = d.get("gen_send_out", 0) or 0
                    interval_mwh = gen_mw * (5/60)
                    total_mwh += interval_mwh

                    ts = d.get("timestamp", "")
                    hour = None
                    if "T" in ts:
                        hour = int(ts.split("T")[1].split(":")[0])
                    elif " " in ts:
                        hour = int(ts.split(" ")[1].split(":")[0])

                    if hour is not None:
                        he = hour + 1 if hour < 23 else 24  # Convert to hour ending
                        hourly_gen[he] += interval_mwh
                        hourly_count[he] += 1

                last_dispatch = dispatches[-1]
                last_ts = last_dispatch.get("timestamp", "")

                return {
                    "total_mwh": round(total_mwh, 2),
                    "hourly_gen": {he: round(mwh, 2) for he, mwh in hourly_gen.items()},
                    "hourly_intervals": dict(hourly_count),
                    "interval_count": len(dispatches),
                    "hours_covered": round(len(dispatches) * 5 / 60, 1),
                    "last_timestamp": last_ts,
                    "current_mw": last_dispatch.get("gen_send_out", 0),
                    "source": "dispatches",
                }
            return {"total_mwh": 0, "hourly_gen": {}, "interval_count": 0, "source": "dispatches"}
        else:
            logger.warning(f"Failed to fetch today's dispatches: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Error fetching today's generation from dispatches: {e}")
        return None


def fetch_pharos_today_rt_lmp():
    """
    Fetch today's RT LMP from Pharos lmp/historic endpoint.
    Returns dict keyed by hour ending with RT LMP values.
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"{PHAROS_BASE_URL}/pjm/lmp/historic"
        params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": today,
            "end_date": today,
        }

        response = requests.get(url, auth=get_pharos_auth(), params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            lmps = data.get("lmp", [])

            # Log first record fields to discover hub LMP field name
            if lmps:
                logger.info(f"[LMP API] First record keys: {list(lmps[0].keys())}")
                logger.info(f"[LMP API] First record: {lmps[0]}")

            rt_lmp_by_he = {}
            hub_lmp_by_he = {}
            for l in lmps:
                hour = l.get("hour_beginning", 0)
                he = hour + 1 if hour < 23 else 24
                rt_lmp_by_he[he] = float(l.get("rt_lmp", 0) or 0)
                # Try multiple possible field names for hub RT LMP
                hub_val = (l.get("hub_rt_lmp") or l.get("hub_lmp") or
                          l.get("hub_rt_total_lmp") or l.get("hub_total_lmp_rt") or
                          l.get("total_lmp_rt_hub") or 0)
                hub_lmp_by_he[he] = float(hub_val or 0)

            return {"rt_lmp": rt_lmp_by_he, "hub_lmp": hub_lmp_by_he}
        else:
            logger.warning(f"Failed to fetch today's RT LMP: {response.status_code}")
            return {"rt_lmp": {}, "hub_lmp": {}}

    except Exception as e:
        logger.error(f"Error fetching today's RT LMP: {e}")
        return {"rt_lmp": {}, "hub_lmp": {}}

def aggregate_pharos_unit_operations(ops):
    """
    Aggregate Pharos unit operations data by daily, monthly, and annual periods.
    Calculates PnL from DA awards + RT deviations.

    PnL Formula for PJM:
    - DA Revenue = DA Award (MWh) × DA LMP
    - RT Imbalance = RT Deviation (MWh) × RT LMP (negative deviation = under-gen = buy back)
    - Total PnL = DA Revenue + RT Imbalance

    Args:
        ops: List of unit operation records from Pharos API
    """
    est_tz = ZoneInfo("America/New_York")

    # Try to cache hub prices for basis calculation
    if ops:
        # Get date range from ops
        dates = [op.get("timestamp", "")[:10] for op in ops if op.get("timestamp")]
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            try:
                ensure_hub_prices_cached(min_date, max_date)
            except Exception as e:
                logger.warning(f"Could not cache hub prices: {e}")

    daily = defaultdict(lambda: {
        "pnl": 0, "volume": 0, "count": 0,
        "da_mwh": 0, "da_revenue": 0,
        "da_lmp_product": 0,  # Sum of DA MWh × DA LMP for weighted avg
        "rt_mwh": 0, "rt_imbalance": 0,
        "rt_sales_mwh": 0, "rt_sales_revenue": 0,  # Over-generation sold at RT
        "rt_purchase_mwh": 0, "rt_purchase_cost": 0,  # Under-generation bought at RT
        "rt_lmp_product": 0,  # Sum of gen MWh × RT LMP for weighted avg
        "volume_basis_product": 0,
        "avg_da_price": 0, "avg_rt_price": 0,
    })
    monthly = defaultdict(lambda: {
        "pnl": 0, "volume": 0, "count": 0,
        "da_mwh": 0, "da_revenue": 0,
        "da_lmp_product": 0,
        "rt_mwh": 0, "rt_imbalance": 0,
        "rt_sales_mwh": 0, "rt_sales_revenue": 0,
        "rt_purchase_mwh": 0, "rt_purchase_cost": 0,
        "rt_lmp_product": 0,
        "volume_basis_product": 0,
    })
    annual = defaultdict(lambda: {
        "pnl": 0, "volume": 0, "count": 0,
        "da_mwh": 0, "da_revenue": 0,
        "da_lmp_product": 0,
        "rt_mwh": 0, "rt_imbalance": 0,
        "rt_sales_mwh": 0, "rt_sales_revenue": 0,
        "rt_purchase_mwh": 0, "rt_purchase_cost": 0,
        "rt_lmp_product": 0,
        "volume_basis_product": 0,
    })

    for op in ops:
        try:
            timestamp = op.get("timestamp", "")
            if not timestamp:
                continue

            # Parse timestamp - handle multiple formats
            # ISO format: 2026-02-11T00:00:00.000
            # Pharos format: 2026-02-11 00:00:00 -0500
            if "T" in timestamp:
                dt = datetime.fromisoformat(timestamp.replace(".000", ""))
            elif " " in timestamp:
                # Pharos format: "2026-02-11 00:00:00 -0500"
                # Split off the timezone suffix and parse
                parts = timestamp.rsplit(" ", 1)
                if len(parts) == 2 and (parts[1].startswith("-") or parts[1].startswith("+")):
                    # Has timezone suffix like -0500
                    dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                else:
                    # No timezone suffix
                    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            else:
                continue

            day_key = dt.strftime("%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")
            year_key = dt.strftime("%Y")

            # Extract values - use pre-calculated values from hourly_revenue_estimate if available
            source = op.get("source", "")
            is_hourly = op.get("is_hourly", False)
            INTERVAL_HOURS = 1.0 if is_hourly else (5 / 60)  # Hourly vs 5-minute intervals

            # Check if this is from hourly_revenue_estimate (has pre-calculated values)
            if source == "hourly_revenue_estimate":
                # Use Pharos pre-calculated values directly (already in correct units)
                actual_gen_mwh = float(op.get("gen", 0) or 0)  # gen_mw is actually MWh for hourly
                dam_mwh = float(op.get("dam_mw", 0) or 0)
                da_lmp = float(op.get("da_lmp", 0) or 0)
                rt_mwh = float(op.get("rt_mw", 0) or 0)  # RT deviation (gen - dam)
                rt_lmp = float(op.get("rt_lmp", 0) or 0)

                # Use pre-calculated revenue values from Pharos
                da_revenue = float(op.get("dam_revenue", 0) or 0)
                rt_imbalance = float(op.get("rt_revenue", 0) or 0)
                interval_pnl = float(op.get("net_revenue", 0) or 0)

                # Separate RT sales and purchases based on rt_mwh sign
                if rt_mwh > 0:
                    rt_sales_mwh = rt_mwh
                    rt_sales_rev = rt_imbalance if rt_imbalance > 0 else 0
                    rt_purchase_mwh = 0
                    rt_purchase_cost = 0
                else:
                    rt_sales_mwh = 0
                    rt_sales_rev = 0
                    rt_purchase_mwh = abs(rt_mwh)
                    rt_purchase_cost = abs(rt_imbalance) if rt_imbalance < 0 else 0
            else:
                # Fallback: calculate values ourselves (old method)
                dam_mw = float(op.get("dam_mw") or op.get("da_award") or 0)
                da_lmp = float(op.get("da_lmp", 0) or 0)
                gen_mw = float(op.get("gen", 0) or 0)
                meter_mw = float(op.get("meter_mw", 0) or 0)
                rt_lmp = float(op.get("rt_lmp", 0) or 0)

                actual_gen_mw = meter_mw if meter_mw else gen_mw
                rt_deviation_mw = actual_gen_mw - dam_mw

                actual_gen_mwh = actual_gen_mw * INTERVAL_HOURS
                dam_mwh = dam_mw * INTERVAL_HOURS
                rt_mwh = rt_deviation_mw * INTERVAL_HOURS

                da_revenue = dam_mwh * da_lmp
                rt_imbalance = rt_mwh * rt_lmp

                if rt_mwh > 0:
                    rt_sales_mwh = rt_mwh
                    rt_sales_rev = rt_mwh * rt_lmp
                    rt_purchase_mwh = 0
                    rt_purchase_cost = 0
                else:
                    rt_sales_mwh = 0
                    rt_sales_rev = 0
                    rt_purchase_mwh = abs(rt_mwh)
                    rt_purchase_cost = abs(rt_mwh * rt_lmp)

                interval_pnl = da_revenue + rt_imbalance

            # Basis calculation (hub - node)
            # Try to get hub price from PJM cache, fall back to DA/RT spread
            hub_lmp = get_hub_price_for_timestamp(timestamp)
            if hub_lmp is not None:
                # Proper basis = Hub - Node (positive = hub higher than node)
                basis = hub_lmp - rt_lmp
            else:
                # Fallback: use DA/RT spread as proxy
                basis = da_lmp - rt_lmp if da_lmp else 0

            # Daily aggregation
            daily[day_key]["pnl"] += interval_pnl
            daily[day_key]["volume"] += actual_gen_mwh
            daily[day_key]["count"] += 1
            daily[day_key]["da_mwh"] += dam_mwh
            daily[day_key]["da_revenue"] += da_revenue
            daily[day_key]["da_lmp_product"] += dam_mwh * da_lmp  # For weighted avg
            daily[day_key]["rt_mwh"] += actual_gen_mwh
            daily[day_key]["rt_imbalance"] += rt_imbalance
            daily[day_key]["rt_sales_mwh"] += rt_sales_mwh
            daily[day_key]["rt_sales_revenue"] += rt_sales_rev
            daily[day_key]["rt_purchase_mwh"] += rt_purchase_mwh
            daily[day_key]["rt_purchase_cost"] += rt_purchase_cost
            daily[day_key]["rt_lmp_product"] += actual_gen_mwh * rt_lmp  # For weighted avg
            if hub_lmp is not None:
                daily[day_key]["hub_lmp_product"] = daily[day_key].get("hub_lmp_product", 0) + actual_gen_mwh * hub_lmp
                daily[day_key]["hub_volume"] = daily[day_key].get("hub_volume", 0) + actual_gen_mwh
            if actual_gen_mwh > 0:
                daily[day_key]["volume_basis_product"] += actual_gen_mwh * basis

            # Monthly aggregation
            monthly[month_key]["pnl"] += interval_pnl
            monthly[month_key]["volume"] += actual_gen_mwh
            monthly[month_key]["count"] += 1
            monthly[month_key]["da_mwh"] += dam_mwh
            monthly[month_key]["da_revenue"] += da_revenue
            monthly[month_key]["da_lmp_product"] += dam_mwh * da_lmp
            monthly[month_key]["rt_mwh"] += actual_gen_mwh
            monthly[month_key]["rt_imbalance"] += rt_imbalance
            monthly[month_key]["rt_sales_mwh"] += rt_sales_mwh
            monthly[month_key]["rt_sales_revenue"] += rt_sales_rev
            monthly[month_key]["rt_purchase_mwh"] += rt_purchase_mwh
            monthly[month_key]["rt_purchase_cost"] += rt_purchase_cost
            monthly[month_key]["rt_lmp_product"] += actual_gen_mwh * rt_lmp
            if hub_lmp is not None:
                monthly[month_key]["hub_lmp_product"] = monthly[month_key].get("hub_lmp_product", 0) + actual_gen_mwh * hub_lmp
                monthly[month_key]["hub_volume"] = monthly[month_key].get("hub_volume", 0) + actual_gen_mwh
            if actual_gen_mwh > 0:
                monthly[month_key]["volume_basis_product"] += actual_gen_mwh * basis

            # Annual aggregation
            annual[year_key]["pnl"] += interval_pnl
            annual[year_key]["volume"] += actual_gen_mwh
            annual[year_key]["count"] += 1
            annual[year_key]["da_mwh"] += dam_mwh
            annual[year_key]["da_revenue"] += da_revenue
            annual[year_key]["da_lmp_product"] += dam_mwh * da_lmp
            annual[year_key]["rt_mwh"] += actual_gen_mwh
            annual[year_key]["rt_imbalance"] += rt_imbalance
            annual[year_key]["rt_sales_mwh"] += rt_sales_mwh
            annual[year_key]["rt_sales_revenue"] += rt_sales_rev
            annual[year_key]["rt_purchase_mwh"] += rt_purchase_mwh
            annual[year_key]["rt_purchase_cost"] += rt_purchase_cost
            annual[year_key]["rt_lmp_product"] += actual_gen_mwh * rt_lmp
            if hub_lmp is not None:
                annual[year_key]["hub_lmp_product"] = annual[year_key].get("hub_lmp_product", 0) + actual_gen_mwh * hub_lmp
                annual[year_key]["hub_volume"] = annual[year_key].get("hub_volume", 0) + actual_gen_mwh
            if actual_gen_mwh > 0:
                annual[year_key]["volume_basis_product"] += actual_gen_mwh * basis

        except Exception as e:
            logger.error(f"Error processing Pharos unit operation: {e}")
            continue

    # NWOH PPA constants
    PPA_PRICE = 33.31  # Fixed PPA price $/MWh

    # Round values and calculate averages
    for d in daily.values():
        # Store PJM-only revenue first
        pjm_gross = d["pnl"]
        d["pjm_gross_revenue"] = round(pjm_gross, 2)

        d["volume"] = round(d["volume"], 4)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)
        d["rt_mwh"] = round(d["rt_mwh"], 4)
        d["rt_imbalance"] = round(d["rt_imbalance"], 2)
        d["rt_sales_mwh"] = round(d.get("rt_sales_mwh", 0), 4)
        d["rt_sales_revenue"] = round(d.get("rt_sales_revenue", 0), 2)
        d["rt_purchase_mwh"] = round(d.get("rt_purchase_mwh", 0), 4)
        d["rt_purchase_cost"] = round(d.get("rt_purchase_cost", 0), 2)
        # Weighted average prices
        if d["da_mwh"] > 0:
            d["avg_da_price"] = round(d["da_lmp_product"] / d["da_mwh"], 2)
        else:
            d["avg_da_price"] = None
        if d["volume"] > 0:
            d["avg_rt_price"] = round(d["rt_lmp_product"] / d["volume"], 2)
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["avg_rt_price"] = None
            d["gwa_basis"] = None
        # Hub price (if available)
        hub_vol = d.get("hub_volume", 0)
        if hub_vol > 0:
            d["avg_hub_price"] = round(d.get("hub_lmp_product", 0) / hub_vol, 2)
        else:
            d["avg_hub_price"] = None

        # Calculate PPA settlement for NWOH
        # Fixed payment = Gen × $33.31 (revenue from GM)
        # Floating payment = Gen × Hub LMP (cost to GM)
        # Net PPA = Fixed - Floating (positive when hub < $33.31)
        gen_mwh = d["volume"]
        avg_hub = d.get("avg_hub_price") or d.get("avg_rt_price") or 0
        if gen_mwh > 0:
            d["ppa_fixed_payment"] = round(gen_mwh * PPA_PRICE, 2)
            d["ppa_floating_payment"] = round(gen_mwh * avg_hub, 2)
            d["ppa_net_settlement"] = round(d["ppa_fixed_payment"] - d["ppa_floating_payment"], 2)
            # Total PnL = PJM revenue + PPA settlement
            d["pnl"] = round(pjm_gross + d["ppa_net_settlement"], 2)
            # Realized price = total PnL / volume
            d["realized_price"] = round(d["pnl"] / gen_mwh, 2) if gen_mwh > 0 else None
        else:
            d["ppa_fixed_payment"] = 0
            d["ppa_floating_payment"] = 0
            d["ppa_net_settlement"] = 0
            d["pnl"] = round(pjm_gross, 2)
            d["realized_price"] = None

    for d in monthly.values():
        # Store PJM-only revenue first
        pjm_gross = d["pnl"]
        d["pjm_gross_revenue"] = round(pjm_gross, 2)

        d["volume"] = round(d["volume"], 4)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)
        d["rt_mwh"] = round(d["rt_mwh"], 4)
        d["rt_imbalance"] = round(d["rt_imbalance"], 2)
        d["rt_sales_mwh"] = round(d.get("rt_sales_mwh", 0), 4)
        d["rt_sales_revenue"] = round(d.get("rt_sales_revenue", 0), 2)
        d["rt_purchase_mwh"] = round(d.get("rt_purchase_mwh", 0), 4)
        d["rt_purchase_cost"] = round(d.get("rt_purchase_cost", 0), 2)
        if d["da_mwh"] > 0:
            d["avg_da_price"] = round(d["da_lmp_product"] / d["da_mwh"], 2)
        else:
            d["avg_da_price"] = None
        if d["volume"] > 0:
            d["avg_rt_price"] = round(d["rt_lmp_product"] / d["volume"], 2)
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["avg_rt_price"] = None
            d["gwa_basis"] = None
        hub_vol = d.get("hub_volume", 0)
        if hub_vol > 0:
            d["avg_hub_price"] = round(d.get("hub_lmp_product", 0) / hub_vol, 2)
        else:
            d["avg_hub_price"] = None

        # Calculate PPA settlement for NWOH
        gen_mwh = d["volume"]
        avg_hub = d.get("avg_hub_price") or d.get("avg_rt_price") or 0
        if gen_mwh > 0:
            d["ppa_fixed_payment"] = round(gen_mwh * PPA_PRICE, 2)
            d["ppa_floating_payment"] = round(gen_mwh * avg_hub, 2)
            d["ppa_net_settlement"] = round(d["ppa_fixed_payment"] - d["ppa_floating_payment"], 2)
            d["pnl"] = round(pjm_gross + d["ppa_net_settlement"], 2)
            d["realized_price"] = round(d["pnl"] / gen_mwh, 2)
        else:
            d["ppa_fixed_payment"] = 0
            d["ppa_floating_payment"] = 0
            d["ppa_net_settlement"] = 0
            d["pnl"] = round(pjm_gross, 2)
            d["realized_price"] = None

    for d in annual.values():
        # Store PJM-only revenue first
        pjm_gross = d["pnl"]
        d["pjm_gross_revenue"] = round(pjm_gross, 2)

        d["volume"] = round(d["volume"], 4)
        d["da_mwh"] = round(d["da_mwh"], 2)
        d["da_revenue"] = round(d["da_revenue"], 2)
        d["rt_mwh"] = round(d["rt_mwh"], 4)
        d["rt_imbalance"] = round(d["rt_imbalance"], 2)
        d["rt_sales_mwh"] = round(d.get("rt_sales_mwh", 0), 4)
        d["rt_sales_revenue"] = round(d.get("rt_sales_revenue", 0), 2)
        d["rt_purchase_mwh"] = round(d.get("rt_purchase_mwh", 0), 4)
        d["rt_purchase_cost"] = round(d.get("rt_purchase_cost", 0), 2)
        if d["da_mwh"] > 0:
            d["avg_da_price"] = round(d["da_lmp_product"] / d["da_mwh"], 2)
        else:
            d["avg_da_price"] = None
        if d["volume"] > 0:
            d["avg_rt_price"] = round(d["rt_lmp_product"] / d["volume"], 2)
            d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        else:
            d["avg_rt_price"] = None
            d["gwa_basis"] = None
        hub_vol = d.get("hub_volume", 0)
        if hub_vol > 0:
            d["avg_hub_price"] = round(d.get("hub_lmp_product", 0) / hub_vol, 2)
        else:
            d["avg_hub_price"] = None

        # Calculate PPA settlement for NWOH
        gen_mwh = d["volume"]
        avg_hub = d.get("avg_hub_price") or d.get("avg_rt_price") or 0
        if gen_mwh > 0:
            d["ppa_fixed_payment"] = round(gen_mwh * PPA_PRICE, 2)
            d["ppa_floating_payment"] = round(gen_mwh * avg_hub, 2)
            d["ppa_net_settlement"] = round(d["ppa_fixed_payment"] - d["ppa_floating_payment"], 2)
            d["pnl"] = round(pjm_gross + d["ppa_net_settlement"], 2)
            d["realized_price"] = round(d["pnl"] / gen_mwh, 2)
        else:
            d["ppa_fixed_payment"] = 0
            d["ppa_floating_payment"] = 0
            d["ppa_net_settlement"] = 0
            d["pnl"] = round(pjm_gross, 2)
            d["realized_price"] = None
            d["avg_hub_price"] = None

    total_pnl = sum(d["pnl"] for d in daily.values())
    total_volume = sum(d["volume"] for d in daily.values())
    total_da_mwh = sum(d["da_mwh"] for d in daily.values())

    logger.info(f"Pharos unit ops aggregation: PnL=${total_pnl:.2f}, Volume={total_volume:.2f} MWh, DA={total_da_mwh:.2f} MWh")

    return {
        "daily": dict(daily),
        "monthly": dict(monthly),
        "annual": dict(annual),
        "total_pnl": round(total_pnl, 2),
        "total_volume": round(total_volume, 4),
        "total_da_mwh": round(total_da_mwh, 2),
        "record_count": len(ops),
    }

def save_pharos_data(data):
    """Save Pharos/NWOH data to JSON file."""
    try:
        with open(PHAROS_HISTORY_FILE, 'w') as f:
            json.dump(data, f, default=str)
        logger.info(f"Saved Pharos data to {PHAROS_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving Pharos data: {e}")

def load_pharos_data():
    """Load Pharos/NWOH data from JSON file."""
    try:
        if os.path.exists(PHAROS_HISTORY_FILE):
            with open(PHAROS_HISTORY_FILE, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded Pharos data from {PHAROS_HISTORY_FILE}: {len(data.get('daily_pnl', {}))} daily records, PnL=${data.get('total_pnl', 0):,.0f}")
            return data
        else:
            logger.info(f"Pharos cache file {PHAROS_HISTORY_FILE} does not exist")
        return None
    except Exception as e:
        logger.error(f"Error loading Pharos data: {e}")
        return None

# Global storage for Pharos/NWOH data
pharos_data = {
    "da_awards": [],           # Raw DA awards from Pharos
    "daily_da": {},            # Aggregated by day (DA only)
    "monthly_da": {},          # Aggregated by month (DA only)
    "annual_da": {},           # Aggregated by year (DA only)
    "total_da_mwh": 0,
    "total_da_revenue": 0,
    "capped_intervals": [],    # Price-capped intervals for alerting
    # Unit operations data (combined DA+RT for PnL)
    "unit_ops": [],            # Raw unit operations from Pharos
    "daily_pnl": {},           # PnL by day (includes DA rev + RT imbalance)
    "monthly_pnl": {},         # PnL by month
    "annual_pnl": {},          # PnL by year
    "total_pnl": 0,
    "total_volume": 0,
    "last_pharos_update": None,
}

# Historical NWOH data file (imported from Excel)
NWOH_HISTORICAL_FILE = 'nwoh_historical_data.json'

def load_nwoh_historical_data():
    """
    Load historical NWOH data from JSON file (imported from Excel reports).
    This provides data for months not yet available in Pharos API.
    """
    try:
        if os.path.exists(NWOH_HISTORICAL_FILE):
            with open(NWOH_HISTORICAL_FILE, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded NWOH historical data: {len(data.get('daily_pnl', {}))} days")
            return data
        else:
            logger.info(f"No NWOH historical data file found at {NWOH_HISTORICAL_FILE}")
            return None
    except Exception as e:
        logger.error(f"Error loading NWOH historical data: {e}")
        return None

def merge_nwoh_historical_with_pharos():
    """
    Merge historical NWOH data (from Excel) with current Pharos API data.
    Historical data is used for dates not available in Pharos.
    Pharos data takes precedence for overlapping dates (more up-to-date).
    """
    global pharos_data

    historical = load_nwoh_historical_data()
    if not historical:
        return

    # Merge daily_pnl - historical first, then Pharos overwrites
    merged_daily = {}
    merged_monthly = {}
    merged_annual = {}

    # Add historical data
    for date_key, day_data in historical.get('daily_pnl', {}).items():
        merged_daily[date_key] = day_data

    for month_key, month_data in historical.get('monthly_pnl', {}).items():
        merged_monthly[month_key] = month_data

    for year_key, year_data in historical.get('annual_pnl', {}).items():
        merged_annual[year_key] = year_data

    # Overwrite with Pharos data (more current)
    for date_key, day_data in pharos_data.get('daily_pnl', {}).items():
        merged_daily[date_key] = day_data

    # Recalculate monthly/annual totals from merged daily
    recalc_monthly = {}
    recalc_annual = {}

    for date_key, day_data in merged_daily.items():
        month_key = date_key[:7]  # YYYY-MM
        year_key = date_key[:4]   # YYYY

        if month_key not in recalc_monthly:
            recalc_monthly[month_key] = {
                'pnl': 0, 'volume': 0, 'da_mwh': 0, 'da_revenue': 0, 'rt_revenue': 0,
                'rt_sales_revenue': 0, 'rt_purchase_cost': 0, 'count': 0,
                'hub_product': 0, 'node_product': 0
            }
        if year_key not in recalc_annual:
            recalc_annual[year_key] = {
                'pnl': 0, 'volume': 0, 'da_mwh': 0, 'da_revenue': 0, 'rt_revenue': 0,
                'rt_sales_revenue': 0, 'rt_purchase_cost': 0, 'count': 0,
                'hub_product': 0, 'node_product': 0
            }

        vol = day_data.get('volume', 0)
        hub = day_data.get('avg_hub_price', 0) or 0
        node = day_data.get('avg_rt_price', 0) or 0

        for period_data in [recalc_monthly[month_key], recalc_annual[year_key]]:
            period_data['pnl'] += day_data.get('pnl', 0)
            period_data['volume'] += vol
            period_data['da_mwh'] += day_data.get('da_mwh', 0)
            period_data['da_revenue'] += day_data.get('da_revenue', 0)
            period_data['rt_revenue'] += day_data.get('rt_revenue', 0)
            period_data['rt_sales_revenue'] += day_data.get('rt_sales_revenue', 0)
            period_data['rt_purchase_cost'] += day_data.get('rt_purchase_cost', 0)
            period_data['count'] += 1
            if hub and vol > 0:
                period_data['hub_product'] += vol * hub
                period_data['node_product'] += vol * node

    # Calculate averages for monthly/annual
    for period_data in list(recalc_monthly.values()) + list(recalc_annual.values()):
        vol = period_data.get('volume', 0)
        pnl = period_data.get('pnl', 0)
        if vol > 0 and period_data.get('hub_product', 0) > 0:
            period_data['avg_hub_price'] = round(period_data['hub_product'] / vol, 2)
            period_data['gwa_basis'] = round((period_data['hub_product'] - period_data['node_product']) / vol, 2)
        # Realized price = PnL / Volume (NOT $33.31 + basis)
        if vol > 0:
            period_data['realized_price'] = round(pnl / vol, 2)
        period_data['pnl'] = round(pnl, 2)
        period_data['volume'] = round(vol, 2)
        # Clean up temp fields
        period_data.pop('hub_product', None)
        period_data.pop('node_product', None)

    # Update pharos_data with merged data (use data_lock for thread safety)
    total_pnl = round(sum(d.get('pnl', 0) for d in merged_daily.values()), 2)
    total_volume = round(sum(d.get('volume', 0) for d in merged_daily.values()), 2)
    with data_lock:
        pharos_data['daily_pnl'] = merged_daily
        pharos_data['monthly_pnl'] = recalc_monthly
        pharos_data['annual_pnl'] = recalc_annual
        pharos_data['total_pnl'] = total_pnl
        pharos_data['total_volume'] = total_volume

    logger.info(f"Merged NWOH data: {len(merged_daily)} total days, ${total_pnl:,.2f} total PnL")

# ============================================================================
# ERCOT Helper Functions
# ============================================================================
def get_historical_prices(hours_back=4):
    try:
        cst_tz = ZoneInfo("US/Central")
        today_cst = datetime.now(cst_tz).date()
        
        logger.info(f"Fetching ERCOT data for {today_cst}")
        ercot = Ercot()
        lmp_data = ercot.get_lmp(date=str(today_cst), location_type="settlement point")
        
        if lmp_data is None or len(lmp_data) == 0:
            logger.warning(f"No LMP data available for {today_cst}")
            return []
        
        node1_data = lmp_data[lmp_data['Location'] == NODE_1].copy()
        node2_data = lmp_data[lmp_data['Location'] == NODE_2].copy()
        hub_data = lmp_data[lmp_data['Location'] == HUB].copy()
        
        if len(node1_data) == 0 or len(node2_data) == 0 or len(hub_data) == 0:
            logger.warning(f"No data found for nodes {NODE_1}, {NODE_2}, or {HUB}")
            return []
        
        merged = node1_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_1_LMP'}).merge(
            node2_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_2_LMP'}),
            on='Interval Start'
        ).merge(
            hub_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'HUB_LMP'}),
            on='Interval Start'
        )
        
        merged['BASIS_1'] = merged['NODE_1_LMP'] - merged['HUB_LMP']
        merged['BASIS_2'] = merged['NODE_2_LMP'] - merged['HUB_LMP']
        merged = merged.sort_values('Interval Start')
        
        cutoff_time = datetime.now(cst_tz) - __import__('datetime').timedelta(hours=hours_back)
        merged['Interval Start'] = __import__('pandas').to_datetime(merged['Interval Start'])
        merged = merged[merged['Interval Start'] >= cutoff_time]
        
        logger.info(f"Fetched {len(merged)} historical data points for {NODE_1}, {NODE_2} vs {HUB}")
        
        history = []
        for _, row in merged.iterrows():
            basis1 = row['BASIS_1']
            basis2 = row['BASIS_2']
            status1 = "safe" if basis1 > 0 else ("caution" if basis1 >= -100 else "alert")
            status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -30 else "alert")
            history.append({
                'time': row['Interval Start'],
                'node1_price': round(float(row['NODE_1_LMP']), 2),
                'node2_price': round(float(row['NODE_2_LMP']), 2),
                'hub_price': round(float(row['HUB_LMP']), 2),
                'basis1': round(float(basis1), 2),
                'basis2': round(float(basis2), 2),
                'status1': status1,
                'status2': status2
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return []

def background_data_fetch():
    global last_basis_time, last_pjm_time, latest_data, pharos_data

    # Fetch initial ERCOT data
    logger.info("Fetching initial ERCOT historical data...")
    initial_history = get_historical_prices()

    logger.info(f"Got {len(initial_history)} ERCOT points")
    if initial_history:
        logger.info(f"First ERCOT point: {initial_history[0]}")
        logger.info(f"Last ERCOT point: {initial_history[-1]}")

    # Load existing PJM data from file
    logger.info("Loading existing PJM historical data from file...")
    stored_pjm_history = load_pjm_history()

    # Fetch fresh PJM data from API (get 24 hours to build better initial chart)
    logger.info("Fetching fresh PJM historical data from API...")
    fresh_pjm_history = get_pjm_lmp_data(hours_back=24)

    # Merge stored and fresh data, avoiding duplicates
    if stored_pjm_history:
        # Create set of existing timestamps
        existing_times = {point['time'] for point in stored_pjm_history}
        # Add only new points from fresh data
        for point in fresh_pjm_history:
            if point['time'] not in existing_times:
                stored_pjm_history.append(point)
        # Sort by time
        stored_pjm_history.sort(key=lambda x: x['time'])
        # Keep last 2000 points (about 7 days of 5-min data)
        stored_pjm_history = stored_pjm_history[-2000:]
        initial_pjm_history = stored_pjm_history
        logger.info(f"Merged PJM data: {len(stored_pjm_history)} total points")
    else:
        initial_pjm_history = fresh_pjm_history
        logger.info(f"No stored history, using {len(fresh_pjm_history)} fresh points")

    # Save merged history
    if initial_pjm_history:
        save_pjm_history(initial_pjm_history)
        logger.info(f"First PJM point: {initial_pjm_history[0]}")
        logger.info(f"Last PJM point: {initial_pjm_history[-1]}")

    with data_lock:
        # Update ERCOT data
        latest_data["history"] = initial_history
        if initial_history:
            last_point = initial_history[-1]
            latest_data["node1_price"] = last_point['node1_price']
            latest_data["node2_price"] = last_point['node2_price']
            latest_data["hub_price"] = last_point['hub_price']
            latest_data["basis1"] = last_point['basis1']
            latest_data["basis2"] = last_point['basis2']
            latest_data["status1"] = last_point['status1']
            latest_data["status2"] = last_point['status2']
            latest_data["data_time"] = str(last_point['time'])
            last_basis_time = last_point['time']
            logger.info(f"Updated ERCOT latest_data: node1=${latest_data['node1_price']}, basis1=${latest_data['basis1']}")

        # Update PJM data
        latest_data["pjm_history"] = initial_pjm_history
        if initial_pjm_history:
            last_pjm_point = initial_pjm_history[-1]
            latest_data["pjm_node_price"] = last_pjm_point['node_price']
            latest_data["pjm_hub_price"] = last_pjm_point['hub_price']
            latest_data["pjm_basis"] = last_pjm_point['basis']
            latest_data["pjm_status"] = last_pjm_point['status']
            last_pjm_time = last_pjm_point['time']
            logger.info(f"Updated PJM latest_data: node=${latest_data['pjm_node_price']}, basis=${latest_data['pjm_basis']}")

        latest_data["last_update"] = datetime.now().isoformat()

    logger.info(f"Loaded {len(initial_history)} ERCOT + {len(initial_pjm_history)} PJM historical data points")

    # Load Pharos/NWOH data FIRST (before heavy Tenaska load)
    # This ensures NWOH is available quickly even if Tenaska times out
    last_pharos_fetch_time = None
    logger.info("Loading initial Pharos/NWOH data...")
    try:
        cached_pharos = load_pharos_data()
        # Check if cache has actual data (not just empty structure)
        cache_has_data = cached_pharos and cached_pharos.get("total_pnl", 0) != 0

        if cache_has_data:
            with data_lock:
                pharos_data.update(cached_pharos)
            logger.info(f"Loaded cached Pharos data: total_pnl=${pharos_data.get('total_pnl', 0):,.0f}, volume={pharos_data.get('total_volume', 0):,.0f} MWh")
            # Also merge historical data from Excel (for months not in API cache)
            merge_nwoh_historical_with_pharos()
            last_pharos_fetch_time = datetime.now()  # Don't immediately re-fetch if cache is valid
        else:
            # Fetch fresh data on first run or if cache is empty
            if PHAROS_AUTO_FETCH:
                logger.info("No cached Pharos data found. Fetching from API (first run)...")
                # Fetch DA awards
                awards = fetch_pharos_da_awards(start_date=PHAROS_FETCH_START_DATE)
                if awards:
                    aggregated = aggregate_pharos_da_data(awards)
                    with data_lock:
                        pharos_data["da_awards"] = awards
                        pharos_data["daily_da"] = aggregated["daily"]
                        pharos_data["monthly_da"] = aggregated["monthly"]
                        pharos_data["annual_da"] = aggregated["annual"]
                        pharos_data["total_da_mwh"] = aggregated["total_da_mwh"]
                        pharos_data["total_da_revenue"] = aggregated["total_da_revenue"]
                        pharos_data["capped_intervals"] = aggregated["capped_intervals"]

                # Fetch PnL data using combined endpoint (market_results + power_meter + lmp)
                logger.info("Fetching Pharos combined PnL data...")
                unit_ops = fetch_pharos_hourly_revenue(start_date=PHAROS_FETCH_START_DATE)

                if unit_ops:
                    ops_aggregated = aggregate_pharos_unit_operations(unit_ops)
                    with data_lock:
                        pharos_data["unit_ops"] = unit_ops
                        pharos_data["daily_pnl"] = ops_aggregated["daily"]
                        pharos_data["monthly_pnl"] = ops_aggregated["monthly"]
                        pharos_data["annual_pnl"] = ops_aggregated["annual"]
                        pharos_data["total_pnl"] = ops_aggregated["total_pnl"]
                        pharos_data["total_volume"] = ops_aggregated["total_volume"]
                    logger.info(f"Pharos PnL loaded: ${ops_aggregated['total_pnl']:,.0f}, {ops_aggregated['total_volume']:,.0f} MWh")

                with data_lock:
                    pharos_data["last_pharos_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()

                # Merge historical NWOH data (from Excel) with Pharos data
                merge_nwoh_historical_with_pharos()

                save_pharos_data(pharos_data)
                last_pharos_fetch_time = datetime.now()
    except Exception as e:
        logger.error(f"Error loading Pharos data: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Always ensure historical NWOH data is merged, even if Pharos API failed
    # This guarantees Jan+ data is available from the Excel-imported JSON
    if not pharos_data.get("daily_pnl"):
        logger.info("No Pharos API data available, loading historical NWOH data as fallback...")
        merge_nwoh_historical_with_pharos()

    # Load PnL data (ERCOT/Tenaska) - this is slower, happens after Pharos
    last_tenaska_fetch_time = None

    def refresh_pnl_data(source="auto"):
        """
        Refresh PnL data from API or Excel.
        source: "api", "excel", or "auto" (try API first, then Excel)
        """
        nonlocal last_tenaska_fetch_time
        records = []
        hub_prices = {}

        if source in ("api", "auto") and TENASKA_AUTO_FETCH:
            logger.info(f"Fetching PnL data from Tenaska API (start_date={TENASKA_FETCH_START_DATE})...")
            records = fetch_energy_imbalance_data(start_date=TENASKA_FETCH_START_DATE)
            if records:
                logger.info(f"Successfully fetched {len(records)} records from Tenaska API")
                # Also fetch hub prices for basis calculation
                logger.info("Fetching hub prices from Tenaska API...")
                hub_prices = fetch_hub_prices(start_date=TENASKA_FETCH_START_DATE)
                last_tenaska_fetch_time = datetime.now()

        # Fall back to Excel if API returned nothing
        if not records and source in ("excel", "auto"):
            logger.info("Loading PnL data from Excel file...")
            records = load_energy_imbalance_from_excel()
            if records:
                logger.info(f"Loaded {len(records)} records from Excel file")
                # Try to fetch hub prices for basis calculation
                if TENASKA_AUTO_FETCH and not hub_prices:
                    logger.info("Fetching hub prices from Tenaska API for Excel data...")
                    hub_prices = fetch_hub_prices(start_date=TENASKA_FETCH_START_DATE)

        if records:
            # Pass hub prices for basis calculation
            aggregated = aggregate_excel_pnl(records, hub_prices=hub_prices)
            with data_lock:
                pnl_data["daily_pnl"] = aggregated["daily"]
                pnl_data["monthly_pnl"] = aggregated["monthly"]
                pnl_data["annual_pnl"] = aggregated["annual"]
                pnl_data["total_pnl"] = aggregated["total_pnl"]
                pnl_data["total_volume"] = aggregated["total_volume"]
                pnl_data["record_count"] = aggregated["record_count"]
                pnl_data["assets"] = aggregated.get("assets", {})
                pnl_data["worst_basis_intervals"] = aggregated.get("worst_basis_intervals", [])
                pnl_data["last_tenaska_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()
            save_pnl_data(pnl_data)
            assets_loaded = list(aggregated.get("assets", {}).keys())
            logger.info(f"PnL data updated: {aggregated['record_count']} records, total_pnl=${aggregated['total_pnl']}, assets={assets_loaded}")
            return True
        return False

    logger.info("Loading initial PnL data...")
    try:
        # First try to load from cached JSON for quick startup
        cached_pnl = load_pnl_data()
        if cached_pnl:
            with data_lock:
                pnl_data.update(cached_pnl)
            logger.info(f"Loaded cached PnL data: total_pnl=${pnl_data.get('total_pnl', 0)}")
            # Skip initial API refresh for fast startup - the while loop will refresh periodically
            # Set last_tenaska_fetch_time to None so the while loop refreshes on first iteration
            last_tenaska_fetch_time = None
            logger.info("Using cached data for fast startup. API refresh will happen in background loop.")
        else:
            # No cache, must load fresh data on first run
            logger.info("No cached PnL data found. Fetching from API (first run)...")
            refresh_pnl_data(source="auto")
    except Exception as e:
        logger.error(f"Error loading PnL data: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Signal that initial data is ready
    logger.info("Initial data ready, entering update loop")
    
    while True:
        try:
            # Fetch ERCOT data
            ercot = Ercot()
            lmp_data = ercot.get_lmp(date="latest", location_type="settlement point")

            if lmp_data is not None and len(lmp_data) > 0:
                latest_time = lmp_data['Interval Start'].max()

                if latest_time != last_basis_time:
                    latest_data_df = lmp_data[lmp_data['Interval Start'] == latest_time]

                    node1_data = latest_data_df[latest_data_df['Location'] == NODE_1]
                    node2_data = latest_data_df[latest_data_df['Location'] == NODE_2]
                    hub_data = latest_data_df[latest_data_df['Location'] == HUB]

                    if len(node1_data) > 0 and len(node2_data) > 0 and len(hub_data) > 0:
                        node1_price = float(node1_data['LMP'].values[0])
                        node2_price = float(node2_data['LMP'].values[0])
                        hub_price = float(hub_data['LMP'].values[0])
                        basis1 = node1_price - hub_price
                        basis2 = node2_price - hub_price
                        status1 = "safe" if basis1 > 0 else ("caution" if basis1 >= -100 else "alert")
                        status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -30 else "alert")

                        new_point = {
                            'time': latest_time,
                            'node1_price': round(node1_price, 2),
                            'node2_price': round(node2_price, 2),
                            'hub_price': round(hub_price, 2),
                            'basis1': round(basis1, 2),
                            'basis2': round(basis2, 2),
                            'status1': status1,
                            'status2': status2
                        }

                        with data_lock:
                            latest_data["node1_price"] = new_point['node1_price']
                            latest_data["node2_price"] = new_point['node2_price']
                            latest_data["hub_price"] = new_point['hub_price']
                            latest_data["basis1"] = new_point['basis1']
                            latest_data["basis2"] = new_point['basis2']
                            latest_data["last_update"] = datetime.now().isoformat()
                            latest_data["data_time"] = str(latest_time)
                            latest_data["status1"] = status1
                            latest_data["status2"] = status2
                            latest_data["history"].append(new_point)
                            latest_data["history"] = latest_data["history"][-100:]

                        last_basis_time = latest_time
                        logger.info(f"ERCOT update: {NODE_1}=${new_point['node1_price']}, {NODE_2}=${new_point['node2_price']}, Basis1=${new_point['basis1']}, Basis2=${new_point['basis2']}")
            else:
                logger.warning("No ERCOT real-time data available")

            # Fetch PJM data (fetches last 1 hour to get latest)
            pjm_data = get_pjm_lmp_data(hours_back=1)
            if pjm_data and len(pjm_data) > 0:
                # Get the most recent PJM point
                latest_pjm_point = pjm_data[-1]
                latest_pjm_time = latest_pjm_point['time']

                if latest_pjm_time != last_pjm_time:
                    with data_lock:
                        latest_data["pjm_node_price"] = latest_pjm_point['node_price']
                        latest_data["pjm_hub_price"] = latest_pjm_point['hub_price']
                        latest_data["pjm_basis"] = latest_pjm_point['basis']
                        latest_data["pjm_status"] = latest_pjm_point['status']
                        latest_data["pjm_history"].append(latest_pjm_point)
                        # Keep last 2000 points (about 7 days of 5-min data)
                        latest_data["pjm_history"] = latest_data["pjm_history"][-2000:]
                        latest_data["last_update"] = datetime.now().isoformat()

                        # Save updated history to file
                        save_pjm_history(latest_data["pjm_history"])

                    last_pjm_time = latest_pjm_time
                    logger.info(f"PJM update: {PJM_NODE}=${latest_pjm_point['node_price']}, {PJM_HUB}=${latest_pjm_point['hub_price']}, Basis=${latest_pjm_point['basis']}")
            else:
                # Fallback: Try to get latest data from Pharos unit_operations
                logger.warning("No PJM API data available, trying Pharos fallback...")
                try:
                    today = datetime.now().strftime("%Y-%m-%d")
                    url = f"{PHAROS_BASE_URL}/pjm/unit_operations/historic"
                    params = {
                        "organization_key": PHAROS_ORGANIZATION_KEY,
                        "start_date": today,
                        "end_date": today,
                    }
                    pharos_resp = requests.get(url, auth=get_pharos_auth(), params=params, timeout=30)
                    if pharos_resp.status_code == 200:
                        pharos_resp_data = pharos_resp.json()
                        # Store unit_operations without overwriting the entire pharos_data dict
                        if isinstance(pharos_resp_data, dict) and "unit_operations" in pharos_resp_data:
                            pharos_data["unit_operations"] = pharos_resp_data["unit_operations"]
                        ops = pharos_resp_data.get("unit_operations", pharos_resp_data) if isinstance(pharos_resp_data, dict) else pharos_resp_data
                        if ops:
                            # Get most recent record
                            latest_op = ops[-1]
                            rt_lmp = float(latest_op.get("rt_lmp", 0) or 0)
                            timestamp = latest_op.get("timestamp", "")

                            # Try to get hub price from cache
                            hub_lmp = get_hub_price_for_timestamp(timestamp)
                            if hub_lmp is None:
                                # Use most recent hub price from cache
                                if pjm_hub_price_cache:
                                    hub_lmp = list(pjm_hub_price_cache.values())[-1]

                            if hub_lmp:
                                basis = hub_lmp - rt_lmp
                                status = "safe" if basis > 0 else ("caution" if basis >= -30 else "alert")
                                with data_lock:
                                    latest_data["pjm_node_price"] = round(rt_lmp, 2)
                                    latest_data["pjm_hub_price"] = round(hub_lmp, 2)
                                    latest_data["pjm_basis"] = round(basis, 2)
                                    latest_data["pjm_status"] = status
                                    latest_data["last_update"] = datetime.now().isoformat()
                                logger.info(f"PJM Pharos fallback: node=${rt_lmp:.2f}, hub=${hub_lmp:.2f}, basis=${basis:.2f}")
                except Exception as e:
                    logger.error(f"Pharos fallback failed: {e}")

            # Periodic Tenaska API refresh for PnL data
            if TENASKA_AUTO_FETCH:
                should_refresh = False
                if last_tenaska_fetch_time is None:
                    should_refresh = True
                else:
                    seconds_since_fetch = (datetime.now() - last_tenaska_fetch_time).total_seconds()
                    if seconds_since_fetch >= TENASKA_FETCH_INTERVAL:
                        should_refresh = True

                if should_refresh:
                    logger.info("Refreshing PnL data from Tenaska API...")
                    try:
                        refresh_pnl_data(source="api")
                    except Exception as e:
                        logger.error(f"Error refreshing Tenaska PnL data: {e}")

            # Periodic Pharos API refresh for NWOH DA data
            if PHAROS_AUTO_FETCH:
                should_refresh_pharos = False
                if last_pharos_fetch_time is None:
                    should_refresh_pharos = True
                else:
                    seconds_since_pharos = (datetime.now() - last_pharos_fetch_time).total_seconds()
                    if seconds_since_pharos >= PHAROS_FETCH_INTERVAL:
                        should_refresh_pharos = True

                if should_refresh_pharos:
                    logger.info("Refreshing Pharos/NWOH data...")
                    try:
                        # Fetch DA awards
                        awards = fetch_pharos_da_awards(start_date=PHAROS_FETCH_START_DATE)
                        if awards:
                            aggregated = aggregate_pharos_da_data(awards)
                            with data_lock:
                                pharos_data["da_awards"] = awards
                                pharos_data["daily_da"] = aggregated["daily"]
                                pharos_data["monthly_da"] = aggregated["monthly"]
                                pharos_data["annual_da"] = aggregated["annual"]
                                pharos_data["total_da_mwh"] = aggregated["total_da_mwh"]
                                pharos_data["total_da_revenue"] = aggregated["total_da_revenue"]
                                pharos_data["capped_intervals"] = aggregated["capped_intervals"]

                        # Fetch PnL data using combined endpoint (market_results + power_meter + lmp)
                        unit_ops = fetch_pharos_hourly_revenue(start_date=PHAROS_FETCH_START_DATE)

                        if unit_ops:
                            ops_aggregated = aggregate_pharos_unit_operations(unit_ops)
                            with data_lock:
                                pharos_data["unit_ops"] = unit_ops
                                pharos_data["daily_pnl"] = ops_aggregated["daily"]
                                pharos_data["monthly_pnl"] = ops_aggregated["monthly"]
                                pharos_data["annual_pnl"] = ops_aggregated["annual"]
                                pharos_data["total_pnl"] = ops_aggregated["total_pnl"]
                                pharos_data["total_volume"] = ops_aggregated["total_volume"]

                        with data_lock:
                            pharos_data["last_pharos_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()

                        # Merge historical NWOH data (from Excel) with Pharos data
                        merge_nwoh_historical_with_pharos()

                        save_pharos_data(pharos_data)
                        last_pharos_fetch_time = datetime.now()
                        logger.info(f"Pharos refresh complete: PnL=${pharos_data.get('total_pnl', 0)}, DA={pharos_data.get('total_da_mwh', 0)} MWh")
                    except Exception as e:
                        logger.error(f"Error refreshing Pharos data: {e}")

            time.sleep(120)

        except Exception as e:
            logger.error(f"Error in background fetch: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Keep last known good data instead of setting status to error
            time.sleep(60)

# Flag to track if background thread has started in this process
_background_thread_started = False
_cache_loaded = False
_cache_lock = threading.Lock()

def load_caches_if_needed():
    """Load cached data files for immediate availability (before background thread finishes)."""
    global _cache_loaded
    if _cache_loaded:
        return
    # Use double-checked locking to prevent race condition with multi-threaded gunicorn:
    # Without this, thread 2 could see _cache_loaded=True before thread 1 finishes loading,
    # causing /api/pnl to return empty pharos_data (NWOH shows N/A).
    with _cache_lock:
        if _cache_loaded:
            return
        try:
            cached_pnl = load_pnl_data()
            if cached_pnl:
                with data_lock:
                    pnl_data.update(cached_pnl)
                logger.info(f"Pre-loaded PnL cache: total_pnl=${pnl_data.get('total_pnl', 0):,.0f}")
            cached_pharos = load_pharos_data()
            if cached_pharos and cached_pharos.get("total_pnl", 0) != 0:
                with data_lock:
                    pharos_data.update(cached_pharos)
                logger.info(f"Pre-loaded Pharos cache: total_pnl=${pharos_data.get('total_pnl', 0):,.0f}")
                merge_nwoh_historical_with_pharos()
            elif os.path.exists(NWOH_HISTORICAL_FILE):
                merge_nwoh_historical_with_pharos()
                logger.info("Pre-loaded NWOH historical data as fallback")
        except Exception as e:
            logger.error(f"Error pre-loading caches: {e}")
        _cache_loaded = True

def start_background_thread_if_needed():
    """Start the background thread if not already running in this process."""
    global _background_thread_started
    if not _background_thread_started:
        _background_thread_started = True
        fetch_thread = threading.Thread(target=background_data_fetch, daemon=True)
        fetch_thread.start()
        logger.info(f"Background data fetch thread started in process {os.getpid()}")

@app.before_request
def ensure_data_loaded():
    """Ensure caches are loaded and background thread is running for every worker."""
    load_caches_if_needed()
    start_background_thread_if_needed()

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == DASHBOARD_PASSWORD:
            session['authenticated'] = True
            return redirect('/')
        else:
            return '''<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); color: #0E2C51; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
                <div style="background: white; padding: 48px; border-radius: 2px; border: 1px solid #e5e5e5; box-shadow: 0 2px 8px rgba(0,0,0,0.08); max-width: 400px; width: 100%;">
                    <h1 style="margin-top: 0; font-family: Georgia, serif; color: #0E2C51; font-weight: 700; letter-spacing: -0.02em;">ERCOT Basis Tracker</h1>
                    <p style="color: #ef4444; margin-bottom: 24px; font-size: 14px; background: #ffebee; padding: 12px; border-radius: 2px; border-left: 3px solid #ef4444;">Invalid password. Try again.</p>
                    <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 12px; border: 1px solid #d0d0d0; border-radius: 2px; background: #ffffff; color: #0E2C51; width: 100%; box-sizing: border-box; margin-bottom: 12px; font-size: 14px;">
                    <button type="submit" style="padding: 12px; background: #2291EB; color: white; border: none; border-radius: 2px; cursor: pointer; width: 100%; font-weight: 600; font-size: 14px; transition: background 0.2s;" onmouseover="this.style.background='#0E2C51'" onmouseout="this.style.background='#2291EB'">Login</button></form>
                </div></body></html>'''

    return '''<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); color: #0E2C51; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
        <div style="background: white; padding: 48px; border-radius: 2px; border: 1px solid #e5e5e5; box-shadow: 0 2px 8px rgba(0,0,0,0.08); max-width: 400px; width: 100%;">
            <h1 style="margin-top: 0; font-family: Georgia, serif; color: #0E2C51; font-weight: 700; letter-spacing: -0.02em;">ERCOT Basis Tracker</h1>
            <p style="color: #666; margin-bottom: 24px; font-size: 14px;">Enter the password to access the dashboard</p>
            <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 12px; border: 1px solid #d0d0d0; border-radius: 2px; background: #ffffff; color: #0E2C51; width: 100%; box-sizing: border-box; margin-bottom: 12px; font-size: 14px;">
            <button type="submit" style="padding: 12px; background: #2291EB; color: white; border: none; border-radius: 2px; cursor: pointer; width: 100%; font-weight: 600; font-size: 14px; transition: background 0.2s;" onmouseover="this.style.background='#0E2C51'" onmouseout="this.style.background='#2291EB'">Login</button></form>
        </div></body></html>'''

@app.route('/api/basis', methods=['GET'])
@login_required
def get_basis():
    # Ensure background thread is running in this worker process
    start_background_thread_if_needed()

    with data_lock:
        # Return current state (includes both ERCOT and PJM data)
        logger.info(f"API called - ERCOT: node1=${latest_data['node1_price']}, basis1=${latest_data['basis1']}, history={len(latest_data['history'])} | PJM: node=${latest_data['pjm_node_price']}, basis=${latest_data['pjm_basis']}, history={len(latest_data['pjm_history'])}")
        return jsonify(latest_data)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "ercot_status1": latest_data["status1"],
        "ercot_status2": latest_data["status2"],
        "pjm_status": latest_data["pjm_status"]
    })

# ============================================================================
# PnL API ENDPOINTS
# ============================================================================
@app.route('/api/pnl', methods=['GET'])
@login_required
def get_pnl():
    """Get PnL summary and aggregated data including per-asset breakdown."""
    with data_lock:
        # Merge Tenaska and Pharos assets
        assets = dict(pnl_data.get("assets", {}))

        # Debug: log pharos_data state at request time
        pharos_daily = pharos_data.get("daily_pnl")
        logger.info(f"[/api/pnl] pharos_data keys: {list(pharos_data.keys())}, daily_pnl truthy: {bool(pharos_daily)}, daily_pnl count: {len(pharos_daily) if pharos_daily else 0}, total_pnl: {pharos_data.get('total_pnl', 'MISSING')}")

        # Add NWOH from Pharos data
        if pharos_data.get("daily_pnl"):
            assets["NWOH"] = {
                "total_pnl": pharos_data.get("total_pnl", 0),
                "total_volume": pharos_data.get("total_volume", 0),
                "daily_pnl": pharos_data.get("daily_pnl", {}),
                "monthly_pnl": pharos_data.get("monthly_pnl", {}),
                "annual_pnl": pharos_data.get("annual_pnl", {}),
                "gwa_basis": None,  # Will be calculated from daily data
            }
            # Calculate overall GWA basis for NWOH
            total_vol = pharos_data.get("total_volume", 0)
            if total_vol > 0:
                total_vbp = sum(d.get("volume_basis_product", 0) for d in pharos_data.get("daily_pnl", {}).values())
                assets["NWOH"]["gwa_basis"] = round(total_vbp / total_vol, 2)

        # Calculate combined totals
        combined_total_pnl = pnl_data.get("total_pnl", 0) + pharos_data.get("total_pnl", 0)
        combined_total_volume = pnl_data.get("total_volume", 0) + pharos_data.get("total_volume", 0)

        # Merge daily/monthly/annual aggregates (deep copy to avoid mutating pnl_data)
        daily_pnl = {k: dict(v) for k, v in pnl_data.get("daily_pnl", {}).items()}
        monthly_pnl = {k: dict(v) for k, v in pnl_data.get("monthly_pnl", {}).items()}
        annual_pnl = {k: dict(v) for k, v in pnl_data.get("annual_pnl", {}).items()}

        # Add Pharos daily data to combined totals
        for day, data in pharos_data.get("daily_pnl", {}).items():
            if day in daily_pnl:
                daily_pnl[day]["pnl"] = daily_pnl[day].get("pnl", 0) + data.get("pnl", 0)
                daily_pnl[day]["volume"] = daily_pnl[day].get("volume", 0) + data.get("volume", 0)
                daily_pnl[day]["count"] = daily_pnl[day].get("count", 0) + data.get("count", 0)
                daily_pnl[day]["volume_basis_product"] = daily_pnl[day].get("volume_basis_product", 0) + data.get("volume_basis_product", 0)
            else:
                daily_pnl[day] = dict(data)

        # Add Pharos monthly data
        for month, data in pharos_data.get("monthly_pnl", {}).items():
            if month in monthly_pnl:
                monthly_pnl[month]["pnl"] = monthly_pnl[month].get("pnl", 0) + data.get("pnl", 0)
                monthly_pnl[month]["volume"] = monthly_pnl[month].get("volume", 0) + data.get("volume", 0)
                monthly_pnl[month]["count"] = monthly_pnl[month].get("count", 0) + data.get("count", 0)
                monthly_pnl[month]["volume_basis_product"] = monthly_pnl[month].get("volume_basis_product", 0) + data.get("volume_basis_product", 0)
            else:
                monthly_pnl[month] = dict(data)

        # Add Pharos annual data
        for year, data in pharos_data.get("annual_pnl", {}).items():
            if year in annual_pnl:
                annual_pnl[year]["pnl"] = annual_pnl[year].get("pnl", 0) + data.get("pnl", 0)
                annual_pnl[year]["volume"] = annual_pnl[year].get("volume", 0) + data.get("volume", 0)
                annual_pnl[year]["count"] = annual_pnl[year].get("count", 0) + data.get("count", 0)
                annual_pnl[year]["volume_basis_product"] = annual_pnl[year].get("volume_basis_product", 0) + data.get("volume_basis_product", 0)
            else:
                annual_pnl[year] = dict(data)

        # Recalculate GWA basis for combined data
        for d in daily_pnl.values():
            if d.get("volume", 0) > 0 and "volume_basis_product" in d:
                d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        for d in monthly_pnl.values():
            if d.get("volume", 0) > 0 and "volume_basis_product" in d:
                d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)
        for d in annual_pnl.values():
            if d.get("volume", 0) > 0 and "volume_basis_product" in d:
                d["gwa_basis"] = round(d["volume_basis_product"] / d["volume"], 2)

        logger.info(f"[/api/pnl] Response assets: {list(assets.keys())}, combined_pnl: ${combined_total_pnl:,.0f}, NWOH in assets: {'NWOH' in assets}")

        return jsonify({
            "total_pnl": combined_total_pnl,
            "total_volume": combined_total_volume,
            "record_count": pnl_data.get("record_count", 0),
            "daily_pnl": daily_pnl,
            "monthly_pnl": monthly_pnl,
            "annual_pnl": annual_pnl,
            "assets": assets,
            "worst_basis_intervals": pnl_data.get("worst_basis_intervals", []),
            "last_update": pnl_data.get("last_tenaska_update"),
        })

@app.route('/api/pnl/status', methods=['GET'])
@login_required
def get_pnl_status():
    """Get Tenaska API configuration and connection status."""
    # Test token fetch
    token_status = "unknown"
    try:
        token = get_tenaska_token()
        token_status = "ok" if token else "failed"
    except Exception as e:
        token_status = f"error: {str(e)}"

    return jsonify({
        "tenaska_auto_fetch": TENASKA_AUTO_FETCH,
        "tenaska_fetch_interval_seconds": TENASKA_FETCH_INTERVAL,
        "tenaska_fetch_days_back": TENASKA_FETCH_DAYS_BACK,
        "tenaska_token_status": token_status,
        "excel_file_path": ENERGY_IMBALANCE_EXCEL,
        "excel_file_exists": os.path.exists(ENERGY_IMBALANCE_EXCEL),
        "last_update": pnl_data.get("last_tenaska_update"),
        "record_count": pnl_data.get("record_count", 0),
        "assets_configured": list(ASSET_CONFIG.keys()),
        "assets_loaded": list(pnl_data.get("assets", {}).keys()),
    })

@app.route('/api/pnl/assets', methods=['GET'])
@login_required
def get_asset_pnl():
    """Get per-asset PnL breakdown."""
    asset_filter = request.args.get('asset')  # Optional: filter by specific asset

    with data_lock:
        # Merge Tenaska and Pharos assets
        assets = dict(pnl_data.get("assets", {}))

        # Add NWOH from Pharos data
        if pharos_data.get("daily_pnl"):
            assets["NWOH"] = {
                "total_pnl": pharos_data.get("total_pnl", 0),
                "total_volume": pharos_data.get("total_volume", 0),
                "daily_pnl": pharos_data.get("daily_pnl", {}),
                "monthly_pnl": pharos_data.get("monthly_pnl", {}),
                "annual_pnl": pharos_data.get("annual_pnl", {}),
            }

        if asset_filter and asset_filter in assets:
            return jsonify({
                "asset": asset_filter,
                "data": assets[asset_filter]
            })

        return jsonify({
            "assets": assets,
            "asset_config": {k: {
                "display_name": v["display_name"],
                "ppa_percent": v["ppa_percent"],
                "merchant_percent": v["merchant_percent"],
                "ppa_price": v["ppa_price"],
            } for k, v in ASSET_CONFIG.items()}
        })

@app.route('/api/pnl/worst-basis', methods=['GET'])
@login_required
def get_worst_basis():
    """Get worst basis intervals for PPA exclusion tracking."""
    limit = request.args.get('limit', WORST_BASIS_DISPLAY_COUNT, type=int)
    asset_filter = request.args.get('asset')

    with data_lock:
        intervals = pnl_data.get("worst_basis_intervals", [])

        if asset_filter:
            intervals = [i for i in intervals if i.get("asset") == asset_filter]

        # Limit results
        intervals = intervals[:min(limit, WORST_BASIS_INTERVALS_TO_TRACK)]

        # Calculate total impact if all worst intervals were excluded
        total_excludable_impact = sum(abs(i.get("basis_pnl_impact", 0)) for i in intervals)

        return jsonify({
            "worst_intervals": intervals,
            "count": len(intervals),
            "max_excludable": WORST_BASIS_INTERVALS_TO_TRACK,
            "total_excludable_impact": round(total_excludable_impact, 2),
            "note": "PPA exclusion candidates from prior day only (Gen × Basis formula)"
        })

@app.route('/api/pnl/daily', methods=['GET'])
@login_required
def get_daily_pnl():
    """Get daily PnL data with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    with data_lock:
        daily = pnl_data.get("daily_pnl", {})

        if start_date or end_date:
            filtered = {}
            for date_key, data in daily.items():
                if start_date and date_key < start_date:
                    continue
                if end_date and date_key > end_date:
                    continue
                filtered[date_key] = data
            daily = filtered

        # Sort by date descending (most recent first)
        sorted_daily = dict(sorted(daily.items(), reverse=True))

        return jsonify({
            "daily_pnl": sorted_daily,
            "total_pnl": sum(d.get("pnl", 0) for d in sorted_daily.values()),
            "total_volume": sum(d.get("volume", 0) for d in sorted_daily.values()),
            "count": len(sorted_daily)
        })

@app.route('/api/pnl/monthly', methods=['GET'])
@login_required
def get_monthly_pnl():
    """Get monthly PnL data."""
    with data_lock:
        monthly = pnl_data.get("monthly_pnl", {})
        sorted_monthly = dict(sorted(monthly.items(), reverse=True))

        return jsonify({
            "monthly_pnl": sorted_monthly,
            "total_pnl": sum(d.get("pnl", 0) for d in sorted_monthly.values()),
            "total_volume": sum(d.get("volume", 0) for d in sorted_monthly.values()),
            "count": len(sorted_monthly)
        })

@app.route('/api/pnl/annual', methods=['GET'])
@login_required
def get_annual_pnl():
    """Get annual PnL data."""
    with data_lock:
        annual = pnl_data.get("annual_pnl", {})
        sorted_annual = dict(sorted(annual.items(), reverse=True))

        return jsonify({
            "annual_pnl": sorted_annual,
            "total_pnl": sum(d.get("pnl", 0) for d in sorted_annual.values()),
            "total_volume": sum(d.get("volume", 0) for d in sorted_annual.values()),
            "count": len(sorted_annual)
        })

@app.route('/api/pnl/reload', methods=['POST'])
@login_required
def reload_pnl():
    """
    Force reload PnL data.
    Query params:
    - source: "api", "excel", or "auto" (default: "auto")
    - days: number of days to fetch from API (default: TENASKA_FETCH_DAYS_BACK)
    """
    global pnl_data

    source = request.args.get('source', 'auto')
    start_date = request.args.get('start_date', TENASKA_FETCH_START_DATE)

    try:
        records = []
        hub_prices = {}
        data_source = ""

        # Try API first if requested
        if source in ("api", "auto") and TENASKA_AUTO_FETCH:
            logger.info(f"Reloading PnL data from Tenaska API (start_date={start_date})...")
            records = fetch_energy_imbalance_data(start_date=start_date)
            if records:
                data_source = "api"
                logger.info(f"Loaded {len(records)} records from Tenaska API")
                # Also fetch hub prices
                hub_prices = fetch_hub_prices(start_date=start_date)

        # Fall back to Excel if API returned nothing
        if not records and source in ("excel", "auto"):
            logger.info("Reloading PnL data from Excel file...")
            records = load_energy_imbalance_from_excel()
            if records:
                data_source = "excel"
                logger.info(f"Loaded {len(records)} records from Excel file")
                # Fetch hub prices for basis calculation
                if TENASKA_AUTO_FETCH:
                    hub_prices = fetch_hub_prices(start_date=start_date)

        if records:
            # Aggregate the data with hub prices for basis calculation
            aggregated = aggregate_excel_pnl(records, hub_prices=hub_prices)

            with data_lock:
                pnl_data["daily_pnl"] = aggregated["daily"]
                pnl_data["monthly_pnl"] = aggregated["monthly"]
                pnl_data["annual_pnl"] = aggregated["annual"]
                pnl_data["total_pnl"] = aggregated["total_pnl"]
                pnl_data["total_volume"] = aggregated["total_volume"]
                pnl_data["record_count"] = aggregated["record_count"]
                pnl_data["assets"] = aggregated.get("assets", {})
                pnl_data["worst_basis_intervals"] = aggregated.get("worst_basis_intervals", [])
                pnl_data["last_tenaska_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()

            # Save to JSON for persistence
            save_pnl_data(pnl_data)

            # Build asset summary for response
            asset_summary = {k: v.get("total_pnl", 0) for k, v in aggregated.get("assets", {}).items()}

            return jsonify({
                "success": True,
                "message": f"Loaded {aggregated['record_count']} records from {data_source}",
                "source": data_source,
                "total_pnl": aggregated["total_pnl"],
                "assets": asset_summary,
                "worst_intervals_count": len(aggregated.get("worst_basis_intervals", []))
            })
        else:
            return jsonify({
                "success": False,
                "message": f"No records found (source={source})"
            }), 400

    except Exception as e:
        logger.error(f"Error reloading PnL data: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

# ============================================================================
# PHAROS API ENDPOINTS (NWOH - PJM DA/RT Data)
# ============================================================================
@app.route('/api/pharos/da', methods=['GET'])
@login_required
def get_pharos_da():
    """Get NWOH Day-Ahead awards data."""
    with data_lock:
        return jsonify({
            "daily_da": pharos_data.get("daily_da", {}),
            "monthly_da": pharos_data.get("monthly_da", {}),
            "annual_da": pharos_data.get("annual_da", {}),
            "total_da_mwh": pharos_data.get("total_da_mwh", 0),
            "total_da_revenue": pharos_data.get("total_da_revenue", 0),
            "capped_intervals": pharos_data.get("capped_intervals", []),
            "last_update": pharos_data.get("last_pharos_update"),
        })

@app.route('/api/pharos/da/daily', methods=['GET'])
@login_required
def get_pharos_da_daily():
    """Get NWOH daily DA data with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    with data_lock:
        daily = pharos_data.get("daily_da", {})

        if start_date or end_date:
            filtered = {}
            for date_key, data in daily.items():
                if start_date and date_key < start_date:
                    continue
                if end_date and date_key > end_date:
                    continue
                filtered[date_key] = data
            daily = filtered

        sorted_daily = dict(sorted(daily.items(), reverse=True))

        return jsonify({
            "daily_da": sorted_daily,
            "total_da_mwh": sum(d.get("da_mwh", 0) for d in sorted_daily.values()),
            "total_da_revenue": sum(d.get("da_revenue", 0) for d in sorted_daily.values()),
            "count": len(sorted_daily)
        })

@app.route('/api/pharos/da/capped', methods=['GET'])
@login_required
def get_pharos_capped_intervals():
    """Get price-capped intervals for NWOH DA risk management."""
    with data_lock:
        capped = pharos_data.get("capped_intervals", [])
        daily = pharos_data.get("daily_da", {})

        # Calculate summary stats
        total_capped = sum(d.get("capped_count", 0) for d in daily.values())
        total_intervals = sum(d.get("count", 0) for d in daily.values())

        return jsonify({
            "capped_intervals": capped,
            "total_capped_count": total_capped,
            "total_intervals": total_intervals,
            "capped_percentage": round(total_capped / total_intervals * 100, 2) if total_intervals > 0 else 0,
        })

@app.route('/api/pharos/pnl', methods=['GET'])
@login_required
def get_pharos_pnl():
    """Get NWOH PnL data from unit operations (DA + RT combined)."""
    with data_lock:
        return jsonify({
            "daily_pnl": pharos_data.get("daily_pnl", {}),
            "monthly_pnl": pharos_data.get("monthly_pnl", {}),
            "annual_pnl": pharos_data.get("annual_pnl", {}),
            "total_pnl": pharos_data.get("total_pnl", 0),
            "total_volume": pharos_data.get("total_volume", 0),
            "last_update": pharos_data.get("last_pharos_update"),
        })

@app.route('/api/pharos/pnl/daily', methods=['GET'])
@login_required
def get_pharos_pnl_daily():
    """Get NWOH daily PnL data with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    with data_lock:
        daily = pharos_data.get("daily_pnl", {})

        if start_date or end_date:
            filtered = {}
            for date_key, data in daily.items():
                if start_date and date_key < start_date:
                    continue
                if end_date and date_key > end_date:
                    continue
                filtered[date_key] = data
            daily = filtered

        sorted_daily = dict(sorted(daily.items(), reverse=True))

        return jsonify({
            "daily_pnl": sorted_daily,
            "total_pnl": sum(d.get("pnl", 0) for d in sorted_daily.values()),
            "total_volume": sum(d.get("volume", 0) for d in sorted_daily.values()),
            "count": len(sorted_daily)
        })

@app.route('/api/pharos/status', methods=['GET'])
@login_required
def get_pharos_status():
    """Get Pharos API status and data summary."""
    with data_lock:
        daily_days = len(pharos_data.get("daily_pnl", {}))
        monthly_months = len(pharos_data.get("monthly_pnl", {}))

        # Get date range
        daily_dates = list(pharos_data.get("daily_pnl", {}).keys())
        date_range = f"{min(daily_dates)} to {max(daily_dates)}" if daily_dates else "No data"

        return jsonify({
            "pharos_auto_fetch": PHAROS_AUTO_FETCH,
            "pharos_fetch_interval_seconds": PHAROS_FETCH_INTERVAL,
            "pharos_fetch_start_date": PHAROS_FETCH_START_DATE,
            "data_loaded": daily_days > 0,
            "total_pnl": pharos_data.get("total_pnl", 0),
            "total_volume": pharos_data.get("total_volume", 0),
            "total_da_mwh": pharos_data.get("total_da_mwh", 0),
            "daily_records": daily_days,
            "monthly_records": monthly_months,
            "unit_ops_count": len(pharos_data.get("unit_ops", [])),
            "da_awards_count": len(pharos_data.get("da_awards", [])),
            "date_range": date_range,
            "last_update": pharos_data.get("last_pharos_update"),
            "capped_intervals": len(pharos_data.get("capped_intervals", [])),
        })

@app.route('/api/pharos/reload', methods=['POST'])
@login_required
def reload_pharos():
    """Force reload Pharos/NWOH data from API (both DA awards and unit operations)."""
    global pharos_data

    start_date = request.args.get('start_date', PHAROS_FETCH_START_DATE)

    try:
        if not PHAROS_AUTO_FETCH:
            return jsonify({
                "success": False,
                "message": "Pharos auto-fetch is disabled"
            }), 400

        # Fetch DA awards
        logger.info(f"Reloading Pharos DA data (start_date={start_date})...")
        awards = fetch_pharos_da_awards(start_date=start_date)

        if awards:
            aggregated = aggregate_pharos_da_data(awards)
            with data_lock:
                pharos_data["da_awards"] = awards
                pharos_data["daily_da"] = aggregated["daily"]
                pharos_data["monthly_da"] = aggregated["monthly"]
                pharos_data["annual_da"] = aggregated["annual"]
                pharos_data["total_da_mwh"] = aggregated["total_da_mwh"]
                pharos_data["total_da_revenue"] = aggregated["total_da_revenue"]
                pharos_data["capped_intervals"] = aggregated["capped_intervals"]

        # Fetch PnL data using hourly_revenue_estimate endpoint
        # This endpoint provides pre-calculated values that match Pharos exactly
        logger.info(f"Fetching Pharos hourly revenue data (start_date={start_date})...")
        unit_ops = fetch_pharos_hourly_revenue(start_date=start_date)

        if unit_ops:
            ops_aggregated = aggregate_pharos_unit_operations(unit_ops)
            with data_lock:
                pharos_data["unit_ops"] = unit_ops
                pharos_data["daily_pnl"] = ops_aggregated["daily"]
                pharos_data["monthly_pnl"] = ops_aggregated["monthly"]
                pharos_data["annual_pnl"] = ops_aggregated["annual"]
                pharos_data["total_pnl"] = ops_aggregated["total_pnl"]
                pharos_data["total_volume"] = ops_aggregated["total_volume"]

        with data_lock:
            pharos_data["last_pharos_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()

        # Always merge historical NWOH data back in after API refresh
        # This ensures older data from Excel import isn't lost when API returns limited results
        merge_nwoh_historical_with_pharos()

        save_pharos_data(pharos_data)

        if awards or unit_ops:
            return jsonify({
                "success": True,
                "message": f"Loaded {len(awards or [])} DA awards and {len(unit_ops or [])} unit ops from Pharos",
                "total_da_mwh": pharos_data.get("total_da_mwh", 0),
                "total_pnl": pharos_data.get("total_pnl", 0),
                "total_volume": pharos_data.get("total_volume", 0),
                "total_capped": len(pharos_data.get("capped_intervals", [])),
            })
        else:
            # Even if API returned nothing, we still have historical data
            if pharos_data.get("daily_pnl"):
                return jsonify({
                    "success": True,
                    "message": "Pharos API returned no new data, but historical data is available",
                    "total_pnl": pharos_data.get("total_pnl", 0),
                    "total_volume": pharos_data.get("total_volume", 0),
                })
            return jsonify({
                "success": False,
                "message": "No data found from Pharos API"
            }), 400

    except Exception as e:
        logger.error(f"Error reloading Pharos data: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

@app.route('/api/pharos/debug/<date>', methods=['GET'])
@login_required
def debug_pharos_date(date):
    """Debug endpoint to fetch raw Pharos data for a specific date."""
    try:
        # Fetch raw data for the specified date
        da_url = f"{PHAROS_BASE_URL}/pjm/market_results/historic"
        da_params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": date,
            "end_date": date,
        }
        da_response = requests.get(da_url, auth=get_pharos_auth(), params=da_params, timeout=60)

        meter_url = f"{PHAROS_BASE_URL}/pjm/power_meter/submissions"
        meter_params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": date,
            "end_date": date,
        }
        meter_response = requests.get(meter_url, auth=get_pharos_auth(), params=meter_params, timeout=60)

        lmp_url = f"{PHAROS_BASE_URL}/pjm/lmp/historic"
        lmp_params = {
            "organization_key": PHAROS_ORGANIZATION_KEY,
            "start_date": date,
            "end_date": date,
        }
        lmp_response = requests.get(lmp_url, auth=get_pharos_auth(), params=lmp_params, timeout=60)

        # Parse and summarize
        da_data = da_response.json() if da_response.status_code == 200 else {"error": da_response.status_code}
        meter_data = meter_response.json() if meter_response.status_code == 200 else {"error": meter_response.status_code}
        lmp_data = lmp_response.json() if lmp_response.status_code == 200 else {"error": lmp_response.status_code}

        # Calculate totals from DA
        da_results = da_data.get("market_results", da_data) if isinstance(da_data, dict) else da_data
        da_total_mwh = sum(float(r.get("energy_mw", 0) or 0) for r in da_results) if isinstance(da_results, list) else 0
        da_total_rev = sum(float(r.get("energy_mw", 0) or 0) * float(r.get("energy_price", 0) or 0) for r in da_results) if isinstance(da_results, list) else 0

        # Calculate totals from meter
        submissions = meter_data.get("submissions", []) if isinstance(meter_data, dict) else []
        meter_values = submissions[0].get("meter_values", []) if submissions else []
        meter_total_mwh = sum(float(mv.get("mw", 0) or mv.get("mwh", 0) or 0) for mv in meter_values) if meter_values else 0

        # Get sample meter value structure
        sample_meter = meter_values[0] if meter_values else {}

        return jsonify({
            "date": date,
            "da": {
                "count": len(da_results) if isinstance(da_results, list) else 0,
                "total_mwh": da_total_mwh,
                "total_revenue": da_total_rev,
                "sample": da_results[0] if isinstance(da_results, list) and da_results else None,
            },
            "meter": {
                "count": len(meter_values),
                "total_mwh": meter_total_mwh,
                "sample_structure": list(sample_meter.keys()) if sample_meter else [],
                "sample": sample_meter,
            },
            "lmp": {
                "count": len(lmp_data.get("lmp", [])) if isinstance(lmp_data, dict) else 0,
                "sample": lmp_data.get("lmp", [{}])[0] if isinstance(lmp_data, dict) and lmp_data.get("lmp") else None,
            },
            "expected_rt_revenue": meter_total_mwh - da_total_mwh,  # Deviation in MWh
        })
    except Exception as e:
        logger.error(f"Error in debug endpoint: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/nwoh/status', methods=['GET'])
@login_required
def get_nwoh_status():
    """
    Get real-time NWOH status including:
    - Price cap status and current caps
    - Next-day DA awards
    - Current dispatch performance
    - Today's DA commitment vs actual generation
    """
    try:
        # Fetch price caps
        price_caps = fetch_pharos_price_caps()

        # Fetch next-day DA awards
        next_day = fetch_pharos_next_day_awards()

        # Fetch today's DA awards from market_results (full 24-hour commitment)
        today_da = fetch_pharos_today_da_awards()

        # Fetch current dispatch
        current_dispatch = fetch_pharos_current_dispatch()

        # Get today's actual generation (per-hour from dispatches + total)
        today = datetime.now().strftime("%Y-%m-%d")
        today_actual_gen = 0
        gen_source = "meter"
        dispatch_hourly_gen = {}  # {hour_ending: mwh}

        # First try meter data (most accurate for total)
        with data_lock:
            daily_pnl = pharos_data.get("daily_pnl", {})
            today_data = daily_pnl.get(today, {})
            today_actual_gen = today_data.get("volume", 0)

        # Always fetch dispatches for per-hour breakdown (needed for hourly chart)
        today_gen_data = fetch_pharos_today_generation()
        if today_gen_data:
            dispatch_hourly_gen = today_gen_data.get("hourly_gen", {})
            if today_actual_gen == 0:
                today_actual_gen = today_gen_data.get("total_mwh", 0)
                gen_source = "dispatches"

        # Fetch today's RT LMP for deviation settlement calculations
        rt_lmp_data = fetch_pharos_today_rt_lmp()
        rt_lmp_by_he = rt_lmp_data.get("rt_lmp", {}) if rt_lmp_data else {}
        hub_lmp_by_he = rt_lmp_data.get("hub_lmp", {}) if rt_lmp_data else {}

        # Fallback: if Pharos LMP endpoint doesn't have hub prices, use PJM hub cache
        hub_all_zero = all(v == 0 for v in hub_lmp_by_he.values()) if hub_lmp_by_he else True
        if hub_all_zero:
            logger.info("[NWOH] Hub LMP from Pharos is all zeros, trying PJM hub price cache...")
            try:
                # Fetch today + tomorrow UTC to cover full EST day
                tomorrow = (datetime.strptime(today, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                ensure_hub_prices_cached(today, tomorrow)
            except Exception as e:
                logger.warning(f"Could not fetch hub prices from PJM API: {e}")

            if pjm_hub_price_cache:
                est_tz = ZoneInfo("America/New_York")
                hourly_hub_sums = defaultdict(list)
                for ts_str, price in pjm_hub_price_cache.items():
                    try:
                        dt_utc = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
                        dt_est = dt_utc.astimezone(est_tz)
                        day_str = dt_est.strftime("%Y-%m-%d")
                        if day_str == today:
                            # 5-min interval beginning at hour X belongs to HE X+1
                            he = dt_est.hour + 1
                            hourly_hub_sums[he].append(float(price))
                    except Exception:
                        pass

                for he, prices in hourly_hub_sums.items():
                    hub_lmp_by_he[he] = sum(prices) / len(prices) if prices else 0

                if hourly_hub_sums:
                    logger.info(f"[NWOH] Got hub prices from PJM cache for {len(hourly_hub_sums)} hours")
                else:
                    logger.warning("[NWOH] PJM hub cache exists but no data for today")
            else:
                logger.warning("[NWOH] PJM hub price cache is empty - hub LMP will be 0")

        # Use DA commitment from market_results (full day), not unit_operations (partial)
        today_da_commitment = today_da.get("total_da_mwh", 0)

        # Build hourly breakdown combining all data sources:
        # Priority: unit_ops (hourly_revenue_estimate) > dispatches + lmp + da_awards
        # Each hour settles independently in PJM DART
        hourly_breakdown = []
        with data_lock:
            unit_ops = pharos_data.get("unit_ops", [])
            today_ops = [op for op in unit_ops if op.get("date") == today or (op.get("timestamp", "").startswith(today))]

            # Index by hour ending
            ops_by_he = {}
            for op in today_ops:
                he = op.get("he")
                if he is None:
                    # Parse from timestamp
                    ts = op.get("timestamp", "")
                    if "T" in ts:
                        he = int(ts.split("T")[1].split(":")[0]) + 1
                    elif " " in ts:
                        he = int(ts.split(" ")[1].split(":")[0]) + 1
                if he is not None:
                    ops_by_he[he] = op

        # Also index DA awards by hour ending
        da_by_he = {}
        for award in today_da.get("hourly", []):
            he = award.get("hour_ending")
            if he is not None:
                da_by_he[he] = award

        current_he = datetime.now().hour + 1  # Current hour ending

        for he in range(1, 25):
            op = ops_by_he.get(he, {})
            award = da_by_he.get(he, {})

            # DA data: prefer unit_ops, fallback to DA awards
            da_mw = float(op.get("dam_mw", 0) or award.get("da_award_mw", 0) or 0)
            da_lmp = float(op.get("da_lmp", 0) or award.get("da_lmp", 0) or 0)

            # Generation data: prefer unit_ops, fallback to dispatch hourly gen
            gen_mw = float(op.get("gen", 0) or 0)
            has_gen = op.get("has_gen_data", False)

            if gen_mw == 0 and he in dispatch_hourly_gen:
                # Dispatch gen is in MWh (already aggregated from 5-min intervals)
                gen_mw = dispatch_hourly_gen[he]
                has_gen = True

            # RT LMP: prefer unit_ops, fallback to lmp/historic
            rt_lmp = float(op.get("rt_lmp", 0) or 0)
            if rt_lmp == 0 and he in rt_lmp_by_he:
                rt_lmp = rt_lmp_by_he[he]

            # Hub LMP for basis calculation: use lmp/historic only (consistent source with node)
            hub_lmp = hub_lmp_by_he.get(he, 0)

            # RT deviation = actual gen - DA commitment
            rt_dev = float(op.get("rt_mw", 0) or 0)
            if rt_dev == 0 and has_gen and da_mw > 0:
                rt_dev = gen_mw - da_mw

            # Revenue calculations: prefer unit_ops pre-calculated values
            da_rev = float(op.get("dam_revenue", 0) or 0)
            rt_rev = float(op.get("rt_revenue", 0) or 0)
            net_rev = float(op.get("net_revenue", 0) or 0)

            # Calculate revenue if unit_ops doesn't have it
            if da_rev == 0 and da_mw > 0 and da_lmp > 0:
                da_rev = da_mw * da_lmp
            if rt_rev == 0 and abs(rt_dev) > 0 and rt_lmp != 0:
                rt_rev = rt_dev * rt_lmp
            if net_rev == 0 and (da_rev != 0 or rt_rev != 0):
                net_rev = da_rev + rt_rev

            # Determine hour status
            if he > current_he:
                status = "future"
            elif has_gen or gen_mw > 0:
                if da_mw == 0:
                    status = "no_award"
                elif abs(rt_dev) < 1:  # Within 1 MW tolerance
                    status = "matched"
                elif rt_dev > 0:
                    status = "over"  # Over-generated, selling RT
                else:
                    status = "under"  # Under-generated, buying RT
            elif he <= current_he and da_mw > 0:
                status = "pending"  # Past/current hour, no gen data yet
            else:
                status = "no_award"

            hourly_breakdown.append({
                "he": he,
                "da_mw": round(da_mw, 1),
                "gen_mw": round(gen_mw, 1),
                "deviation_mw": round(rt_dev, 1),
                "da_lmp": round(da_lmp, 2),
                "rt_lmp": round(rt_lmp, 2),
                "hub_lmp": round(hub_lmp, 2),
                "da_revenue": round(da_rev, 2),
                "rt_revenue": round(rt_rev, 2),
                "net_revenue": round(net_rev, 2),
                "status": status,
                "has_gen": has_gen,
            })

        # Compute today's revenue totals from hourly breakdown
        total_da_revenue = sum(h["da_revenue"] for h in hourly_breakdown)
        total_rt_revenue = sum(h["rt_revenue"] for h in hourly_breakdown)
        total_net_revenue = sum(h["net_revenue"] for h in hourly_breakdown)
        total_gen = sum(h["gen_mw"] for h in hourly_breakdown)
        total_da_mwh = sum(h["da_mw"] for h in hourly_breakdown)

        # Split RT into sales (over-generation) vs purchases (under-generation)
        rt_sales_revenue = sum(h["rt_revenue"] for h in hourly_breakdown if h["rt_revenue"] > 0)
        rt_purchase_cost = sum(abs(h["rt_revenue"]) for h in hourly_breakdown if h["rt_revenue"] < 0)
        rt_sales_mwh = sum(h["deviation_mw"] for h in hourly_breakdown if h["deviation_mw"] > 0 and h["has_gen"])
        rt_purchase_mwh = sum(abs(h["deviation_mw"]) for h in hourly_breakdown if h["deviation_mw"] < 0 and h["has_gen"])

        # Weighted avg prices
        da_lmp_product = sum(h["da_mw"] * h["da_lmp"] for h in hourly_breakdown)
        rt_lmp_product = sum(h["gen_mw"] * h["rt_lmp"] for h in hourly_breakdown)
        hub_lmp_product = sum(h["gen_mw"] * h.get("hub_lmp", 0) for h in hourly_breakdown)
        avg_da_price = da_lmp_product / total_da_mwh if total_da_mwh > 0 else 0
        avg_rt_price = rt_lmp_product / total_gen if total_gen > 0 else 0
        avg_hub_price = hub_lmp_product / total_gen if total_gen > 0 else 0

        # GWA Basis = (Hub Revenue - Nodal Revenue) / Generation
        # Use ONLY lmp/historic data for both hub and node to ensure consistent source
        basis_hub_rev = 0
        basis_node_rev = 0
        basis_gen = 0
        for h in hourly_breakdown:
            he = h["he"]
            gen = h["gen_mw"]
            if gen > 0 and he in rt_lmp_by_he:
                # Both hub and node from lmp/historic endpoint for apples-to-apples comparison
                node_lmp = rt_lmp_by_he.get(he, 0)
                hub_lmp_val = hub_lmp_by_he.get(he, 0)
                basis_node_rev += gen * node_lmp
                basis_hub_rev += gen * hub_lmp_val
                basis_gen += gen
        gwa_basis = (basis_hub_rev - basis_node_rev) / basis_gen if basis_gen > 0 else 0

        # PPA Settlement: 100% PPA @ $33.31/MWh with GM
        ppa_price = 33.31
        ppa_fixed_payment = total_gen * ppa_price  # GM pays us
        ppa_floating_payment = hub_lmp_product      # We pay GM (gen × hub_lmp, already summed)
        ppa_net_settlement = ppa_fixed_payment - ppa_floating_payment
        # Total PnL = PJM market revenue + PPA net settlement
        total_pnl = total_net_revenue + ppa_net_settlement

        return jsonify({
            "price_caps": price_caps,
            "next_day_awards": next_day,
            "current_dispatch": current_dispatch,
            "today": {
                "date": today,
                "da_commitment_mwh": round(today_da_commitment, 2),
                "actual_gen_mwh": round(today_actual_gen, 2),
                "deviation_mwh": round(today_actual_gen - today_da_commitment, 2),
                "performance_pct": round((today_actual_gen / today_da_commitment * 100), 1) if today_da_commitment > 0 else 0,
                "hourly_awards": today_da.get("hourly", []),
                "hourly_breakdown": hourly_breakdown,
                "hours_with_awards": today_da.get("hours_with_awards", 0),
                "gen_source": gen_source,
                # Revenue totals computed from hourly breakdown
                "da_revenue": round(total_da_revenue, 2),
                "rt_revenue": round(total_rt_revenue, 2),
                "rt_sales_revenue": round(rt_sales_revenue, 2),
                "rt_purchase_cost": round(rt_purchase_cost, 2),
                "rt_sales_mwh": round(rt_sales_mwh, 2),
                "rt_purchase_mwh": round(rt_purchase_mwh, 2),
                "net_revenue": round(total_net_revenue, 2),
                "total_gen_mwh": round(total_gen, 2),
                "total_da_mwh": round(total_da_mwh, 2),
                "avg_da_price": round(avg_da_price, 2),
                "avg_rt_price": round(avg_rt_price, 2),
                "avg_hub_price": round(avg_hub_price, 2),
                "gwa_basis": round(gwa_basis, 2),
                "ppa_fixed_payment": round(ppa_fixed_payment, 2),
                "ppa_floating_payment": round(ppa_floating_payment, 2),
                "ppa_net_settlement": round(ppa_net_settlement, 2),
                "total_pnl": round(total_pnl, 2),
            },
            "fetched_at": datetime.now().isoformat(),
        })

    except Exception as e:
        logger.error(f"Error getting NWOH status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
@login_required
def dashboard():
    return '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ERCOT + PJM Basis Tracker</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        /* SkyVest Color Palette */
        :root {
            --skyvest-navy: #0E2C51;
            --skyvest-light-blue: #A7D3F7;
            --skyvest-gray: #D9D9D9;
            --skyvest-gold: #FFD966;
            --skyvest-blue: #2291EB;
            --skyvest-navy-light: #1a4370;
        }

        /* Operational Dashboard Typography */
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            letter-spacing: -0.01em;
        }

        .dashboard-title {
            font-family: Georgia, "Times New Roman", serif;
            font-weight: 700;
            letter-spacing: -0.02em;
        }

        .metric-label {
            font-size: 0.6875rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
        }

        .card {
            background: white;
            border: 2px solid #e5e5e5;
            transition: all 0.3s ease;
        }

        .card:hover {
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }

        /* Basis card status styling */
        .basis-card {
            transition: background-color 0.3s ease, border-color 0.3s ease, box-shadow 0.3s ease;
        }

        /* Alert pulse animation */
        @keyframes alertPulse {
            0%, 100% {
                box-shadow: 0 0 20px rgba(239, 68, 68, 0.5), 0 4px 12px rgba(0, 0, 0, 0.1);
            }
            50% {
                box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), 0 4px 16px rgba(0, 0, 0, 0.15);
            }
        }

        .alert-pulse {
            animation: alertPulse 2s ease-in-out infinite;
        }
    </style>
</head>
<body style="background-color: #f8f9fa;">
    <div class="p-3 md:p-4" style="background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);">
        <div class="max-w-7xl mx-auto">
            <!-- Header -->
            <div class="mb-3 pb-2" style="border-bottom: 2px solid var(--skyvest-navy);">
                <div class="flex justify-between items-start">
                    <div>
                        <h1 class="dashboard-title text-2xl mb-1" style="color: var(--skyvest-navy);">ERCOT + PJM Basis Tracker</h1>
                        <p class="metric-label text-xs" style="color: var(--skyvest-blue);">Real-time Multi-Market Basis Analysis</p>
                    </div>
                    <div class="text-right">
                        <div id="connection" class="text-xs font-semibold px-2 py-1 rounded-full" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">Connecting...</div>
                    </div>
                </div>
            </div>

            <!-- ERCOT Section -->
            <div class="mb-2">
                <h2 class="text-lg font-bold mb-2" style="color: var(--skyvest-navy); border-left: 3px solid var(--skyvest-blue); padding-left: 8px;">ERCOT</h2>
            </div>

            <!-- ERCOT Price Cards -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">NBOHR_RN</p>
                    <span id="node1" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">HOLSTEIN_ALL</p>
                    <span id="node2" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-1" style="color: var(--skyvest-light-blue);">HB_WEST (Hub)</p>
                    <span id="hub" class="text-2xl font-light text-white">N/A</span>
                </div>
            </div>

            <!-- ERCOT Basis Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div id="basis-card-1" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="basis1-label" class="metric-label mb-1" style="color: #666;">NBOHR_RN Basis</p>
                            <span id="basis1" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="status1-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="status1" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="basis1-subtitle" class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>

                <div id="basis-card-2" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="basis2-label" class="metric-label mb-1" style="color: #666;">HOLSTEIN_ALL Basis</p>
                            <span id="basis2" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="status2-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="status2" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="basis2-subtitle" class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>
            </div>

            <!-- ERCOT Charts -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">NBOHR_RN Basis Trend</h3>
                    <div id="chart-container-1"></div>
                </div>

                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">HOLSTEIN_ALL Basis Trend</h3>
                    <div id="chart-container-2"></div>
                </div>
            </div>

            <!-- PJM Section -->
            <div class="mb-2">
                <h2 class="text-lg font-bold mb-2" style="color: var(--skyvest-navy); border-left: 3px solid var(--skyvest-blue); padding-left: 8px;">PJM</h2>
            </div>

            <!-- PJM Price Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">HAVILAND34.5 KV NTHWSTWF</p>
                    <span id="pjm-node" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-1" style="color: var(--skyvest-light-blue);">AEP-DAYTON HUB</p>
                    <span id="pjm-hub" class="text-2xl font-light text-white">N/A</span>
                </div>
            </div>

            <!-- PJM Basis Card -->
            <div class="grid grid-cols-1 gap-2 mb-3">
                <div id="pjm-basis-card" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="pjm-basis-label" class="metric-label mb-1" style="color: #666;">HAVILAND Basis</p>
                            <span id="pjm-basis" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="pjm-status-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="pjm-status" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="pjm-basis-subtitle" class="text-xs" style="color: #999;">vs AEP-DAYTON HUB</p>
                </div>
            </div>

            <!-- PJM Chart -->
            <div class="grid grid-cols-1 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">HAVILAND Basis Trend</h3>
                    <div id="pjm-chart-container"></div>
                </div>
            </div>

            <!-- PnL Section -->
            <div class="mb-2 mt-4">
                <div class="flex justify-between items-center">
                    <h2 class="text-lg font-bold" style="color: var(--skyvest-navy); border-left: 3px solid var(--skyvest-blue); padding-left: 8px;">Energy Imbalance PnL</h2>
                    <button onclick="reloadPnlData()" class="px-3 py-1 text-xs font-semibold rounded" style="background-color: var(--skyvest-gold); color: var(--skyvest-navy);">Reload Data</button>
                </div>
            </div>

            <!-- Toggle Controls -->
            <div class="card rounded-sm p-3 mb-3">
                <div class="flex flex-wrap gap-4 items-center">
                    <!-- Time Period Toggle -->
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-semibold" style="color: #666;">Period:</span>
                        <div class="flex rounded overflow-hidden border" style="border-color: var(--skyvest-navy);">
                            <button id="period-daily" onclick="setPeriod('daily')" class="px-3 py-1 text-xs font-semibold period-btn" style="background-color: white; color: var(--skyvest-navy);">Daily</button>
                            <button id="period-mtd" onclick="setPeriod('mtd')" class="px-3 py-1 text-xs font-semibold period-btn" style="background-color: white; color: var(--skyvest-navy);">MTD</button>
                            <button id="period-ytd" onclick="setPeriod('ytd')" class="px-3 py-1 text-xs font-semibold period-btn active" style="background-color: var(--skyvest-navy); color: white;">YTD</button>
                        </div>
                    </div>
                    <!-- Asset Filter Toggle -->
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-semibold" style="color: #666;">Asset:</span>
                        <div class="flex rounded overflow-hidden border" style="border-color: var(--skyvest-navy);">
                            <button id="asset-all" onclick="setAssetFilter('all')" class="px-3 py-1 text-xs font-semibold asset-btn active" style="background-color: var(--skyvest-navy); color: white;">All</button>
                            <button id="asset-BKI" onclick="setAssetFilter('BKI')" class="px-3 py-1 text-xs font-semibold asset-btn" style="background-color: white; color: var(--skyvest-navy);">BKI</button>
                            <button id="asset-BKII" onclick="setAssetFilter('BKII')" class="px-3 py-1 text-xs font-semibold asset-btn" style="background-color: white; color: var(--skyvest-navy);">BKII</button>
                            <button id="asset-HOLSTEIN" onclick="setAssetFilter('HOLSTEIN')" class="px-3 py-1 text-xs font-semibold asset-btn" style="background-color: white; color: var(--skyvest-navy);">Holstein</button>
                            <button id="asset-NWOH" onclick="setAssetFilter('NWOH')" class="px-3 py-1 text-xs font-semibold asset-btn" style="background-color: white; color: var(--skyvest-navy);">NWOH</button>
                        </div>
                    </div>
                    <!-- Date Range Picker -->
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-semibold" style="color: #666;">From:</span>
                        <input type="date" id="date-picker-start" onchange="setDateRange()"
                               class="px-2 py-1 text-xs border rounded"
                               style="border-color: var(--skyvest-navy); color: var(--skyvest-navy);">
                        <span class="text-xs font-semibold" style="color: #666;">To:</span>
                        <input type="date" id="date-picker-end" onchange="setDateRange()"
                               class="px-2 py-1 text-xs border rounded"
                               style="border-color: var(--skyvest-navy); color: var(--skyvest-navy);">
                        <button onclick="resetToToday()" class="px-2 py-1 text-xs font-semibold rounded border"
                                style="border-color: var(--skyvest-navy); background-color: white; color: var(--skyvest-navy);"
                                title="Reset to today">Today</button>
                    </div>
                </div>
            </div>

            <!-- Summary Cards (updates based on toggles) -->
            <div class="grid grid-cols-1 md:grid-cols-4 gap-2 mb-3">
                <div class="card rounded-sm p-3" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-1" style="color: var(--skyvest-light-blue);"><span id="pnl-label">YTD</span> PnL</p>
                    <span id="filtered-pnl" class="text-2xl font-bold text-white">$0.00</span>
                </div>
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;"><span id="volume-label">YTD</span> Volume</p>
                    <span id="filtered-volume" class="text-2xl font-light" style="color: var(--skyvest-navy);">0 MWh</span>
                </div>
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">Realized Price</p>
                    <span id="filtered-realized" class="text-2xl font-light" style="color: var(--skyvest-navy);">--</span>
                </div>
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">GWA Basis</p>
                    <span id="filtered-basis" class="text-2xl font-light" style="color: var(--skyvest-navy);">--</span>
                </div>
            </div>

            <!-- Hidden fields for original values -->
            <input type="hidden" id="total-pnl" value="0">
            <input type="hidden" id="total-volume" value="0">
            <input type="hidden" id="today-pnl" value="0">
            <input type="hidden" id="mtd-pnl" value="0">

            <!-- Per-Asset PnL Cards (shown when "All" is selected) -->
            <div id="asset-cards-container" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2 mb-3">
                <div class="card rounded-sm p-3" id="asset-card-BKII">
                    <div class="flex justify-between items-start mb-1">
                        <p class="metric-label" style="color: #666;">McCrae (BKII)</p>
                        <span class="text-xs px-2 py-0.5 rounded" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">100% PPA @ $34</span>
                    </div>
                    <span id="asset-pnl-BKII" class="text-xl font-bold" style="color: var(--skyvest-navy);">$0.00</span>
                    <p class="text-xs mt-1" style="color: #999;"><span id="asset-volume-BKII">0</span> MWh</p>
                    <div class="mt-2 pt-2" style="border-top: 1px solid #eee;">
                        <p class="text-xs" style="color: #666;">Realized: <span id="asset-realized-BKII" class="font-semibold" style="color: var(--skyvest-navy);">--</span></p>
                        <p class="text-xs" style="color: #999;">GWA Basis: <span id="asset-basis-BKII">--</span> (50%)</p>
                    </div>
                </div>
                <div class="card rounded-sm p-3" id="asset-card-BKI">
                    <div class="flex justify-between items-start mb-1">
                        <p class="metric-label" style="color: #666;">Bearkat I</p>
                        <span class="text-xs px-2 py-0.5 rounded" style="background-color: var(--skyvest-gold); color: var(--skyvest-navy);">100% Merchant</span>
                    </div>
                    <span id="asset-pnl-BKI" class="text-xl font-bold" style="color: var(--skyvest-navy);">$0.00</span>
                    <p class="text-xs mt-1" style="color: #999;"><span id="asset-volume-BKI">0</span> MWh</p>
                    <div class="mt-2 pt-2" style="border-top: 1px solid #eee;">
                        <p class="text-xs" style="color: #666;">Realized: <span id="asset-realized-BKI" class="font-semibold" style="color: var(--skyvest-navy);">--</span></p>
                        <p class="text-xs" style="color: #999;">GWA Basis: <span id="asset-basis-BKI">--</span></p>
                    </div>
                </div>
                <div class="card rounded-sm p-3" id="asset-card-HOLSTEIN">
                    <div class="flex justify-between items-start mb-1">
                        <p class="metric-label" style="color: #666;">Holstein</p>
                        <span class="text-xs px-2 py-0.5 rounded" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">87.5% PPA @ $35</span>
                    </div>
                    <span id="asset-pnl-HOLSTEIN" class="text-xl font-bold" style="color: var(--skyvest-navy);">$0.00</span>
                    <p class="text-xs mt-1" style="color: #999;"><span id="asset-volume-HOLSTEIN">0</span> MWh</p>
                    <div class="mt-2 pt-2" style="border-top: 1px solid #eee;">
                        <p class="text-xs" style="color: #666;">PPA: <span id="asset-realized-ppa-HOLSTEIN" class="font-semibold" style="color: var(--skyvest-navy);">--</span> <span style="color: #999;">(87.5%)</span></p>
                        <p class="text-xs" style="color: #666;">Merchant: <span id="asset-realized-merchant-HOLSTEIN" class="font-semibold" style="color: var(--skyvest-navy);">--</span> <span style="color: #999;">(12.5%)</span></p>
                        <p class="text-xs" style="color: #999;">GWA Basis: <span id="asset-basis-HOLSTEIN">--</span> (100%)</p>
                    </div>
                </div>
                <div class="card rounded-sm p-3" id="asset-card-NWOH">
                    <div class="flex justify-between items-start mb-1">
                        <p class="metric-label" style="color: #666;">NW Ohio Wind</p>
                        <span class="text-xs px-2 py-0.5 rounded" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">100% PPA @ $33.31</span>
                    </div>
                    <span id="asset-pnl-NWOH" class="text-xl font-bold" style="color: var(--skyvest-navy);">$0.00</span>
                    <p class="text-xs mt-1" style="color: #999;"><span id="asset-volume-NWOH">0</span> MWh</p>
                    <div class="mt-2 pt-2" style="border-top: 1px solid #eee;">
                        <p class="text-xs" style="color: #666;">Realized: <span id="asset-realized-NWOH" class="font-semibold" style="color: var(--skyvest-navy);">--</span></p>
                        <p class="text-xs" style="color: #999;">GWA Basis: <span id="asset-basis-NWOH">--</span></p>
                    </div>
                    <div id="nwoh-price-cap-warning" class="mt-2 p-2 rounded text-xs" style="display: none; background-color: #fef3c7; border: 1px solid #f59e0b; color: #92400e;">
                        <span style="font-weight: bold;">⚠️ PRICE CAPPED</span>
                    </div>
                </div>
            </div>

            <!-- NWOH Detailed Card (PJM market settlement view) -->
            <div id="nwoh-detail-card" class="card rounded-sm p-4 mb-3" style="display: none; border-left: 4px solid var(--skyvest-blue);">
                <!-- Price Cap Warning Banner -->
                <div id="nwoh-detail-cap-warning" class="mb-3 p-3 rounded" style="display: none; background-color: #fef3c7; border: 1px solid #f59e0b;">
                    <div class="flex items-center gap-2">
                        <span style="font-size: 1.2em;">⚠️</span>
                        <div>
                            <p class="font-bold text-sm" style="color: #92400e;">PRICE CAPPED - Exercise Caution</p>
                            <p id="nwoh-detail-cap-info" class="text-xs" style="color: #a16207;">Capped hours: loading...</p>
                        </div>
                    </div>
                </div>

                <!-- Header -->
                <div class="flex justify-between items-start mb-4">
                    <div>
                        <p class="text-lg font-semibold" style="color: var(--skyvest-navy);">Northwest Ohio Wind</p>
                        <p class="text-xs" style="color: #999;">PJM DA/RT Market | 100% PPA @ $33.31/MWh with GM</p>
                    </div>
                    <div class="flex items-center gap-2">
                        <span id="nwoh-viewing-date" class="text-xs px-2 py-1 rounded" style="display: none; background-color: var(--skyvest-blue); color: white;"></span>
                        <span class="text-xs px-2 py-1 rounded" style="background-color: #e8f4f8; color: var(--skyvest-navy);">105 MW</span>
                    </div>
                </div>

                <!-- ══════════ SECTION 1: TODAY'S PERFORMANCE ══════════ -->
                <div class="mb-5 p-3 rounded" style="background-color: #f8fafc; border: 1px solid #e2e8f0;">
                    <div class="flex justify-between items-center mb-3">
                        <p class="text-sm font-semibold" style="color: var(--skyvest-navy);">Today's Performance</p>
                        <div class="flex items-center gap-2">
                            <span id="nwoh-current-hour" class="text-xs px-2 py-0.5 rounded" style="background-color: var(--skyvest-blue); color: white;">HE --</span>
                            <span id="nwoh-today-date" class="text-xs px-2 py-0.5 rounded" style="background-color: #e2e8f0; color: #64748b;">--</span>
                        </div>
                    </div>

                    <!-- Summary line -->
                    <div class="mb-2">
                        <div class="flex justify-between text-xs mb-1">
                            <span style="color: #64748b;">Day Total DA: <strong id="nwoh-today-da-commitment" style="color: var(--skyvest-navy);">-- MWh</strong> <span id="nwoh-hours-with-awards" style="color: #999;">-- hrs awarded</span></span>
                            <span style="color: #64748b;">Actual Gen: <strong id="nwoh-today-actual-gen" style="color: var(--skyvest-navy);">-- MWh</strong></span>
                        </div>
                    </div>

                    <!-- Hourly DA vs Gen Strip Chart -->
                    <div class="mb-2">
                        <div class="text-xs mb-1" style="color: #64748b; font-weight: 600;">Hourly DA vs Actual (MW)</div>
                        <div id="nwoh-hourly-chart" style="display: flex; gap: 1px; height: 80px; align-items: stretch; background: #f1f5f9; border-radius: 4px; padding: 2px; position: relative;">
                            <!-- 24 hour bars will be populated by JS -->
                        </div>
                        <div id="nwoh-hourly-labels" style="display: flex; gap: 1px; padding: 0 2px;">
                            <!-- Hour labels populated by JS -->
                        </div>
                        <!-- Legend -->
                        <div class="flex gap-3 mt-1" style="font-size: 10px; color: #94a3b8;">
                            <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#22c55e;margin-right:2px;vertical-align:middle;"></span>Over-gen</span>
                            <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:var(--skyvest-blue);margin-right:2px;vertical-align:middle;"></span>Matched</span>
                            <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#ef4444;margin-right:2px;vertical-align:middle;"></span>Under-gen</span>
                            <span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:#e2e8f0;margin-right:2px;vertical-align:middle;"></span>Future</span>
                        </div>
                    </div>

                    <!-- Hourly tooltip (hidden, shown on hover) -->
                    <div id="nwoh-hour-tooltip" style="display:none; position:fixed; background:#1e293b; color:white; padding:8px 12px; border-radius:6px; font-size:11px; z-index:1000; pointer-events:none; box-shadow: 0 4px 12px rgba(0,0,0,0.3); line-height:1.5;">
                    </div>

                    <!-- Deviation summary -->
                    <div class="flex justify-between text-xs mb-3">
                        <span id="nwoh-deviation-text" style="color: #64748b;">RT Deviation: <strong>-- MWh</strong></span>
                        <span id="nwoh-deviation-status" style="color: #64748b;"></span>
                    </div>

                    <!-- Today's Revenue Summary -->
                    <div class="grid grid-cols-3 gap-2 pt-2" style="border-top: 1px dashed #cbd5e1;">
                        <div class="text-center">
                            <p class="text-xs" style="color: #64748b;">DA Revenue</p>
                            <p id="nwoh-today-da-rev" class="font-bold" style="color: var(--skyvest-navy);">$--</p>
                        </div>
                        <div class="text-center">
                            <p class="text-xs" style="color: #64748b;">RT Settlement</p>
                            <p id="nwoh-today-rt-net" class="font-bold" style="color: var(--skyvest-navy);">$--</p>
                        </div>
                        <div class="text-center">
                            <p class="text-xs" style="color: #64748b;">PJM Total</p>
                            <p id="nwoh-today-pjm-total" class="font-bold" style="color: var(--skyvest-blue);">$--</p>
                        </div>
                    </div>
                </div>

                <!-- ══════════ SECTION 2: TOMORROW'S DA AWARDS ══════════ -->
                <div class="mb-5 p-3 rounded" style="background-color: #f0f9ff; border: 1px solid #bae6fd;">
                    <div class="flex justify-between items-center mb-2">
                        <p class="text-sm font-semibold" style="color: var(--skyvest-navy);">Tomorrow's DA Awards</p>
                        <span id="nwoh-tomorrow-date" class="text-xs px-2 py-0.5 rounded" style="background-color: #bae6fd; color: #0369a1;">--</span>
                    </div>
                    <div class="grid grid-cols-3 gap-3">
                        <div>
                            <p class="text-xs" style="color: #64748b;">Total Awarded</p>
                            <p id="nwoh-tomorrow-da-total" class="text-xl font-bold" style="color: var(--skyvest-blue);">-- MWh</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #64748b;">Hours Awarded</p>
                            <p id="nwoh-tomorrow-da-hours" class="text-xl font-bold" style="color: var(--skyvest-navy);">--</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #64748b;">Avg DA Price</p>
                            <p id="nwoh-tomorrow-da-price" class="text-xl font-bold" style="color: var(--skyvest-navy);">$--</p>
                        </div>
                    </div>
                    <div class="mt-2 pt-2" style="border-top: 1px dashed #bae6fd;">
                        <p class="text-xs" style="color: #64748b;">Expected DA Revenue: <strong id="nwoh-tomorrow-da-rev" style="color: var(--skyvest-blue);">$--</strong></p>
                    </div>
                </div>

                <!-- ══════════ SECTION 3: SETTLEMENT FLOW ══════════ -->
                <div class="mb-4">
                    <p class="text-sm font-semibold mb-3" style="color: var(--skyvest-navy);">Settlement Flow <span class="text-xs font-normal" style="color: #999;">(selected period)</span></p>

                    <!-- Visual Flow: PJM Market → PPA Swap → Net Result -->
                    <div class="flex flex-col md:flex-row md:items-stretch gap-2">

                        <!-- Step 1: PJM Market Revenue -->
                        <div class="flex-1 p-3 rounded" style="background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%); border: 1px solid #cbd5e1;">
                            <div class="flex items-center gap-2 mb-2">
                                <span class="text-xs font-bold px-1.5 py-0.5 rounded" style="background: var(--skyvest-navy); color: white;">1</span>
                                <p class="text-xs font-semibold" style="color: var(--skyvest-navy);">PJM Market</p>
                            </div>
                            <div class="space-y-1">
                                <div class="flex justify-between text-xs">
                                    <span style="color: #64748b;">DA Energy</span>
                                    <span id="nwoh-da-revenue" style="color: var(--skyvest-navy);">$0</span>
                                </div>
                                <div class="flex justify-between text-xs">
                                    <span style="color: #64748b;">RT Sales</span>
                                    <span id="nwoh-rt-sales" style="color: #22c55e;">+$0</span>
                                </div>
                                <div class="flex justify-between text-xs">
                                    <span style="color: #64748b;">RT Purchase</span>
                                    <span id="nwoh-rt-purchase" style="color: #ef4444;">-$0</span>
                                </div>
                                <div class="flex justify-between text-xs pt-1 mt-1" style="border-top: 1px solid #cbd5e1;">
                                    <span class="font-semibold" style="color: var(--skyvest-navy);">PJM Total</span>
                                    <span id="nwoh-total-pjm" class="font-bold" style="color: var(--skyvest-blue);">$0</span>
                                </div>
                            </div>
                            <div class="mt-2 pt-2 text-xs" style="border-top: 1px dashed #cbd5e1; color: #64748b;">
                                <span id="nwoh-gen-mwh">0</span> MWh @ <span id="nwoh-avg-rt-lmp">--</span>/MWh avg
                            </div>
                        </div>

                        <!-- Arrow 1 -->
                        <div class="hidden md:flex items-center justify-center px-1" style="color: #94a3b8;">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M5 12h14M12 5l7 7-7 7"/>
                            </svg>
                        </div>

                        <!-- Step 2: PPA Swap Settlement -->
                        <div class="flex-1 p-3 rounded" style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); border: 1px solid #fbbf24;">
                            <div class="flex items-center gap-2 mb-2">
                                <span class="text-xs font-bold px-1.5 py-0.5 rounded" style="background: #92400e; color: white;">2</span>
                                <p class="text-xs font-semibold" style="color: #92400e;">PPA Swap (GM)</p>
                            </div>
                            <div class="space-y-1">
                                <div class="flex justify-between text-xs">
                                    <span style="color: #78350f;">Fixed @ $33.31</span>
                                    <span id="nwoh-fixed-payment" style="color: #22c55e;">+$0</span>
                                </div>
                                <div class="flex justify-between text-xs">
                                    <span style="color: #78350f;">Floating @ Hub</span>
                                    <span id="nwoh-floating-payment" style="color: #ef4444;">-$0</span>
                                </div>
                                <div class="flex justify-between text-xs pt-1 mt-1" style="border-top: 1px solid #fbbf24;">
                                    <span class="font-semibold" style="color: #92400e;">Net PPA</span>
                                    <span id="nwoh-net-ppa" class="font-bold" style="color: #92400e;">$0</span>
                                </div>
                            </div>
                            <div class="mt-2 pt-2 text-xs" style="border-top: 1px dashed #fbbf24; color: #78350f;">
                                Hub: <span id="nwoh-avg-hub">--</span>/MWh | Basis: <span id="nwoh-gwa-basis">--</span>
                            </div>
                        </div>

                        <!-- Arrow 2 -->
                        <div class="hidden md:flex items-center justify-center px-1" style="color: #94a3b8;">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M5 12h14M12 5l7 7-7 7"/>
                            </svg>
                        </div>

                        <!-- Step 3: Net Result -->
                        <div class="flex-1 p-3 rounded" style="background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%); border: 1px solid #34d399;">
                            <div class="flex items-center gap-2 mb-2">
                                <span class="text-xs font-bold px-1.5 py-0.5 rounded" style="background: #059669; color: white;">3</span>
                                <p class="text-xs font-semibold" style="color: #059669;">Net Result</p>
                            </div>
                            <div class="space-y-1">
                                <div class="flex justify-between text-xs">
                                    <span style="color: #047857;">PJM Revenue</span>
                                    <span id="nwoh-result-pjm" style="color: var(--skyvest-navy);">$0</span>
                                </div>
                                <div class="flex justify-between text-xs">
                                    <span style="color: #047857;">PPA Settlement</span>
                                    <span id="nwoh-result-ppa" style="color: var(--skyvest-navy);">$0</span>
                                </div>
                                <div class="flex justify-between pt-1 mt-1" style="border-top: 1px solid #34d399;">
                                    <span class="font-semibold" style="color: #059669;">Total PnL</span>
                                    <span id="nwoh-total-pnl" class="text-lg font-bold" style="color: #059669;">$0</span>
                                </div>
                            </div>
                            <div class="mt-2 pt-2 text-xs" style="border-top: 1px dashed #34d399; color: #047857;">
                                Realized: <span id="nwoh-realized-price" class="font-bold">--</span>/MWh all-in
                            </div>
                        </div>
                    </div>
                </div>

                <!-- ══════════ SECTION 4: DETAIL METRICS ══════════ -->
                <div class="p-3 rounded" style="background-color: #fafafa; border: 1px solid #e5e5e5;">
                    <p class="text-xs font-semibold mb-2" style="color: #666;">Period Details</p>
                    <div class="grid grid-cols-2 md:grid-cols-6 gap-3 text-center">
                        <div>
                            <p class="text-xs" style="color: #999;">Generation</p>
                            <p id="nwoh-detail-gen" class="font-bold" style="color: var(--skyvest-navy);">-- MWh</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #999;">DA Awarded</p>
                            <p id="nwoh-da-mwh" class="font-bold" style="color: var(--skyvest-navy);">-- MWh</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #999;">Avg DA LMP</p>
                            <p id="nwoh-avg-da-lmp" class="font-bold" style="color: var(--skyvest-navy);">$--</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #999;">Avg Node LMP</p>
                            <p id="nwoh-avg-node" class="font-bold" style="color: var(--skyvest-navy);">$--</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #999;">RT Sales</p>
                            <p id="nwoh-rt-sales-mwh" class="font-bold" style="color: #22c55e;">-- MWh</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #999;">RT Purchases</p>
                            <p id="nwoh-rt-purchase-mwh" class="font-bold" style="color: #ef4444;">-- MWh</p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Single Asset Detail Card (shown when specific asset selected) -->
            <div id="single-asset-detail" class="card rounded-sm p-3 mb-3" style="display: none;">
                <div class="flex justify-between items-start mb-2">
                    <div>
                        <p id="single-asset-name" class="text-lg font-semibold" style="color: var(--skyvest-navy);">Asset Name</p>
                        <p id="single-asset-type" class="text-xs" style="color: #999;">Type details</p>
                    </div>
                </div>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
                    <div>
                        <p class="text-xs" style="color: #666;">PnL</p>
                        <p id="single-asset-pnl" class="text-lg font-bold" style="color: var(--skyvest-navy);">$0.00</p>
                    </div>
                    <div>
                        <p class="text-xs" style="color: #666;">Volume</p>
                        <p id="single-asset-volume" class="text-lg font-bold" style="color: var(--skyvest-navy);">0 MWh</p>
                    </div>
                    <div>
                        <p class="text-xs" style="color: #666;">Realized Price</p>
                        <p id="single-asset-realized" class="text-lg font-bold" style="color: var(--skyvest-navy);">--</p>
                    </div>
                    <div>
                        <p class="text-xs" style="color: #666;">GWA Basis</p>
                        <p id="single-asset-basis" class="text-lg font-bold" style="color: var(--skyvest-navy);">--</p>
                    </div>
                </div>
                <!-- Holstein-specific fields -->
                <div id="holstein-extra" class="mt-3 pt-3" style="border-top: 1px solid #eee; display: none;">
                    <div class="grid grid-cols-2 gap-3">
                        <div>
                            <p class="text-xs" style="color: #666;">PPA Realized (87.5%)</p>
                            <p id="single-ppa-realized" class="text-lg font-bold" style="color: var(--skyvest-navy);">--</p>
                        </div>
                        <div>
                            <p class="text-xs" style="color: #666;">Merchant Realized (12.5%)</p>
                            <p id="single-merchant-realized" class="text-lg font-bold" style="color: var(--skyvest-navy);">--</p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- PnL View Selector -->
            <div class="card rounded-sm p-3 mb-3">
                <div class="flex justify-between items-center mb-3">
                    <h3 class="text-sm font-semibold" style="color: var(--skyvest-navy);">PnL by Period</h3>
                    <div class="flex gap-2 flex-wrap">
                        <button id="btn-daily" onclick="setPnlView('daily')" class="px-3 py-1 text-xs font-semibold rounded" style="background-color: var(--skyvest-blue); color: white;">Daily</button>
                        <button id="btn-monthly" onclick="setPnlView('monthly')" class="px-3 py-1 text-xs font-semibold rounded" style="background-color: #e5e5e5; color: var(--skyvest-navy);">Monthly</button>
                        <button id="btn-annual" onclick="setPnlView('annual')" class="px-3 py-1 text-xs font-semibold rounded" style="background-color: #e5e5e5; color: var(--skyvest-navy);">Annual</button>
                    </div>
                </div>
                <div id="pnl-table-container" style="max-height: 400px; overflow-y: auto;">
                    <table class="w-full text-sm">
                        <thead>
                            <tr style="border-bottom: 2px solid var(--skyvest-navy);">
                                <th class="text-left py-2 px-2" style="color: var(--skyvest-navy);">Period</th>
                                <th class="text-right py-2 px-2" style="color: var(--skyvest-navy);">PnL</th>
                                <th class="text-right py-2 px-2" style="color: var(--skyvest-navy);">GWA Basis</th>
                                <th class="text-right py-2 px-2" style="color: var(--skyvest-navy);">Volume (MWh)</th>
                                <th class="text-right py-2 px-2" style="color: var(--skyvest-navy);">Intervals</th>
                            </tr>
                        </thead>
                        <tbody id="pnl-table-body">
                            <tr><td colspan="4" class="text-center py-4" style="color: #999;">Loading PnL data...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- PnL Chart -->
            <div class="card rounded-sm p-3 mb-3">
                <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">PnL Trend</h3>
                <div id="pnl-chart-container"></div>
            </div>

            <!-- Worst Basis Intervals (PPA Exclusion Tracking) - Holstein Only -->
            <div class="card rounded-sm p-3 mb-3">
                <div class="flex justify-between items-center mb-2">
                    <h3 class="text-sm font-semibold" style="color: var(--skyvest-navy);">Worst Basis Intervals - Yesterday (PPA Exclusion Candidates)</h3>
                    <span class="text-xs" style="color: #999;">Gen × Basis (prior day only per contract)</span>
                </div>
                <div id="worst-basis-container" style="max-height: 200px; overflow-y: auto;">
                    <table class="w-full text-xs">
                        <thead>
                            <tr style="border-bottom: 1px solid #e5e5e5;">
                                <th class="text-left py-1 px-2" style="color: #666;">Date/Time</th>
                                <th class="text-left py-1 px-2" style="color: #666;">Asset</th>
                                <th class="text-right py-1 px-2" style="color: #666;">Basis</th>
                                <th class="text-right py-1 px-2" style="color: #666;">Volume</th>
                                <th class="text-right py-1 px-2" style="color: #666;">PnL Impact</th>
                            </tr>
                        </thead>
                        <tbody id="worst-basis-body">
                            <tr><td colspan="5" class="text-center py-2" style="color: #999;">Loading...</td></tr>
                        </tbody>
                    </table>
                </div>
                <div class="mt-2 pt-2" style="border-top: 1px solid #e5e5e5;">
                    <span class="text-xs" style="color: #666;">Potential savings if excluded: </span>
                    <span id="excludable-savings" class="text-sm font-bold" style="color: var(--skyvest-blue);">$0.00</span>
                </div>
            </div>
        </div>
    </div>

    <!-- Data Refresh Status Footer -->
    <div class="fixed bottom-0 left-0 right-0 bg-white border-t border-gray-200 px-4 py-2 text-xs" style="z-index: 100;">
        <div class="max-w-7xl mx-auto flex flex-wrap justify-between items-center gap-4">
            <div class="flex flex-wrap gap-6">
                <div class="flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
                    <span style="color: #666;"><strong>ERCOT/PJM LMP:</strong> 30s</span>
                    <span id="lmp-last-update" style="color: #999;"></span>
                </div>
                <div class="flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-blue-500"></span>
                    <span style="color: #666;"><strong>PnL Display:</strong> 60s</span>
                    <span id="pnl-last-update" style="color: #999;"></span>
                </div>
                <div class="flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-yellow-500"></span>
                    <span style="color: #666;"><strong>Tenaska API:</strong> 30min</span>
                    <span id="tenaska-last-update" style="color: #999;"></span>
                </div>
                <div class="flex items-center gap-2">
                    <span class="w-2 h-2 rounded-full bg-purple-500"></span>
                    <span style="color: #666;"><strong>Pharos API:</strong> 30min</span>
                    <span id="pharos-last-update" style="color: #999;"></span>
                </div>
            </div>
            <div style="color: #999;">
                <span id="current-time"></span>
            </div>
        </div>
    </div>
    <!-- Spacer to prevent content from being hidden behind fixed footer -->
    <div class="h-12"></div>

    <script>
        const API_URL = window.location.protocol + '//' + window.location.host + '/api/basis';
        
        async function fetchData() {
            try {
                const response = await fetch(API_URL);
                const data = await response.json();
                
                // Update ERCOT data
                document.getElementById('node1').textContent = data.node1_price ? '$' + data.node1_price.toFixed(2) : 'N/A';
                document.getElementById('node2').textContent = data.node2_price ? '$' + data.node2_price.toFixed(2) : 'N/A';
                document.getElementById('hub').textContent = data.hub_price ? '$' + data.hub_price.toFixed(2) : 'N/A';
                document.getElementById('basis1').textContent = data.basis1 ? '$' + data.basis1.toFixed(2) : 'N/A';
                document.getElementById('basis2').textContent = data.basis2 ? '$' + data.basis2.toFixed(2) : 'N/A';

                // Update PJM data
                document.getElementById('pjm-node').textContent = data.pjm_node_price ? '$' + data.pjm_node_price.toFixed(2) : 'N/A';
                document.getElementById('pjm-hub').textContent = data.pjm_hub_price ? '$' + data.pjm_hub_price.toFixed(2) : 'N/A';
                document.getElementById('pjm-basis').textContent = data.pjm_basis ? '$' + data.pjm_basis.toFixed(2) : 'N/A';

                // Apply status styling to ERCOT basis card 1
                if (data.status1) {
                    const card1 = document.getElementById('basis-card-1');
                    const style1 = getCardStyle(data.status1);
                    applyCardStyling(card1, style1, 'basis1', 'status1', data.status1);
                }

                // Apply status styling to ERCOT basis card 2
                if (data.status2) {
                    const card2 = document.getElementById('basis-card-2');
                    const style2 = getCardStyle(data.status2);
                    applyCardStyling(card2, style2, 'basis2', 'status2', data.status2);
                }

                // Apply status styling to PJM basis card
                if (data.pjm_status) {
                    const pjmCard = document.getElementById('pjm-basis-card');
                    const pjmStyle = getCardStyle(data.pjm_status);
                    applyCardStyling(pjmCard, pjmStyle, 'pjm-basis', 'pjm-status', data.pjm_status);
                }
                
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connected';
                connEl.style.backgroundColor = '#2291EB';
                connEl.style.color = 'white';

                // Render ERCOT charts
                if (data.history && data.history.length > 0) {
                    renderChart(data.history, 'chart-container-1', 'basis1');
                    renderChart(data.history, 'chart-container-2', 'basis2');
                }

                // Render PJM chart
                if (data.pjm_history && data.pjm_history.length > 0) {
                    renderPJMChart(data.pjm_history, 'pjm-chart-container');
                }
            } catch (error) {
                console.error('Error:', error);
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connection Error';
                connEl.style.backgroundColor = '#ef4444';
                connEl.style.color = 'white';
            }
        }

        // Get full card styling based on status
        function getCardStyle(status) {
            if (status === 'safe') {
                return {
                    bgColor: '#2291EB',      // SkyVest blue
                    textColor: 'white',
                    labelColor: 'rgba(255, 255, 255, 0.9)',
                    subtitleColor: 'rgba(255, 255, 255, 0.8)',
                    statusColor: 'white',
                    borderColor: '#1a7bc4',
                    glow: false
                };
            } else if (status === 'caution') {
                return {
                    bgColor: '#FFD966',      // SkyVest gold
                    textColor: '#0E2C51',    // Navy for contrast
                    labelColor: '#5a4a1f',
                    subtitleColor: '#6b5a2f',
                    statusColor: '#8B6914',  // Darker gold
                    borderColor: '#e6c44d',
                    glow: false
                };
            } else if (status === 'alert') {
                return {
                    bgColor: '#ef4444',      // Red alert
                    textColor: 'white',
                    labelColor: 'rgba(255, 255, 255, 0.95)',
                    subtitleColor: 'rgba(255, 255, 255, 0.85)',
                    statusColor: 'white',
                    borderColor: '#dc2626',   // Darker red
                    glow: true                // Enable pulse animation
                };
            } else {
                return {
                    bgColor: 'white',
                    textColor: 'var(--skyvest-navy)',
                    labelColor: '#666',
                    subtitleColor: '#999',
                    statusColor: '#666',
                    borderColor: '#e5e5e5',
                    glow: false
                };
            }
        }

        // Apply styling to a basis card
        function applyCardStyling(card, style, basisId, statusId, statusText) {
            // Card background and border
            card.style.backgroundColor = style.bgColor;
            card.style.borderColor = style.borderColor;

            // Apply or remove glow effect
            if (style.glow) {
                card.classList.add('alert-pulse');
                card.style.boxShadow = '0 0 20px rgba(239, 68, 68, 0.5)';
            } else {
                card.classList.remove('alert-pulse');
                card.style.boxShadow = '';
            }

            // Update all text elements
            const basisEl = document.getElementById(basisId);
            const statusEl = document.getElementById(statusId);
            const labelEl = document.getElementById(basisId + '-label');
            const subtitleEl = document.getElementById(basisId + '-subtitle');
            const statusLabelEl = document.getElementById(statusId + '-label');

            if (basisEl) basisEl.style.color = style.textColor;
            if (statusEl) {
                statusEl.textContent = statusText.toUpperCase();
                statusEl.style.color = style.statusColor;
            }
            if (labelEl) labelEl.style.color = style.labelColor;
            if (subtitleEl) subtitleEl.style.color = style.subtitleColor;
            if (statusLabelEl) statusLabelEl.style.color = style.labelColor;
        }

        function getStatusColor(status) {
            if (status === 'safe') return '#2291EB';
            if (status === 'caution') return '#FFD966';
            if (status === 'alert') return '#ef4444';
            return '#999';
        }

        function getStatusColorHex(status) {
            if (status === 'safe') return '#2291EB';
            if (status === 'caution') return '#FFD966';
            if (status === 'alert') return '#ef4444';
            return '#999';
        }
        
        function renderChart(history, containerId, basisField) {
            const container = document.getElementById(containerId);
            if (!container) return;
            
            container.innerHTML = '';
            
            const values = history.map(p => p[basisField]);
            const minVal = Math.min(...values);
            const maxVal = Math.max(...values);
            
            // Auto-scale based purely on actual data range with small padding
            const padding = Math.max(5, (maxVal - minVal) * 0.1); // 10% padding or minimum $5
            const yMin = Math.floor((minVal - padding) / 5) * 5;
            const yMax = Math.ceil((maxVal + padding) / 5) * 5;
            const yRange = yMax - yMin || 10;
            
            // Determine step size based on range
            let step = 5;
            if (yRange > 100) step = 20;
            else if (yRange > 50) step = 10;
            
            const chartWrapper = document.createElement('div');
            chartWrapper.style.display = 'flex';
            chartWrapper.style.flexDirection = 'column';
            chartWrapper.style.gap = '4px';
            
            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '60px 1fr';
            chart.style.gap = '8px';
            
            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '12px';
            yAxis.style.fontSize = '11px';
            yAxis.style.color = '#999';
            yAxis.style.fontWeight = '400';
            yAxis.style.borderRight = '1px solid #e5e5e5';

            for (let i = yMax; i >= yMin; i -= step) {
                const label = document.createElement('div');
                label.textContent = '$' + i;
                label.style.padding = '4px 0';
                yAxis.appendChild(label);
            }

            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'flex-end';
            bars.style.gap = '2px';
            bars.style.borderBottom = '1px solid #e5e5e5';
            bars.style.paddingBottom = '8px';
            bars.style.minHeight = '120px';
            bars.style.position = 'relative';

            // Add horizontal gridlines
            const gridContainer = document.createElement('div');
            gridContainer.style.position = 'absolute';
            gridContainer.style.width = '100%';
            gridContainer.style.height = '100%';
            gridContainer.style.pointerEvents = 'none';

            for (let i = yMax; i >= yMin; i -= step) {
                const gridLine = document.createElement('div');
                const position = ((i - yMin) / yRange) * 100;
                gridLine.style.position = 'absolute';
                gridLine.style.bottom = position + '%';
                gridLine.style.width = '100%';
                gridLine.style.height = '1px';
                gridLine.style.backgroundColor = '#f5f5f5';
                gridContainer.appendChild(gridLine);
            }
            bars.appendChild(gridContainer);

            history.forEach((point, idx) => {
                const basisValue = point[basisField];
                const heightPercent = ((basisValue - yMin) / yRange) * 100;
                const statusField = basisField === 'basis1' ? 'status1' : 'status2';
                const color = getStatusColorHex(point[statusField]);

                const time = new Date(point.time);
                const timeStr = time.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                const bar = document.createElement('div');
                bar.style.flex = '1';
                bar.style.height = Math.max(heightPercent, 5) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.85';
                bar.style.borderRadius = '2px 2px 0 0';
                bar.style.minHeight = '5px';
                bar.style.position = 'relative';
                bar.style.transition = 'opacity 0.2s';
                bar.title = timeStr + ': $' + basisValue.toFixed(2);
                bar.addEventListener('mouseenter', () => bar.style.opacity = '1');
                bar.addEventListener('mouseleave', () => bar.style.opacity = '0.85');
                bars.appendChild(bar);
            });
            
            chart.appendChild(yAxis);
            chart.appendChild(bars);
            
            // Add time labels
            const timeLabels = document.createElement('div');
            timeLabels.style.display = 'grid';
            timeLabels.style.gridTemplateColumns = '60px 1fr';
            timeLabels.style.gap = '8px';
            timeLabels.style.marginTop = '4px';
            
            const spacer = document.createElement('div');
            
            const timeContainer = document.createElement('div');
            timeContainer.style.display = 'flex';
            timeContainer.style.justifyContent = 'space-between';
            timeContainer.style.fontSize = '10px';
            timeContainer.style.color = '#999';
            timeContainer.style.paddingLeft = '4px';
            timeContainer.style.paddingRight = '4px';
            timeContainer.style.fontWeight = '400';
            timeContainer.style.textTransform = 'uppercase';
            timeContainer.style.letterSpacing = '0.05em';

            if (history.length > 0) {
                const firstTime = new Date(history[0].time);
                const lastTime = new Date(history[history.length - 1].time);

                const startLabel = document.createElement('span');
                startLabel.textContent = firstTime.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                const endLabel = document.createElement('span');
                endLabel.textContent = lastTime.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                timeContainer.appendChild(startLabel);
                timeContainer.appendChild(endLabel);
            }
            
            timeLabels.appendChild(spacer);
            timeLabels.appendChild(timeContainer);
            
            chartWrapper.appendChild(chart);
            chartWrapper.appendChild(timeLabels);
            container.appendChild(chartWrapper);
        }

        function renderPJMChart(history, containerId) {
            const container = document.getElementById(containerId);
            if (!container) return;

            container.innerHTML = '';

            if (!history || history.length === 0) {
                container.innerHTML = '<p style="color: #999; text-align: center; padding: 40px;">No data available</p>';
                return;
            }

            // Filter to show only last 4 hours of data (48 points at 5-min intervals)
            // This keeps the chart focused on recent data
            const recentHistory = history.slice(-48);

            const chartWrapper = document.createElement('div');

            // Calculate Y-axis range
            const basisValues = recentHistory.map(p => p.basis);
            const minBasis = Math.min(...basisValues);
            const maxBasis = Math.max(...basisValues);
            const padding = Math.max(10, (maxBasis - minBasis) * 0.1);
            const yMin = Math.floor((minBasis - padding) / 10) * 10;
            const yMax = Math.ceil((maxBasis + padding) / 10) * 10;
            const yRange = yMax - yMin;
            const step = Math.max(10, Math.ceil(yRange / 5 / 10) * 10);

            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '60px 1fr';
            chart.style.gap = '8px';

            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '12px';
            yAxis.style.fontSize = '11px';
            yAxis.style.color = '#999';
            yAxis.style.fontWeight = '400';
            yAxis.style.borderRight = '1px solid #e5e5e5';

            for (let i = yMax; i >= yMin; i -= step) {
                const label = document.createElement('div');
                label.textContent = '$' + i;
                label.style.padding = '4px 0';
                yAxis.appendChild(label);
            }

            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'flex-end';
            bars.style.gap = '2px';
            bars.style.borderBottom = '1px solid #e5e5e5';
            bars.style.paddingBottom = '8px';
            bars.style.minHeight = '120px';
            bars.style.position = 'relative';

            // Add horizontal gridlines
            const gridContainer = document.createElement('div');
            gridContainer.style.position = 'absolute';
            gridContainer.style.width = '100%';
            gridContainer.style.height = '100%';
            gridContainer.style.pointerEvents = 'none';

            for (let i = yMax; i >= yMin; i -= step) {
                const gridLine = document.createElement('div');
                const position = ((i - yMin) / yRange) * 100;
                gridLine.style.position = 'absolute';
                gridLine.style.bottom = position + '%';
                gridLine.style.width = '100%';
                gridLine.style.height = '1px';
                gridLine.style.backgroundColor = '#f5f5f5';
                gridContainer.appendChild(gridLine);
            }
            bars.appendChild(gridContainer);

            recentHistory.forEach((point, idx) => {
                const basisValue = point.basis;
                const heightPercent = ((basisValue - yMin) / yRange) * 100;
                const color = getStatusColorHex(point.status);

                const time = new Date(point.time);
                const timeStr = time.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                const bar = document.createElement('div');
                bar.style.flex = '1';
                bar.style.height = Math.max(heightPercent, 5) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.85';
                bar.style.borderRadius = '2px 2px 0 0';
                bar.style.minHeight = '5px';
                bar.style.position = 'relative';
                bar.style.transition = 'opacity 0.2s';
                bar.title = timeStr + ': $' + basisValue.toFixed(2);
                bar.addEventListener('mouseenter', () => bar.style.opacity = '1');
                bar.addEventListener('mouseleave', () => bar.style.opacity = '0.85');
                bars.appendChild(bar);
            });

            chart.appendChild(yAxis);
            chart.appendChild(bars);

            // Add time labels
            const timeLabels = document.createElement('div');
            timeLabels.style.display = 'grid';
            timeLabels.style.gridTemplateColumns = '60px 1fr';
            timeLabels.style.gap = '8px';
            timeLabels.style.marginTop = '4px';

            const spacer = document.createElement('div');

            const timeContainer = document.createElement('div');
            timeContainer.style.display = 'flex';
            timeContainer.style.justifyContent = 'space-between';
            timeContainer.style.fontSize = '10px';
            timeContainer.style.color = '#999';
            timeContainer.style.paddingLeft = '4px';
            timeContainer.style.paddingRight = '4px';
            timeContainer.style.fontWeight = '400';
            timeContainer.style.textTransform = 'uppercase';
            timeContainer.style.letterSpacing = '0.05em';

            if (recentHistory.length > 0) {
                const firstTime = new Date(recentHistory[0].time);
                const lastTime = new Date(recentHistory[recentHistory.length - 1].time);

                const startLabel = document.createElement('span');
                startLabel.textContent = firstTime.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                const endLabel = document.createElement('span');
                endLabel.textContent = lastTime.toLocaleTimeString('en-US', {
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false,
                    timeZone: 'America/New_York'
                });

                timeContainer.appendChild(startLabel);
                timeContainer.appendChild(endLabel);
            }

            timeLabels.appendChild(spacer);
            timeLabels.appendChild(timeContainer);

            chartWrapper.appendChild(chart);
            chartWrapper.appendChild(timeLabels);
            container.appendChild(chartWrapper);
        }

        // ============================================================================
        // PnL Functions
        // ============================================================================
        const PNL_API_URL = window.location.protocol + '//' + window.location.host + '/api/pnl';
        let currentPnlView = 'daily';
        let pnlData = null;

        // Toggle state variables
        let currentPeriod = 'ytd';  // 'daily', 'mtd', 'ytd'
        let currentAssetFilter = 'all';  // 'all', 'BKI', 'BKII', 'HOLSTEIN'
        let selectedDate = null;  // Selected start date for Daily view (YYYY-MM-DD format)
        let selectedEndDate = null;  // Selected end date for Daily view (YYYY-MM-DD format)

        // Initialize date picker with today's date
        function initDatePicker() {
            const today = new Date().toISOString().split('T')[0];
            const startPicker = document.getElementById('date-picker-start');
            const endPicker = document.getElementById('date-picker-end');

            // Default both to today
            startPicker.value = today;
            endPicker.value = today;
            startPicker.max = today;  // Can't select future dates
            endPicker.max = today;
            selectedDate = today;
            selectedEndDate = today;
        }

        function setDateRange() {
            const startPicker = document.getElementById('date-picker-start');
            const endPicker = document.getElementById('date-picker-end');

            selectedDate = startPicker.value;
            selectedEndDate = endPicker.value;

            // Ensure end >= start
            if (selectedEndDate < selectedDate) {
                endPicker.value = selectedDate;
                selectedEndDate = selectedDate;
            }

            // Auto-switch to Daily view when a date is selected
            if (currentPeriod !== 'daily') {
                setPeriod('daily');
            } else {
                // If already on daily, just refresh the display
                updateFilteredDisplay();
                updateAssetCards();
                if (currentAssetFilter === 'NWOH') {
                    updateNwohDetailCard();
                }
            }
        }

        function setSelectedDate(date) {
            // Legacy function for compatibility - sets single date
            selectedDate = date;
            selectedEndDate = date;
            document.getElementById('date-picker-start').value = date;
            document.getElementById('date-picker-end').value = date;
            setDateRange();
        }

        function resetToToday() {
            const today = new Date().toISOString().split('T')[0];
            document.getElementById('date-picker-start').value = today;
            document.getElementById('date-picker-end').value = today;
            selectedDate = today;
            selectedEndDate = today;
            // Refresh if on daily view
            if (currentPeriod === 'daily') {
                updateFilteredDisplay();
                updateAssetCards();
                if (currentAssetFilter === 'NWOH') {
                    updateNwohDetailCard();
                }
            }
        }

        // Helper function to check if a date is within the selected range
        function isDateInRange(dateStr) {
            if (!selectedDate || !selectedEndDate) return true;
            return dateStr >= selectedDate && dateStr <= selectedEndDate;
        }

        // Helper function to get all dates in the selected range
        function getDatesInRange() {
            if (!selectedDate || !selectedEndDate) return [];
            const dates = [];
            let current = new Date(selectedDate);
            const end = new Date(selectedEndDate);
            while (current <= end) {
                dates.push(current.toISOString().split('T')[0]);
                current.setDate(current.getDate() + 1);
            }
            return dates;
        }

        function setPeriod(period) {
            currentPeriod = period;

            // Update button styles
            ['daily', 'mtd', 'ytd'].forEach(p => {
                const btn = document.getElementById('period-' + p);
                if (p === period) {
                    btn.style.backgroundColor = 'var(--skyvest-navy)';
                    btn.style.color = 'white';
                } else {
                    btn.style.backgroundColor = 'white';
                    btn.style.color = 'var(--skyvest-navy)';
                }
            });

            // Update labels
            let label = { 'daily': 'Daily', 'mtd': 'MTD', 'ytd': 'YTD' }[period];

            // For daily view with date range, show the range in the label
            if (period === 'daily' && selectedDate && selectedEndDate && selectedDate !== selectedEndDate) {
                const startParts = selectedDate.split('-');
                const endParts = selectedEndDate.split('-');
                const startFormatted = startParts[1] + '/' + startParts[2];
                const endFormatted = endParts[1] + '/' + endParts[2];
                label = startFormatted + ' - ' + endFormatted;
            } else if (period === 'daily' && selectedDate) {
                const parts = selectedDate.split('-');
                label = parts[1] + '/' + parts[2] + '/' + parts[0].slice(2);
            }

            document.getElementById('pnl-label').textContent = label;
            document.getElementById('volume-label').textContent = label;

            updateFilteredDisplay();
            updateAssetCards();  // Update asset cards to reflect selected period
            if (currentAssetFilter === 'NWOH') {
                updateNwohDetailCard();  // Update NWOH detail card if visible
            }
        }

        function setAssetFilter(asset) {
            currentAssetFilter = asset;

            // Update button styles
            ['all', 'BKI', 'BKII', 'HOLSTEIN', 'NWOH'].forEach(a => {
                const btn = document.getElementById('asset-' + a);
                if (btn) {
                    if (a === asset) {
                        btn.style.backgroundColor = 'var(--skyvest-navy)';
                        btn.style.color = 'white';
                    } else {
                        btn.style.backgroundColor = 'white';
                        btn.style.color = 'var(--skyvest-navy)';
                    }
                }
            });

            // Show/hide asset cards vs single asset detail vs NWOH detail
            const assetCardsContainer = document.getElementById('asset-cards-container');
            const singleAssetDetail = document.getElementById('single-asset-detail');
            const nwohDetailCard = document.getElementById('nwoh-detail-card');

            if (asset === 'all') {
                assetCardsContainer.style.display = '';
                singleAssetDetail.style.display = 'none';
                nwohDetailCard.style.display = 'none';
            } else if (asset === 'NWOH') {
                assetCardsContainer.style.display = 'none';
                singleAssetDetail.style.display = 'none';
                nwohDetailCard.style.display = '';
                updateNwohDetailCard();
            } else {
                assetCardsContainer.style.display = 'none';
                singleAssetDetail.style.display = '';
                nwohDetailCard.style.display = 'none';
            }

            updateFilteredDisplay();
            updatePnlTable();  // Keep PnL table in sync with asset filter
        }

        // Update NWOH detailed card with invoice-style metrics
        function updateNwohDetailCard() {
            if (!pnlData || !pnlData.assets?.NWOH) return;

            const nwoh = pnlData.assets.NWOH;
            let data = {};

            // Update viewing date indicator
            const viewingDateEl = document.getElementById('nwoh-viewing-date');
            const today = new Date().toISOString().split('T')[0];

            // Helper to aggregate NWOH daily data across a date filter
            function aggregateNwohDays(dailyPnl, filterFn) {
                const agg = {pnl: 0, volume: 0, da_revenue: 0, da_mwh: 0, rt_sales_revenue: 0, rt_sales_mwh: 0,
                    rt_purchase_cost: 0, rt_purchase_mwh: 0, da_lmp_product: 0, rt_lmp_product: 0,
                    hub_lmp_product: 0, hub_volume: 0, ppa_fixed_payment: 0, ppa_floating_payment: 0, ppa_net_settlement: 0};
                Object.entries(dailyPnl || {}).forEach(([day, d]) => {
                    if (filterFn(day)) {
                        agg.pnl += d.pnl || 0;
                        agg.volume += d.volume || 0;
                        agg.da_revenue += d.da_revenue || 0;
                        agg.da_mwh += d.da_mwh || 0;
                        agg.rt_sales_revenue += d.rt_sales_revenue || 0;
                        agg.rt_sales_mwh += d.rt_sales_mwh || 0;
                        agg.rt_purchase_cost += d.rt_purchase_cost || 0;
                        agg.rt_purchase_mwh += d.rt_purchase_mwh || 0;
                        agg.da_lmp_product += d.da_lmp_product || 0;
                        agg.rt_lmp_product += d.rt_lmp_product || 0;
                        agg.hub_lmp_product += d.hub_lmp_product || 0;
                        agg.hub_volume += d.hub_volume || 0;
                        agg.ppa_fixed_payment += d.ppa_fixed_payment || 0;
                        agg.ppa_floating_payment += d.ppa_floating_payment || 0;
                        agg.ppa_net_settlement += d.ppa_net_settlement || 0;
                    }
                });
                if (agg.da_mwh > 0) agg.avg_da_price = agg.da_lmp_product / agg.da_mwh;
                if (agg.volume > 0) agg.avg_rt_price = agg.rt_lmp_product / agg.volume;
                if (agg.hub_volume > 0) agg.avg_hub_price = agg.hub_lmp_product / agg.hub_volume;
                if (agg.volume > 0) agg.gwa_basis = (agg.hub_lmp_product - agg.rt_lmp_product) / agg.volume;
                return agg;
            }

            // Get data based on current period
            if (currentPeriod === 'daily') {
                const startDate = selectedDate || today;
                const endDate = selectedEndDate || today;
                const isDateRange = startDate !== endDate;

                if (isDateRange) {
                    // Aggregate across date range
                    data = aggregateNwohDays(nwoh.daily_pnl, day => day >= startDate && day <= endDate);
                    viewingDateEl.textContent = 'Viewing: ' + startDate + ' to ' + endDate;
                    viewingDateEl.style.display = '';
                } else {
                    // Single day
                    const days = Object.keys(nwoh.daily_pnl || {}).sort();
                    const targetDay = startDate;
                    data = nwoh.daily_pnl?.[targetDay] || {};

                    // For today: always use nwohStatus (more current than hourly_revenue_estimate)
                    if (targetDay === today && nwohStatus?.today) {
                        const t = nwohStatus.today;
                        data = Object.assign({}, data, {
                            da_revenue: t.da_revenue || data.da_revenue || 0,
                            da_mwh: t.total_da_mwh || data.da_mwh || 0,
                            volume: t.total_gen_mwh || data.volume || 0,
                            avg_da_price: t.avg_da_price || data.avg_da_price || 0,
                            avg_rt_price: t.avg_rt_price || data.avg_rt_price || 0,
                            avg_hub_price: t.avg_hub_price || data.avg_hub_price || 0,
                            gwa_basis: t.gwa_basis,
                            rt_sales_revenue: t.rt_sales_revenue || data.rt_sales_revenue || 0,
                            rt_purchase_cost: t.rt_purchase_cost || data.rt_purchase_cost || 0,
                            rt_sales_mwh: t.rt_sales_mwh || data.rt_sales_mwh || 0,
                            rt_purchase_mwh: t.rt_purchase_mwh || data.rt_purchase_mwh || 0,
                            ppa_fixed_payment: t.ppa_fixed_payment,
                            ppa_floating_payment: t.ppa_floating_payment,
                            ppa_net_settlement: t.ppa_net_settlement,
                        });
                        // Don't set data.pnl - let Total PnL be computed from PJM + PPA downstream
                    }

                    if (targetDay && targetDay !== today) {
                        const dateObj = new Date(targetDay + 'T12:00:00');
                        viewingDateEl.textContent = 'Viewing: ' + dateObj.toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'});
                        viewingDateEl.style.display = '';
                    } else {
                        viewingDateEl.style.display = 'none';
                    }
                }
            } else if (currentPeriod === 'mtd') {
                viewingDateEl.style.display = 'none';
                const currentMonth = new Date().getFullYear() + '-' + String(new Date().getMonth() + 1).padStart(2, '0');
                data = nwoh.monthly_pnl?.[currentMonth] || {};
            } else {
                // YTD - aggregate all daily data for current year
                viewingDateEl.style.display = 'none';
                const currentYear = new Date().getFullYear().toString();
                data = aggregateNwohDays(nwoh.daily_pnl, day => day.startsWith(currentYear));
            }

            const formatCurrency = (val) => {
                if (val === null || val === undefined) return '--';
                return '$' + Math.abs(val).toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0});
            };

            const formatNumber = (val, decimals = 2) => {
                if (val === null || val === undefined) return '--';
                return val.toLocaleString('en-US', {minimumFractionDigits: decimals, maximumFractionDigits: decimals});
            };

            // PJM Market Settlement
            const daRevenue = data.da_revenue || 0;
            const rtSalesRev = data.rt_sales_revenue || 0;
            const rtPurchaseCost = data.rt_purchase_cost || 0;
            const totalPjmRevenue = daRevenue + rtSalesRev - rtPurchaseCost;

            document.getElementById('nwoh-da-revenue').textContent = formatCurrency(daRevenue);
            document.getElementById('nwoh-avg-da-lmp').textContent = '$' + formatNumber(data.avg_da_price);
            document.getElementById('nwoh-rt-sales').textContent = '+' + formatCurrency(rtSalesRev);
            document.getElementById('nwoh-rt-sales-mwh').textContent = formatNumber(data.rt_sales_mwh || 0) + ' MWh';
            document.getElementById('nwoh-rt-purchase').textContent = '-' + formatCurrency(rtPurchaseCost);
            document.getElementById('nwoh-rt-purchase-mwh').textContent = formatNumber(data.rt_purchase_mwh || 0) + ' MWh';
            document.getElementById('nwoh-total-pjm').textContent = formatCurrency(totalPjmRevenue);
            document.getElementById('nwoh-avg-rt-lmp').textContent = '$' + formatNumber(data.avg_rt_price);

            // Generation & Pricing
            const genMwh = data.volume || 0;
            const daMwh = data.da_mwh || 0;
            const avgNodeLmp = data.avg_rt_price || 0;
            const avgHubLmp = data.avg_hub_price || avgNodeLmp;  // Fallback to node if no hub
            const gwaBasis = (data.gwa_basis !== undefined && data.gwa_basis !== null) ? data.gwa_basis : (avgHubLmp - avgNodeLmp);

            document.getElementById('nwoh-gen-mwh').textContent = formatNumber(genMwh);
            document.getElementById('nwoh-detail-gen').textContent = formatNumber(genMwh) + ' MWh';
            document.getElementById('nwoh-da-mwh').textContent = formatNumber(daMwh) + ' MWh';
            document.getElementById('nwoh-avg-node').textContent = '$' + formatNumber(avgNodeLmp);
            document.getElementById('nwoh-avg-hub').textContent = '$' + formatNumber(avgHubLmp);
            const basisEl = document.getElementById('nwoh-gwa-basis');
            basisEl.textContent = (gwaBasis >= 0 ? '+' : '') + '$' + formatNumber(gwaBasis);
            basisEl.style.color = gwaBasis >= 0 ? '#22c55e' : '#ef4444';

            // PPA Settlement - use pre-calculated values when available (more accurate for aggregated periods)
            // Fixed: GM pays us $33.31/MWh (revenue)
            // Floating: We pay GM Hub LMP (cost)
            // Net = Fixed - Floating (positive when hub < $33.31)
            const ppaPrice = 33.31;
            const fixedPayment = data.ppa_fixed_payment || (genMwh * ppaPrice);
            const floatingPayment = data.ppa_floating_payment || (genMwh * avgHubLmp);
            const netPpaSettlement = data.ppa_net_settlement !== undefined ? data.ppa_net_settlement : (fixedPayment - floatingPayment);

            document.getElementById('nwoh-fixed-payment').textContent = '+' + formatCurrency(fixedPayment);
            document.getElementById('nwoh-floating-payment').textContent = '-' + formatCurrency(floatingPayment);
            const netEl = document.getElementById('nwoh-net-ppa');
            netEl.textContent = (netPpaSettlement >= 0 ? '+' : '-') + formatCurrency(Math.abs(netPpaSettlement));
            netEl.style.color = netPpaSettlement >= 0 ? '#22c55e' : '#ef4444';

            // Total PnL = PJM Revenue + PPA Settlement (use backend value when available)
            const totalRevenue = data.pnl || (totalPjmRevenue + netPpaSettlement);
            // Realized Price = Total PnL / Generation
            const realizedPrice = genMwh > 0 ? totalRevenue / genMwh : 0;
            document.getElementById('nwoh-realized-price').textContent = '$' + formatNumber(realizedPrice);

            // Update Settlement Flow result section
            document.getElementById('nwoh-result-pjm').textContent = formatCurrency(totalPjmRevenue);
            document.getElementById('nwoh-result-ppa').textContent = (netPpaSettlement >= 0 ? '+' : '-') + formatCurrency(Math.abs(netPpaSettlement));
            const totalPnlEl = document.getElementById('nwoh-total-pnl');
            totalPnlEl.textContent = (totalRevenue >= 0 ? '' : '-') + formatCurrency(Math.abs(totalRevenue));
            totalPnlEl.style.color = totalRevenue >= 0 ? '#059669' : '#ef4444';

            // Update DA Performance & Awards section from nwohStatus
            updateNwohDaSection();
        }

        function updateNwohDaSection() {
            if (!nwohStatus) return;

            const formatNumber = (val, decimals = 1) => {
                if (val === null || val === undefined) return '--';
                return val.toLocaleString('en-US', {minimumFractionDigits: decimals, maximumFractionDigits: decimals});
            };

            const formatCurrency = (val) => {
                if (val === null || val === undefined) return '$--';
                return '$' + Math.abs(val).toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0});
            };

            // Update price cap warning in detail card
            const detailWarning = document.getElementById('nwoh-detail-cap-warning');
            const detailCapInfo = document.getElementById('nwoh-detail-cap-info');
            if (nwohStatus.price_caps?.is_capped) {
                detailWarning.style.display = 'block';
                const capList = nwohStatus.price_caps.caps.map(c => 'HE' + c.hour_ending + ': $' + c.energy_price).join(', ');
                detailCapInfo.textContent = 'Capped hours: ' + capList;
            } else {
                detailWarning.style.display = 'none';
            }

            // Get today's data
            const today = nwohStatus.today || {};
            const daCommitment = today.da_commitment_mwh || 0;
            const actualGen = today.actual_gen_mwh || 0;
            const deviation = today.deviation_mwh || 0;
            const hoursWithAwards = today.hours_with_awards || 0;
            const hourlyBreakdown = today.hourly_breakdown || [];

            // Update today's date label and current hour
            const todayDate = new Date();
            const currentHourEnding = todayDate.getHours() + 1;
            document.getElementById('nwoh-today-date').textContent = todayDate.toLocaleDateString('en-US', {month: 'short', day: 'numeric'});
            document.getElementById('nwoh-current-hour').textContent = 'HE ' + currentHourEnding;
            document.getElementById('nwoh-hours-with-awards').textContent = '(' + hoursWithAwards + ' hrs awarded)';

            // Update summary text
            document.getElementById('nwoh-today-da-commitment').textContent = formatNumber(daCommitment) + ' MWh';
            document.getElementById('nwoh-today-actual-gen').textContent = formatNumber(actualGen) + ' MWh';

            // Build hourly strip chart
            const chartEl = document.getElementById('nwoh-hourly-chart');
            const labelsEl = document.getElementById('nwoh-hourly-labels');
            const tooltipEl = document.getElementById('nwoh-hour-tooltip');
            chartEl.innerHTML = '';
            labelsEl.innerHTML = '';

            // Find max MW for scaling bars
            const maxMw = Math.max(...hourlyBreakdown.map(h => Math.max(h.da_mw || 0, h.gen_mw || 0)), 1);
            console.log('[NWOH Chart] maxMw:', maxMw, 'breakdown sample HE1:', hourlyBreakdown[0], 'HE14:', hourlyBreakdown[13]);

            hourlyBreakdown.forEach(hour => {
                const he = hour.he;
                const daMw = hour.da_mw || 0;
                const genMw = hour.gen_mw || 0;
                const dev = hour.deviation_mw || 0;
                const status = hour.status;

                // Bar container for this hour
                const barContainer = document.createElement('div');
                barContainer.style.cssText = 'flex:1; position:relative; cursor:pointer; min-width:0; height:100%;';

                // Color based on status
                let genColor, daColor;
                switch (status) {
                    case 'over':    genColor = '#22c55e'; daColor = '#bbf7d0'; break;
                    case 'matched': genColor = 'var(--skyvest-blue)'; daColor = '#bfdbfe'; break;
                    case 'under':   genColor = '#ef4444'; daColor = '#fecaca'; break;
                    case 'future':  genColor = 'transparent'; daColor = '#e2e8f0'; break;
                    case 'pending': genColor = '#fbbf24'; daColor = '#fef3c7'; break;
                    default:        genColor = '#94a3b8'; daColor = '#e2e8f0'; break;
                }

                // DA commitment bar (background)
                const daPct = maxMw > 0 ? (daMw / maxMw) * 100 : 0;
                const genPct = maxMw > 0 ? (genMw / maxMw) * 100 : 0;

                const daBar = document.createElement('div');
                daBar.style.cssText = 'width:100%; border-radius:2px 2px 0 0; position:absolute; bottom:0; background:' + daColor + '; height:' + daPct + '%;';

                const genBar = document.createElement('div');
                genBar.style.cssText = 'width:100%; border-radius:2px 2px 0 0; position:absolute; bottom:0; background:' + genColor + '; height:' + genPct + '%; z-index:1;';

                // Current hour indicator
                if (he === currentHourEnding) {
                    barContainer.style.outline = '2px solid var(--skyvest-navy)';
                    barContainer.style.borderRadius = '2px';
                    barContainer.style.outlineOffset = '-1px';
                }

                barContainer.appendChild(daBar);
                barContainer.appendChild(genBar);

                // Tooltip on hover
                barContainer.addEventListener('mouseenter', function(e) {
                    let tipHtml = '<strong>HE ' + he + '</strong>';
                    if (status === 'future') {
                        tipHtml += '<br>DA Award: ' + daMw.toFixed(1) + ' MW';
                        tipHtml += '<br>DA LMP: $' + (hour.da_lmp || 0).toFixed(2);
                        tipHtml += '<br><span style="color:#94a3b8">Awaiting generation</span>';
                    } else if (daMw === 0 && genMw === 0) {
                        tipHtml += '<br><span style="color:#94a3b8">No award</span>';
                    } else {
                        tipHtml += '<br>DA Award: ' + daMw.toFixed(1) + ' MW @ $' + (hour.da_lmp || 0).toFixed(2);
                        tipHtml += '<br>Actual: ' + genMw.toFixed(1) + ' MW';
                        tipHtml += '<br>Deviation: <span style="color:' + (dev >= 0 ? '#4ade80' : '#f87171') + '">' + (dev >= 0 ? '+' : '') + dev.toFixed(1) + ' MW</span>';
                        tipHtml += '<br>RT LMP: $' + (hour.rt_lmp || 0).toFixed(2);
                        const rtRev = hour.rt_revenue || 0;
                        if (Math.abs(rtRev) > 0.01) {
                            tipHtml += '<br>RT $: <span style="color:' + (rtRev >= 0 ? '#4ade80' : '#f87171') + '">' + (rtRev >= 0 ? '+' : '-') + '$' + Math.abs(rtRev).toFixed(0) + '</span>';
                        }
                    }
                    tooltipEl.innerHTML = tipHtml;
                    tooltipEl.style.display = 'block';
                });
                barContainer.addEventListener('mousemove', function(e) {
                    tooltipEl.style.left = (e.clientX + 12) + 'px';
                    tooltipEl.style.top = (e.clientY - 10) + 'px';
                });
                barContainer.addEventListener('mouseleave', function() {
                    tooltipEl.style.display = 'none';
                });

                chartEl.appendChild(barContainer);

                // Hour label (show every other to avoid crowding)
                const label = document.createElement('div');
                label.style.cssText = 'flex:1; text-align:center; font-size:8px; color:#94a3b8; min-width:0; overflow:hidden;';
                label.textContent = (he % 2 === 0 || he === 1) ? he : '';
                labelsEl.appendChild(label);
            });

            // Deviation summary text
            const deviationText = document.getElementById('nwoh-deviation-text');
            deviationText.innerHTML = 'RT Deviation: <strong style=\"color: ' + (deviation >= 0 ? '#22c55e' : '#ef4444') + '\">' + (deviation >= 0 ? '+' : '') + formatNumber(deviation) + ' MWh</strong>';

            const deviationStatus = document.getElementById('nwoh-deviation-status');
            if (deviation > 0) {
                deviationStatus.innerHTML = '<span style=\"color: #22c55e;\">Over-generated (RT sales)</span>';
            } else if (deviation < 0) {
                deviationStatus.innerHTML = '<span style=\"color: #ef4444;\">Under-generated (RT purchase)</span>';
            } else {
                deviationStatus.textContent = 'Matched DA';
            }

            // Today's revenue summary - use nwohStatus totals (computed from hourly breakdown)
            const todayDaRev = today.da_revenue || 0;
            const todayRtRev = today.rt_revenue || 0;
            const todayNetRev = today.net_revenue || 0;

            document.getElementById('nwoh-today-da-rev').textContent = formatCurrency(todayDaRev);
            const rtNetEl = document.getElementById('nwoh-today-rt-net');
            rtNetEl.textContent = (todayRtRev >= 0 ? '+' : '-') + formatCurrency(Math.abs(todayRtRev));
            rtNetEl.style.color = todayRtRev >= 0 ? '#22c55e' : '#ef4444';
            document.getElementById('nwoh-today-pjm-total').textContent = formatCurrency(todayNetRev);

            // Update tomorrow's DA awards
            const nextDay = nwohStatus.next_day_awards || {};
            const tomorrowMwh = nextDay.total_da_mwh || 0;
            const tomorrowHours = nextDay.hours_awarded || 0;
            const tomorrowAvgPrice = nextDay.avg_da_price || 0;
            const tomorrowExpectedRev = tomorrowMwh * tomorrowAvgPrice;

            // Tomorrow's date label
            const tomorrow = new Date(todayDate);
            tomorrow.setDate(tomorrow.getDate() + 1);
            document.getElementById('nwoh-tomorrow-date').textContent = tomorrow.toLocaleDateString('en-US', {month: 'short', day: 'numeric'});

            document.getElementById('nwoh-tomorrow-da-total').textContent = formatNumber(tomorrowMwh) + ' MWh';
            document.getElementById('nwoh-tomorrow-da-hours').textContent = tomorrowHours + ' / 24';
            document.getElementById('nwoh-tomorrow-da-price').textContent = '$' + formatNumber(tomorrowAvgPrice, 2);
            document.getElementById('nwoh-tomorrow-da-rev').textContent = formatCurrency(tomorrowExpectedRev);
        }

        // Helper to calculate realized price for a specific asset
        // Industry Standard: Realized Price = Total Revenue / Total Volume ($/MWh)
        // For PPA assets with basis exposure: Realized = PPA Price + (Basis Exposure % × GWA Basis)
        function calcRealizedPrice(assetKey, pnl, volume, gwaBasis, ppaRevenue, merchantRevenue, merchantVolume) {
            if (!volume || volume <= 0) return null;

            // BKI: 100% Merchant - Realized = Total Revenue / Volume
            if (assetKey === 'BKI') {
                return Math.round(pnl / volume * 100) / 100;
            }

            // BKII: 100% PPA @ $34, 50% basis exposure
            // Realized = PPA Price + (50% × GWA Basis)
            if (assetKey === 'BKII') {
                const basisEffect = gwaBasis !== null ? 0.5 * gwaBasis : 0;
                return Math.round((34 + basisEffect) * 100) / 100;
            }

            // Holstein: 87.5% PPA @ $35 with 100% basis, 12.5% Merchant
            // Blended = 0.875 × (PPA + Basis) + 0.125 × (Merchant Rev / Merchant Vol)
            if (assetKey === 'HOLSTEIN') {
                const ppaPrice = gwaBasis !== null ? 35 + gwaBasis : 35;
                const merchantPrice = merchantVolume > 0 ? merchantRevenue / merchantVolume : pnl / volume;
                return Math.round((0.875 * ppaPrice + 0.125 * merchantPrice) * 100) / 100;
            }

            // NWOH: 100% PPA @ $33.31 with hub settlement
            // Realized = (PJM Revenue + Net PPA) / Volume = pnl / volume
            if (assetKey === 'NWOH') {
                return Math.round(pnl / volume * 100) / 100;
            }

            // Default: Revenue / Volume
            return Math.round(pnl / volume * 100) / 100;
        }

        // Helper to aggregate daily data across a date range
        // Note: Does NOT calculate realized_price - that must be done per-asset by caller
        function aggregateDateRange(dailyPnl, startDate, endDate) {
            let totalPnl = 0, totalVolume = 0, totalBasisProduct = 0;
            let totalDaRevenue = 0, totalRtRevenue = 0;
            let totalPpaRevenue = 0, totalMerchantRevenue = 0, totalMerchantVolume = 0;
            let totalPjmGross = 0, totalPpaNet = 0;
            const dates = [];

            // Get all dates in range that have data
            for (const [dateKey, data] of Object.entries(dailyPnl || {})) {
                if (dateKey >= startDate && dateKey <= endDate) {
                    dates.push(dateKey);
                    totalPnl += data.pnl || 0;
                    totalVolume += data.volume || 0;
                    totalDaRevenue += data.da_revenue || 0;
                    totalRtRevenue += data.rt_revenue || 0;
                    totalPpaRevenue += data.ppa_revenue || 0;
                    totalMerchantRevenue += data.merchant_revenue || 0;
                    totalMerchantVolume += data.merchant_volume || 0;
                    totalPjmGross += data.pjm_gross_revenue || 0;
                    totalPpaNet += data.ppa_net_settlement || 0;
                    if (data.gwa_basis && data.volume) {
                        totalBasisProduct += data.gwa_basis * data.volume;
                    }
                }
            }

            const gwaBasis = totalVolume > 0 ? totalBasisProduct / totalVolume : null;

            return {
                pnl: totalPnl,
                volume: totalVolume,
                da_revenue: totalDaRevenue,
                rt_revenue: totalRtRevenue,
                gwa_basis: gwaBasis,
                ppa_revenue: totalPpaRevenue,
                merchant_revenue: totalMerchantRevenue,
                merchant_volume: totalMerchantVolume,
                pjm_gross_revenue: totalPjmGross,
                ppa_net_settlement: totalPpaNet,
                // realized_price must be calculated by caller based on asset type
                days: dates.length
            };
        }

        function updateFilteredDisplay() {
            if (!pnlData) return;

            // Use local date (not UTC) to match backend date keys
            const now = new Date();
            let today = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
            const currentMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
            const currentYear = now.getFullYear().toString();

            // For daily view, use date range if both dates are set
            let startDate = selectedDate || today;
            let endDate = selectedEndDate || today;
            const userSelectedDate = startDate;  // Preserve before fallback modifies it

            // If selected dates don't exist in aggregate, fall back to most recent available
            // (but only for non-NWOH assets; NWOH uses nwohStatus for today's data)
            if (currentPeriod === 'daily' && pnlData.daily_pnl) {
                const actualToday = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
                const availableDates = Object.keys(pnlData.daily_pnl).filter(d => d <= actualToday).sort();
                if (availableDates.length > 0) {
                    // If start date has no data, use most recent (unless NWOH today - nwohStatus has live data)
                    if (!pnlData.daily_pnl[startDate] && !(currentAssetFilter === 'NWOH' && startDate === today)) {
                        startDate = availableDates[availableDates.length - 1];
                        endDate = startDate;
                    }
                    // Ensure end date doesn't exceed available data
                    if (!pnlData.daily_pnl[endDate] && endDate > startDate) {
                        endDate = availableDates[availableDates.length - 1];
                    }
                }
            }

            // Check if this is a date range (multiple days)
            const isDateRange = currentPeriod === 'daily' && startDate !== endDate;

            let pnl = 0, volume = 0, realizedPrice = null, gwaBasis = null;
            let realizedPpaPrice = null, realizedMerchantPrice = null;

            if (currentAssetFilter === 'all') {
                // Aggregate all assets
                if (currentPeriod === 'daily') {
                    if (isDateRange) {
                        // Aggregate across date range
                        const rangeData = aggregateDateRange(pnlData.daily_pnl, startDate, endDate);
                        pnl = rangeData.pnl;
                        volume = rangeData.volume;
                        gwaBasis = rangeData.gwa_basis;
                    } else {
                        // Single day
                        const dayData = pnlData.daily_pnl?.[startDate];
                        pnl = dayData?.pnl || 0;
                        volume = dayData?.volume || 0;
                    }
                } else if (currentPeriod === 'mtd') {
                    const monthData = pnlData.monthly_pnl?.[currentMonth];
                    pnl = monthData?.pnl || 0;
                    volume = monthData?.volume || 0;
                } else {
                    // YTD - use current year's annual data, not total
                    const yearData = pnlData.annual_pnl?.[currentYear];
                    pnl = yearData?.pnl || 0;
                    volume = yearData?.volume || 0;
                }
                // Calculate aggregate realized price as weighted average across assets
                let totalWeightedPrice = 0;
                let totalVolumeForAvg = 0;
                const assets = pnlData.assets || {};
                for (const [key, asset] of Object.entries(assets)) {
                    if (key === 'UNKNOWN') continue;
                    let assetVol = 0, assetPrice = null;
                    if (currentPeriod === 'daily') {
                        const d = asset.daily_pnl?.[today];
                        assetVol = d?.volume || 0;
                        assetPrice = d?.realized_price || d?.realized_ppa_price;
                    } else if (currentPeriod === 'mtd') {
                        const m = asset.monthly_pnl?.[currentMonth];
                        assetVol = m?.volume || 0;
                        assetPrice = m?.realized_price || m?.realized_ppa_price;
                    } else {
                        assetVol = asset.total_volume || 0;
                        assetPrice = asset.realized_price || asset.realized_ppa_price;
                    }
                    if (assetPrice && assetVol > 0) {
                        totalWeightedPrice += assetPrice * assetVol;
                        totalVolumeForAvg += assetVol;
                    }
                }
                realizedPrice = totalVolumeForAvg > 0 ? Math.round(totalWeightedPrice / totalVolumeForAvg * 100) / 100 : null;

                // Calculate aggregate GWA basis as weighted average across assets
                let totalWeightedBasis = 0;
                let totalVolumeForBasis = 0;
                for (const [key, asset] of Object.entries(assets)) {
                    if (key === 'UNKNOWN') continue;
                    let assetVol = 0, assetBasis = null;
                    if (currentPeriod === 'daily') {
                        const d = asset.daily_pnl?.[today];
                        assetVol = d?.volume || 0;
                        assetBasis = d?.gwa_basis;
                    } else if (currentPeriod === 'mtd') {
                        const m = asset.monthly_pnl?.[currentMonth];
                        assetVol = m?.volume || 0;
                        assetBasis = m?.gwa_basis;
                    } else {
                        assetVol = asset.total_volume || 0;
                        assetBasis = asset.gwa_basis;
                    }
                    if (assetBasis !== null && assetBasis !== undefined && assetVol > 0) {
                        totalWeightedBasis += assetBasis * assetVol;
                        totalVolumeForBasis += assetVol;
                    }
                }
                gwaBasis = totalVolumeForBasis > 0 ? Math.round(totalWeightedBasis / totalVolumeForBasis * 100) / 100 : null;
            } else {
                // Single asset selected
                const assetData = pnlData.assets?.[currentAssetFilter];
                if (assetData) {
                    if (currentPeriod === 'daily') {
                        if (isDateRange) {
                            // Aggregate across date range for this asset
                            const rangeData = aggregateDateRange(assetData.daily_pnl, startDate, endDate);
                            pnl = rangeData.pnl;
                            volume = rangeData.volume;
                            gwaBasis = rangeData.gwa_basis;
                            // Calculate realized price based on asset type
                            realizedPrice = calcRealizedPrice(currentAssetFilter, pnl, volume, gwaBasis,
                                rangeData.ppa_revenue, rangeData.merchant_revenue, rangeData.merchant_volume);
                        } else {
                            // Single day
                            const dayData = assetData.daily_pnl?.[startDate];
                            pnl = dayData?.pnl || 0;
                            volume = dayData?.volume || 0;
                            gwaBasis = dayData?.gwa_basis;
                            realizedPpaPrice = dayData?.realized_ppa_price;
                            realizedMerchantPrice = dayData?.realized_merchant_price;

                            // For NWOH today: always use nwohStatus (more current than hourly_revenue_estimate)
                            if (currentAssetFilter === 'NWOH' && userSelectedDate === today && nwohStatus?.today) {
                                const t = nwohStatus.today;
                                // Use total_pnl (PJM + PPA), with proper null checks (0 is valid)
                                if (t.total_pnl !== undefined && t.total_pnl !== null) {
                                    pnl = t.total_pnl;
                                } else if (t.net_revenue !== undefined && t.net_revenue !== null) {
                                    pnl = t.net_revenue;
                                }
                                if (t.total_gen_mwh) volume = t.total_gen_mwh;
                                if (t.gwa_basis !== undefined && t.gwa_basis !== null) {
                                    gwaBasis = t.gwa_basis;
                                }
                            }

                            // Calculate realized price (uses updated pnl/volume from nwohStatus)
                            realizedPrice = calcRealizedPrice(currentAssetFilter, pnl, volume, gwaBasis,
                                dayData?.ppa_revenue, dayData?.merchant_revenue, dayData?.merchant_volume);
                        }
                    } else if (currentPeriod === 'mtd') {
                        const monthData = assetData.monthly_pnl?.[currentMonth];
                        pnl = monthData?.pnl || 0;
                        volume = monthData?.volume || 0;
                        gwaBasis = monthData?.gwa_basis;
                        realizedPpaPrice = monthData?.realized_ppa_price;
                        realizedMerchantPrice = monthData?.realized_merchant_price;
                        realizedPrice = calcRealizedPrice(currentAssetFilter, pnl, volume, gwaBasis,
                            monthData?.ppa_revenue, monthData?.merchant_revenue, monthData?.merchant_volume);
                    } else {
                        // YTD - use current year's annual data
                        const yearData = assetData.annual_pnl?.[currentYear];
                        pnl = yearData?.pnl || 0;
                        volume = yearData?.volume || 0;
                        gwaBasis = yearData?.gwa_basis;
                        realizedPpaPrice = yearData?.realized_ppa_price;
                        realizedMerchantPrice = yearData?.realized_merchant_price;
                        // Calculate realized price using SAME pnl/volume that are displayed
                        realizedPrice = calcRealizedPrice(currentAssetFilter, pnl, volume, gwaBasis,
                            yearData?.ppa_revenue, yearData?.merchant_revenue, yearData?.merchant_volume);
                    }
                }

                // Update single asset detail view
                updateSingleAssetDetail(currentAssetFilter, pnl, volume, realizedPrice, gwaBasis, realizedPpaPrice, realizedMerchantPrice);
            }

            // Update summary cards
            document.getElementById('filtered-pnl').textContent = formatCurrency(pnl);
            document.getElementById('filtered-pnl').style.color = pnl >= 0 ? '#4ade80' : '#ef4444';
            document.getElementById('filtered-volume').textContent = formatNumber(volume) + ' MWh';

            // For Holstein, calculate blended realized price if we have both PPA and merchant
            if (currentAssetFilter === 'HOLSTEIN') {
                if (realizedPpaPrice !== null && realizedMerchantPrice !== null) {
                    // 87.5% PPA + 12.5% Merchant
                    realizedPrice = Math.round((0.875 * realizedPpaPrice + 0.125 * realizedMerchantPrice) * 100) / 100;
                } else if (realizedPpaPrice !== null) {
                    realizedPrice = realizedPpaPrice;
                }
            }

            // Update summary cards - show realized price for all views
            document.getElementById('filtered-realized').textContent = realizedPrice !== null && realizedPrice !== undefined ? formatCurrency(realizedPrice) + '/MWh' : '--';
            document.getElementById('filtered-basis').textContent = gwaBasis !== null && gwaBasis !== undefined ? formatCurrency(gwaBasis) : '--';
            if (gwaBasis !== null && gwaBasis !== undefined) {
                document.getElementById('filtered-basis').style.color = gwaBasis < 0 ? '#ef4444' : '#22c55e';
            }

            // Update label to show actual date when displaying daily data from a different day
            if (currentPeriod === 'daily') {
                const actualNow = new Date();
                const actualToday = actualNow.getFullYear() + '-' + String(actualNow.getMonth() + 1).padStart(2, '0') + '-' + String(actualNow.getDate()).padStart(2, '0');
                if (today !== actualToday) {
                    // Format the date nicely (e.g., "Jan 28")
                    const [year, month, day] = today.split('-');
                    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                    const formattedDate = monthNames[parseInt(month) - 1] + ' ' + parseInt(day);
                    document.getElementById('pnl-label').textContent = formattedDate;
                    document.getElementById('volume-label').textContent = formattedDate;
                } else {
                    document.getElementById('pnl-label').textContent = 'Daily';
                    document.getElementById('volume-label').textContent = 'Daily';
                }
            }
        }

        function updateSingleAssetDetail(assetKey, pnl, volume, realizedPrice, gwaBasis, realizedPpaPrice, realizedMerchantPrice) {
            const assetNames = {
                'BKI': 'Bearkat I',
                'BKII': 'McCrae (BKII)',
                'HOLSTEIN': 'Holstein',
                'NWOH': 'Northwest Ohio Wind'
            };
            const assetTypes = {
                'BKI': '100% Merchant | 197 MW',
                'BKII': '100% PPA @ $34 (50% Basis) | 162.5 MW',
                'HOLSTEIN': '87.5% PPA @ $35 / 12.5% Merchant (100% Basis) | 200 MW',
                'NWOH': '100% PPA @ $33.31 | 105 MW'
            };

            document.getElementById('single-asset-name').textContent = assetNames[assetKey] || assetKey;
            document.getElementById('single-asset-type').textContent = assetTypes[assetKey] || '';

            document.getElementById('single-asset-pnl').textContent = formatCurrency(pnl);
            document.getElementById('single-asset-pnl').style.color = pnl >= 0 ? 'var(--skyvest-blue)' : '#ef4444';
            document.getElementById('single-asset-volume').textContent = formatNumber(volume) + ' MWh';

            if (assetKey === 'HOLSTEIN') {
                // Calculate blended realized price: 87.5% PPA + 12.5% Merchant
                let blendedPrice = null;
                if (realizedPpaPrice !== null && realizedPpaPrice !== undefined && realizedMerchantPrice !== null && realizedMerchantPrice !== undefined) {
                    blendedPrice = Math.round((0.875 * realizedPpaPrice + 0.125 * realizedMerchantPrice) * 100) / 100;
                } else if (realizedPpaPrice !== null && realizedPpaPrice !== undefined) {
                    blendedPrice = realizedPpaPrice;
                }
                document.getElementById('single-asset-realized').textContent = blendedPrice !== null ? formatCurrency(blendedPrice) + '/MWh' : '--';
                document.getElementById('holstein-extra').style.display = '';
                document.getElementById('single-ppa-realized').textContent = realizedPpaPrice !== null && realizedPpaPrice !== undefined ? formatCurrency(realizedPpaPrice) + '/MWh' : '--';
                document.getElementById('single-merchant-realized').textContent = realizedMerchantPrice !== null && realizedMerchantPrice !== undefined ? formatCurrency(realizedMerchantPrice) + '/MWh' : '--';
            } else {
                document.getElementById('holstein-extra').style.display = 'none';
                document.getElementById('single-asset-realized').textContent = realizedPrice !== null && realizedPrice !== undefined ? formatCurrency(realizedPrice) + '/MWh' : '--';
            }

            document.getElementById('single-asset-basis').textContent = gwaBasis !== null && gwaBasis !== undefined ? formatCurrency(gwaBasis) : '--';
            if (gwaBasis !== null && gwaBasis !== undefined) {
                document.getElementById('single-asset-basis').style.color = gwaBasis < 0 ? '#ef4444' : '#22c55e';
            }
        }

        async function fetchPnlData() {
            try {
                const response = await fetch(PNL_API_URL);
                pnlData = await response.json();
                updatePnlDisplay();
                // Update Tenaska timestamp in footer
                if (typeof updateTenaskaTimestamp === 'function') {
                    updateTenaskaTimestamp(pnlData);
                }
                // Fetch NWOH status (price caps, next-day awards)
                fetchNwohStatus();
            } catch (error) {
                console.error('Error fetching PnL data:', error);
            }
        }

        let nwohStatus = null;

        async function fetchNwohStatus() {
            try {
                const response = await fetch('/api/nwoh/status');
                nwohStatus = await response.json();
                updateNwohPriceCapWarning();
                // Re-run display updates now that nwohStatus is available
                // (first run from fetchPnlData had nwohStatus=null, so NWOH today fallbacks didn't trigger)
                if (pnlData) {
                    updateAssetCards();
                    updateFilteredDisplay();
                    if (currentAssetFilter === 'NWOH') {
                        updateNwohDetailCard();
                        updateNwohDaSection();
                    }
                }
            } catch (error) {
                console.error('Error fetching NWOH status:', error);
            }
        }

        function updateNwohPriceCapWarning() {
            const warningEl = document.getElementById('nwoh-price-cap-warning');

            if (!nwohStatus || !nwohStatus.price_caps) {
                if (warningEl) warningEl.style.display = 'none';
                return;
            }

            const caps = nwohStatus.price_caps;
            if (caps.is_capped) {
                if (warningEl) warningEl.style.display = 'block';
            } else {
                if (warningEl) warningEl.style.display = 'none';
            }
        }

        function updatePnlDisplay() {
            if (!pnlData) return;

            // Store hidden values for reference
            const totalPnl = pnlData.total_pnl || 0;
            const totalVolume = pnlData.total_volume || 0;
            document.getElementById('total-pnl').value = totalPnl;
            document.getElementById('total-volume').value = totalVolume;

            // Calculate today's PnL
            const today = new Date().toISOString().split('T')[0];
            const todayData = pnlData.daily_pnl?.[today];
            const todayPnl = todayData?.pnl || 0;
            document.getElementById('today-pnl').value = todayPnl;

            // Calculate MTD PnL
            const currentMonth = new Date().toISOString().slice(0, 7);
            const mtdData = pnlData.monthly_pnl?.[currentMonth];
            const mtdPnl = mtdData?.pnl || 0;
            document.getElementById('mtd-pnl').value = mtdPnl;

            // Update per-asset PnL cards (always updated for YTD totals)
            updateAssetCards();

            // Update filtered display based on current toggles
            updateFilteredDisplay();

            // Update NWOH settlement flow if visible
            if (currentAssetFilter === 'NWOH') {
                updateNwohDetailCard();
            }

            // Update worst basis intervals
            updateWorstBasisTable();

            // Update table based on current view
            updatePnlTable();
            renderPnlChart();
        }

        function updateAssetCards() {
            const assets = pnlData.assets || {};

            // Get date keys based on current period
            const now = new Date();
            const actualToday = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
            const currentMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
            const currentYear = now.getFullYear().toString();

            // For daily view, use date range
            let startDate = selectedDate || actualToday;
            let endDate = selectedEndDate || actualToday;

            // Check if this is a date range
            const isDateRange = currentPeriod === 'daily' && startDate !== endDate;

            ['BKII', 'BKI', 'HOLSTEIN', 'NWOH'].forEach(assetKey => {
                const assetData = assets[assetKey];
                const pnlEl = document.getElementById('asset-pnl-' + assetKey);
                const volEl = document.getElementById('asset-volume-' + assetKey);
                const basisEl = document.getElementById('asset-basis-' + assetKey);

                if (assetData) {
                    // Get data based on current period toggle
                    let pnl, volume, gwaBasis, realizedPrice, realizedPpaPrice, realizedMerchantPrice;

                    if (currentPeriod === 'daily') {
                        if (isDateRange) {
                            // Aggregate across date range
                            const rangeData = aggregateDateRange(assetData.daily_pnl, startDate, endDate);
                            pnl = rangeData.pnl;
                            volume = rangeData.volume;
                            gwaBasis = rangeData.gwa_basis;
                            // Calculate realized price per asset type
                            realizedPrice = calcRealizedPrice(assetKey, pnl, volume, gwaBasis,
                                rangeData.ppa_revenue, rangeData.merchant_revenue, rangeData.merchant_volume);
                        } else {
                            // Single day
                            const dayData = assetData.daily_pnl?.[startDate];
                            pnl = dayData?.pnl || 0;
                            volume = dayData?.volume || 0;
                            gwaBasis = dayData?.gwa_basis;
                            realizedPpaPrice = dayData?.realized_ppa_price;
                            realizedMerchantPrice = dayData?.realized_merchant_price;

                            // For NWOH today: always use nwohStatus (more current than hourly_revenue_estimate)
                            if (assetKey === 'NWOH' && startDate === actualToday && nwohStatus?.today) {
                                const t = nwohStatus.today;
                                if (t.total_pnl !== undefined && t.total_pnl !== null) {
                                    pnl = t.total_pnl;
                                } else if (t.net_revenue !== undefined && t.net_revenue !== null) {
                                    pnl = t.net_revenue;
                                }
                                if (t.total_gen_mwh) volume = t.total_gen_mwh;
                                if (t.gwa_basis !== undefined && t.gwa_basis !== null) {
                                    gwaBasis = t.gwa_basis;
                                }
                            }

                            // Calculate realized price per asset type
                            realizedPrice = calcRealizedPrice(assetKey, pnl, volume, gwaBasis,
                                dayData?.ppa_revenue, dayData?.merchant_revenue, dayData?.merchant_volume);
                        }
                    } else if (currentPeriod === 'mtd') {
                        const monthData = assetData.monthly_pnl?.[currentMonth];
                        pnl = monthData?.pnl || 0;
                        volume = monthData?.volume || 0;
                        gwaBasis = monthData?.gwa_basis;
                        realizedPpaPrice = monthData?.realized_ppa_price;
                        realizedMerchantPrice = monthData?.realized_merchant_price;
                        // Calculate realized price per asset type
                        realizedPrice = calcRealizedPrice(assetKey, pnl, volume, gwaBasis,
                            monthData?.ppa_revenue, monthData?.merchant_revenue, monthData?.merchant_volume);
                    } else {
                        // YTD - use current year's annual data
                        const yearData = assetData.annual_pnl?.[currentYear];
                        pnl = yearData?.pnl || 0;
                        volume = yearData?.volume || 0;
                        gwaBasis = yearData?.gwa_basis;
                        realizedPpaPrice = yearData?.realized_ppa_price;
                        realizedMerchantPrice = yearData?.realized_merchant_price;
                        // Calculate realized price using SAME pnl/volume that are displayed
                        realizedPrice = calcRealizedPrice(assetKey, pnl, volume, gwaBasis,
                            yearData?.ppa_revenue, yearData?.merchant_revenue, yearData?.merchant_volume);
                    }

                    if (pnlEl) {
                        pnlEl.textContent = formatCurrency(pnl);
                        pnlEl.style.color = pnl >= 0 ? 'var(--skyvest-blue)' : '#ef4444';
                    }
                    if (volEl) {
                        volEl.textContent = formatNumber(volume);
                    }
                    if (basisEl && gwaBasis !== null && gwaBasis !== undefined) {
                        basisEl.textContent = formatCurrency(gwaBasis);
                        basisEl.style.color = gwaBasis < 0 ? '#ef4444' : '#22c55e';
                    }

                    // Update realized prices based on asset type (using period-specific data)
                    if (assetKey === 'BKI') {
                        const realizedEl = document.getElementById('asset-realized-BKI');
                        if (realizedEl) {
                            realizedEl.textContent = realizedPrice !== null && realizedPrice !== undefined ? formatCurrency(realizedPrice) + '/MWh' : '--';
                        }
                    } else if (assetKey === 'BKII') {
                        const realizedEl = document.getElementById('asset-realized-BKII');
                        if (realizedEl) {
                            realizedEl.textContent = realizedPrice !== null && realizedPrice !== undefined ? formatCurrency(realizedPrice) + '/MWh' : '--';
                        }
                    } else if (assetKey === 'HOLSTEIN') {
                        const ppaEl = document.getElementById('asset-realized-ppa-HOLSTEIN');
                        const merchantEl = document.getElementById('asset-realized-merchant-HOLSTEIN');
                        if (ppaEl) {
                            ppaEl.textContent = realizedPpaPrice !== null && realizedPpaPrice !== undefined ? formatCurrency(realizedPpaPrice) + '/MWh' : '--';
                        }
                        if (merchantEl) {
                            merchantEl.textContent = realizedMerchantPrice !== null && realizedMerchantPrice !== undefined ? formatCurrency(realizedMerchantPrice) + '/MWh' : '--';
                        }
                    } else if (assetKey === 'NWOH') {
                        // NWOH realized price = PnL / Volume (same as calcRealizedPrice)
                        const realizedEl = document.getElementById('asset-realized-NWOH');
                        if (realizedEl) {
                            realizedEl.textContent = realizedPrice !== null && realizedPrice !== undefined ? formatCurrency(realizedPrice) + '/MWh' : '--';
                        }
                    }
                } else {
                    if (pnlEl) pnlEl.textContent = 'N/A';
                    if (volEl) volEl.textContent = '0';
                }
            });
        }

        function updateWorstBasisTable() {
            const intervals = pnlData.worst_basis_intervals || [];
            const tbody = document.getElementById('worst-basis-body');

            if (intervals.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="text-center py-2" style="color: #999;">No basis data available</td></tr>';
                document.getElementById('excludable-savings').textContent = '$0.00';
                return;
            }

            tbody.innerHTML = intervals.map(interval => {
                const datetime = interval.datetime || interval.interval;
                const dt = new Date(datetime);
                const dateStr = dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                const timeStr = dt.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
                const basis = interval.basis || 0;
                const volume = interval.volume || 0;
                const impact = interval.basis_pnl_impact || 0;
                const asset = interval.asset || 'UNKNOWN';

                return `
                    <tr style="border-bottom: 1px solid #f5f5f5;">
                        <td class="py-1 px-2">${dateStr} ${timeStr}</td>
                        <td class="py-1 px-2">${asset}</td>
                        <td class="py-1 px-2 text-right" style="color: ${basis < 0 ? '#ef4444' : '#666'};">${formatCurrency(basis)}</td>
                        <td class="py-1 px-2 text-right">${volume.toFixed(2)}</td>
                        <td class="py-1 px-2 text-right font-semibold" style="color: ${impact < 0 ? '#ef4444' : 'var(--skyvest-blue)'};">${formatCurrency(impact)}</td>
                    </tr>
                `;
            }).join('');

            // Calculate total excludable savings (sum of absolute negative impacts)
            const totalSavings = intervals
                .filter(i => i.basis_pnl_impact < 0)
                .reduce((sum, i) => sum + Math.abs(i.basis_pnl_impact), 0);
            document.getElementById('excludable-savings').textContent = formatCurrency(totalSavings);
        }

        function setPnlView(view) {
            currentPnlView = view;

            // Update button styles
            ['daily', 'monthly', 'annual'].forEach(v => {
                const btn = document.getElementById('btn-' + v);
                if (v === view) {
                    btn.style.backgroundColor = 'var(--skyvest-blue)';
                    btn.style.color = 'white';
                } else {
                    btn.style.backgroundColor = '#e5e5e5';
                    btn.style.color = 'var(--skyvest-navy)';
                }
            });

            updatePnlTable();
            renderPnlChart();
        }

        function updatePnlTable() {
            if (!pnlData) return;

            const tbody = document.getElementById('pnl-table-body');
            const assetFilter = currentAssetFilter;  // Sync with main asset filter buttons

            let data = {};

            // Get data based on view and asset filter
            if (assetFilter === 'all') {
                // Use total aggregations
                if (currentPnlView === 'daily') {
                    data = pnlData.daily_pnl || {};
                } else if (currentPnlView === 'monthly') {
                    data = pnlData.monthly_pnl || {};
                } else {
                    data = pnlData.annual_pnl || {};
                }
            } else {
                // Use per-asset data
                const assetData = pnlData.assets?.[assetFilter];
                if (assetData) {
                    if (currentPnlView === 'daily') {
                        data = assetData.daily_pnl || {};
                    } else if (currentPnlView === 'monthly') {
                        data = assetData.monthly_pnl || {};
                    } else {
                        data = assetData.annual_pnl || {};
                    }
                }
            }

            // Sort by period descending
            const sortedEntries = Object.entries(data).sort((a, b) => b[0].localeCompare(a[0]));

            if (sortedEntries.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="text-center py-4" style="color: #999;">No PnL data available for ' + (assetFilter === 'all' ? 'all assets' : assetFilter) + '</td></tr>';
                return;
            }

            tbody.innerHTML = sortedEntries.map(([period, values]) => {
                const pnl = values.pnl || 0;
                const volume = values.volume || 0;
                const count = values.count || 0;
                const gwaBasis = values.gwa_basis;
                const pnlColor = pnl >= 0 ? 'var(--skyvest-blue)' : '#ef4444';
                const basisColor = gwaBasis !== null && gwaBasis !== undefined ? (gwaBasis < 0 ? '#ef4444' : '#22c55e') : '#999';
                const basisDisplay = gwaBasis !== null && gwaBasis !== undefined ? formatCurrency(gwaBasis) : '--';

                return `
                    <tr style="border-bottom: 1px solid #f0f0f0;">
                        <td class="py-2 px-2 font-medium" style="color: var(--skyvest-navy);">${period}</td>
                        <td class="py-2 px-2 text-right font-bold" style="color: ${pnlColor};">${formatCurrency(pnl)}</td>
                        <td class="py-2 px-2 text-right" style="color: ${basisColor};">${basisDisplay}</td>
                        <td class="py-2 px-2 text-right" style="color: #666;">${formatNumber(volume)}</td>
                        <td class="py-2 px-2 text-right" style="color: #999;">${count}</td>
                    </tr>
                `;
            }).join('');
        }

        function renderPnlChart() {
            const container = document.getElementById('pnl-chart-container');
            if (!container || !pnlData) return;

            container.innerHTML = '';

            const assetFilter = document.getElementById('asset-filter')?.value || 'all';
            let data = {};

            // Get data based on view and asset filter
            if (assetFilter === 'all') {
                if (currentPnlView === 'daily') {
                    data = pnlData.daily_pnl || {};
                } else if (currentPnlView === 'monthly') {
                    data = pnlData.monthly_pnl || {};
                } else {
                    data = pnlData.annual_pnl || {};
                }
            } else {
                const assetData = pnlData.assets?.[assetFilter];
                if (assetData) {
                    if (currentPnlView === 'daily') {
                        data = assetData.daily_pnl || {};
                    } else if (currentPnlView === 'monthly') {
                        data = assetData.monthly_pnl || {};
                    } else {
                        data = assetData.annual_pnl || {};
                    }
                }
            }

            // Sort by period ascending for chart
            const sortedEntries = Object.entries(data).sort((a, b) => a[0].localeCompare(b[0]));

            // Take last 30 entries for daily, all for others
            const chartData = currentPnlView === 'daily' ? sortedEntries.slice(-30) : sortedEntries;

            if (chartData.length === 0) {
                container.innerHTML = '<p style="color: #999; text-align: center; padding: 40px;">No data available</p>';
                return;
            }

            const values = chartData.map(([_, v]) => v.pnl);
            const minVal = Math.min(...values, 0);
            const maxVal = Math.max(...values, 0);
            const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal));
            const yRange = absMax * 2 || 100;
            const yMin = -absMax || -50;
            const yMax = absMax || 50;

            const chartWrapper = document.createElement('div');
            chartWrapper.style.display = 'flex';
            chartWrapper.style.flexDirection = 'column';
            chartWrapper.style.gap = '4px';

            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '80px 1fr';
            chart.style.gap = '8px';

            // Y-axis
            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '12px';
            yAxis.style.fontSize = '11px';
            yAxis.style.color = '#999';
            yAxis.style.borderRight = '1px solid #e5e5e5';

            const step = Math.ceil(absMax / 3 / 100) * 100 || 100;
            for (let i = yMax; i >= yMin; i -= step) {
                const label = document.createElement('div');
                label.textContent = formatCurrency(i);
                label.style.padding = '4px 0';
                yAxis.appendChild(label);
            }

            // Bars container
            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'center';
            bars.style.gap = '2px';
            bars.style.minHeight = '120px';
            bars.style.position = 'relative';

            // Zero line
            const zeroLine = document.createElement('div');
            zeroLine.style.position = 'absolute';
            zeroLine.style.left = '0';
            zeroLine.style.right = '0';
            zeroLine.style.top = '50%';
            zeroLine.style.height = '1px';
            zeroLine.style.backgroundColor = '#999';
            bars.appendChild(zeroLine);

            // Draw bars
            chartData.forEach(([period, values]) => {
                const pnl = values.pnl;
                const heightPercent = Math.abs(pnl) / yRange * 100;
                const isPositive = pnl >= 0;
                const color = isPositive ? 'var(--skyvest-blue)' : '#ef4444';

                const barContainer = document.createElement('div');
                barContainer.style.flex = '1';
                barContainer.style.height = '100%';
                barContainer.style.display = 'flex';
                barContainer.style.flexDirection = 'column';
                barContainer.style.justifyContent = 'center';
                barContainer.style.position = 'relative';

                const bar = document.createElement('div');
                bar.style.width = '100%';
                bar.style.height = Math.max(heightPercent, 2) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.85';
                bar.style.position = 'absolute';
                bar.style.left = '0';
                bar.style.right = '0';
                bar.style.transition = 'opacity 0.2s';
                bar.title = period + ': ' + formatCurrency(pnl);

                if (isPositive) {
                    bar.style.bottom = '50%';
                    bar.style.borderRadius = '2px 2px 0 0';
                } else {
                    bar.style.top = '50%';
                    bar.style.borderRadius = '0 0 2px 2px';
                }

                bar.addEventListener('mouseenter', () => bar.style.opacity = '1');
                bar.addEventListener('mouseleave', () => bar.style.opacity = '0.85');

                barContainer.appendChild(bar);
                bars.appendChild(barContainer);
            });

            chart.appendChild(yAxis);
            chart.appendChild(bars);
            chartWrapper.appendChild(chart);
            container.appendChild(chartWrapper);
        }

        async function reloadPnlData() {
            try {
                // Helper to safely fetch and parse JSON
                async function safeFetch(url) {
                    const response = await fetch(url, { method: 'POST' });
                    if (!response.ok) {
                        return { success: false, message: 'HTTP ' + response.status };
                    }
                    const text = await response.text();
                    try {
                        return JSON.parse(text);
                    } catch (e) {
                        return { success: false, message: 'Invalid response (may need to refresh page)' };
                    }
                }

                // Reload both Tenaska and Pharos data
                const [tenaskaResult, pharosResult] = await Promise.all([
                    safeFetch(PNL_API_URL + '/reload'),
                    safeFetch('/api/pharos/reload')
                ]);

                await fetchPnlData();
                fetchNwohStatus();

                const messages = [];
                if (tenaskaResult.success) messages.push('Tenaska: ' + tenaskaResult.message);
                else messages.push('Tenaska: ' + (tenaskaResult.message || 'Failed'));
                if (pharosResult.success) messages.push('Pharos: ' + pharosResult.message);
                else messages.push('Pharos: ' + (pharosResult.message || 'Failed'));

                alert('Data reload results:\\n' + messages.join('\\n'));
            } catch (error) {
                console.error('Error reloading data:', error);
                alert('Error reloading data: ' + error.message);
            }
        }

        function formatCurrency(value) {
            if (value === null || value === undefined) return '$0.00';
            const formatted = Math.abs(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            return (value < 0 ? '-$' : '$') + formatted;
        }

        function formatNumber(value) {
            if (value === null || value === undefined) return '0';
            return value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }

        // Status footer update functions
        let lmpLastUpdate = null;
        let pnlLastUpdate = null;

        // Format time in Eastern Time
        const estTimeOptions = { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true };
        const estDateTimeOptions = { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true };

        function formatTimeEST(date) {
            return date.toLocaleTimeString('en-US', estTimeOptions);
        }

        function formatDateTimeEST(date) {
            return date.toLocaleString('en-US', estDateTimeOptions);
        }

        function updateLmpTimestamp() {
            lmpLastUpdate = new Date();
            const el = document.getElementById('lmp-last-update');
            if (el) el.textContent = '(updated ' + formatTimeEST(lmpLastUpdate) + ' ET)';
        }

        function updatePnlTimestamp() {
            pnlLastUpdate = new Date();
            const el = document.getElementById('pnl-last-update');
            if (el) el.textContent = '(updated ' + formatTimeEST(pnlLastUpdate) + ' ET)';
        }

        function updateCurrentTime() {
            const el = document.getElementById('current-time');
            if (el) el.textContent = formatDateTimeEST(new Date()) + ' ET';
        }

        // Update Tenaska timestamp from PnL data if available
        function updateTenaskaTimestamp(pnlData) {
            const el = document.getElementById('tenaska-last-update');
            if (el && pnlData && pnlData.last_update) {
                const lastUpdated = new Date(pnlData.last_update);
                el.textContent = '(last fetch ' + formatTimeEST(lastUpdated) + ' ET)';
            }
        }

        // Update Pharos timestamp and status
        function updatePharosTimestamp() {
            fetch('/api/pharos/status')
                .then(r => r.json())
                .then(data => {
                    const el = document.getElementById('pharos-last-update');
                    const dot = el?.previousElementSibling?.previousElementSibling;

                    if (el) {
                        if (data.data_loaded) {
                            const lastUpdated = data.last_update ? new Date(data.last_update) : null;
                            const timeStr = lastUpdated ? formatTimeEST(lastUpdated) + ' ET' : '';
                            const vol = (data.total_volume / 1000).toFixed(1);
                            el.textContent = `(${vol}k MWh, ${data.daily_records} days${timeStr ? ', ' + timeStr : ''})`;
                            if (dot) dot.className = 'w-2 h-2 rounded-full bg-purple-500';
                        } else {
                            el.textContent = '(No data - check logs)';
                            if (dot) dot.className = 'w-2 h-2 rounded-full bg-red-500';
                        }
                    }
                })
                .catch(err => {
                    console.log('Pharos status fetch error:', err);
                    const el = document.getElementById('pharos-last-update');
                    if (el) el.textContent = '(Error fetching status)';
                });
        }

        // Wrap original fetch functions to update timestamps
        const originalFetchData = fetchData;
        fetchData = async function() {
            await originalFetchData();
            updateLmpTimestamp();
        };

        const originalFetchPnlData = fetchPnlData;
        fetchPnlData = async function() {
            await originalFetchPnlData();
            updatePnlTimestamp();
        };

        // Initialize both basis and PnL data
        initDatePicker();
        fetchData();
        fetchPnlData();
        updatePharosTimestamp();
        setInterval(fetchData, 30000);
        setInterval(fetchPnlData, 2100000);  // Refresh PnL every 35 minutes (Tenaska updates every 15 mins)
        setInterval(updatePharosTimestamp, 1800000);  // Refresh Pharos timestamp every 30 minutes
        setInterval(updateCurrentTime, 1000);  // Update clock every second
        updateCurrentTime();
    </script>
</body>
</html>'''

if __name__ == '__main__':
    # Only start background thread here for local development
    start_background_thread_if_needed()
    app.run(debug=False, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))

