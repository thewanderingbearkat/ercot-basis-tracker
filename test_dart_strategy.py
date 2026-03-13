"""
Shadow DART Strategy Analysis - Test File
Tests all data fetching, calculations, and aggregation before integrating into app.py.
"""
import requests
import json
import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
TENASKA_API_AUTH = (
    os.getenv("TENASKA_API_USER", "tmartin@skyvest.com"),
    os.getenv("TENASKA_API_PASSWORD", "Rowhard2024!!!")
)
TENASKA_TOKEN_URL = "https://api.ptp.energy/v1/token"
TENASKA_MARKET_PRICES_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Market-Prices/data"
TENASKA_DART_DETAILS_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/DART-Energy-Details/data"
TENASKA_DART_FORECAST_URL = "https://api.ptp.energy/v1/markets/ERCOTNodal/endpoints/Optimization-Renewable-Forecast/data"

# Settlement points
NODES = ["NBOHR_RN", "HOLSTEIN_ALL", "HB_WEST"]
PRICE_KEYS = ["DASPP", "RTSPP"]

# Asset config (subset from app.py)
ASSET_CONFIG = {
    "BKI": {
        "display_name": "Bearkat I",
        "settlement_point": "NBOHR_RN",
        "element_patterns": ["Bearkat Wind Energy I, LLC - Gen"],
    },
    "BKII": {
        "display_name": "McCrae (BKII)",
        "settlement_point": "NBOHR_RN",
        "element_patterns": ["Bearkat Wind Energy II, LLC - Gen"],
    },
    "HOLSTEIN": {
        "display_name": "Holstein",
        "settlement_point": "HOLSTEIN_ALL",
        "element_patterns": ["Holstein Solar - Generation"],
    },
}

# Forecast element mapping (from Optimization-Renewable-Forecast)
FORECAST_ELEMENT_MAP = {
    "Holstein Solar - DART Optimization": "HOLSTEIN",
    "226HC 8me LLC (Holstein Solar)": "HOLSTEIN",
    "Holstein Solar - PTP Optimization": "HOLSTEIN",
    # BKI/BKII elements will be discovered and logged
}

DART_TRAILING_AVG_DAYS = 7
CST_TZ = ZoneInfo("America/Chicago")
UTC_TZ = ZoneInfo("UTC")


# ============================================================================
# AUTH
# ============================================================================
def get_tenaska_token():
    try:
        resp = requests.get(TENASKA_TOKEN_URL, auth=TENASKA_API_AUTH, timeout=10)
        if resp.status_code == 200:
            token = resp.json().get("data")
            logger.info("Got Tenaska API token")
            return token
        else:
            logger.error(f"Token request failed: {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Token error: {e}")
        return None


def identify_asset(element_name):
    """Identify asset from element name, only accepting '- Gen' or '- Generation' elements."""
    element_lower = (element_name or "").lower()
    is_generation = "- generation" in element_lower
    is_gen = element_lower.endswith("- gen")
    if not is_generation and not is_gen:
        return "UNKNOWN"
    for asset_key, config in ASSET_CONFIG.items():
        for pattern in config.get("element_patterns", []):
            if pattern.lower() in element_lower:
                return asset_key
    return "UNKNOWN"


# ============================================================================
# DATA FETCHING
# ============================================================================
def fetch_market_prices_da_rt(start_date, end_date, token):
    """
    Fetch DASPP (hourly) and RTSPP (15-min) for NBOHR_RN, HOLSTEIN_ALL, HB_WEST.
    Returns: { settlement_point: { "DASPP": {iso_str: price}, "RTSPP": {iso_str: price} } }
    """
    headers = {"Authorization": f"Bearer {token}"}
    result = {node: {"DASPP": {}, "RTSPP": {}} for node in NODES}

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current_dt = start_dt

    logger.info(f"Fetching market prices (DASPP+RTSPP) for {NODES} from {start_date} to {end_date}")

    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}

        try:
            resp = requests.get(TENASKA_MARKET_PRICES_URL, headers=headers, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("data", []):
                    elem = item.get("element", "")
                    if elem not in NODES:
                        continue
                    for dp in item.get("dataPoints", []):
                        key_name = dp.get("keyName")
                        if key_name not in PRICE_KEYS:
                            continue
                        for val_entry in dp.get("values", []):
                            interval_utc = val_entry.get("intervalStartUtc", "")
                            for nested in val_entry.get("data", []):
                                price = nested.get("value", 0)
                                try:
                                    dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                                    dt = dt.replace(tzinfo=UTC_TZ).astimezone(CST_TZ)
                                    result[elem][key_name][dt.isoformat()] = float(price) if price else 0
                                except:
                                    continue
            else:
                logger.warning(f"Market-Prices returned {resp.status_code} for {day_str}")
        except Exception as e:
            logger.warning(f"Error fetching market prices for {day_str}: {e}")

        current_dt += timedelta(days=1)

    for node in NODES:
        logger.info(f"  {node}: DASPP={len(result[node]['DASPP'])} intervals, RTSPP={len(result[node]['RTSPP'])} intervals")

    return result


def fetch_dart_generation(start_date, end_date, token):
    """
    Fetch hourly generation from DART-Energy-Details.
    Returns: { asset_key: { "YYYY-MM-DD HE##": { "gen_mwh": float, "rt_settlement": float, "rt_qty": float } } }
    """
    headers = {"Authorization": f"Bearer {token}"}
    result = {k: {} for k in ASSET_CONFIG}

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current_dt = start_dt

    logger.info(f"Fetching DART generation from {start_date} to {end_date}")
    all_elements_seen = set()

    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}

        try:
            resp = requests.get(TENASKA_DART_DETAILS_URL, headers=headers, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("data", []):
                    elem = item.get("element", "")
                    all_elements_seen.add(elem)
                    asset = identify_asset(elem)
                    if asset == "UNKNOWN":
                        continue

                    # Extract hourly gen and RT settlement
                    hourly_gen = {}
                    hourly_rt_amt = {}
                    hourly_rt_qty = {}

                    for dp in item.get("dataPoints", []):
                        key_name = dp.get("keyName")
                        for val_entry in dp.get("values", []):
                            interval_utc = val_entry.get("intervalStartUtc", "")
                            for nested in val_entry.get("data", []):
                                val = float(nested.get("value", 0) or 0)
                                try:
                                    dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                                    dt_cst = dt.replace(tzinfo=UTC_TZ).astimezone(CST_TZ)
                                    # Determine hour ending (HE1 = 00:00-01:00 CST)
                                    date_str = dt_cst.strftime("%Y-%m-%d")
                                    he = dt_cst.hour + 1  # hour ending
                                    if he == 0:
                                        he = 24
                                    hour_key = f"{date_str} HE{he:02d}"

                                    if key_name == "GEN_MWH_HRLY":
                                        hourly_gen[hour_key] = val
                                    elif key_name == "RTEIAMT":
                                        # 15-min data, sum to hourly
                                        hourly_rt_amt[hour_key] = hourly_rt_amt.get(hour_key, 0) + val
                                    elif key_name == "RTEI_QTY":
                                        hourly_rt_qty[hour_key] = hourly_rt_qty.get(hour_key, 0) + val
                                except:
                                    continue

                    # Merge into result
                    for hour_key, gen in hourly_gen.items():
                        result[asset][hour_key] = {
                            "gen_mwh": gen,
                            "rt_settlement": hourly_rt_amt.get(hour_key, 0),
                            "rt_qty": hourly_rt_qty.get(hour_key, 0),
                        }
            else:
                logger.warning(f"DART-Energy-Details returned {resp.status_code} for {day_str}")
        except Exception as e:
            logger.warning(f"Error fetching DART generation for {day_str}: {e}")

        current_dt += timedelta(days=1)

    logger.info(f"  All elements seen: {sorted(all_elements_seen)}")
    for asset in ASSET_CONFIG:
        logger.info(f"  {asset}: {len(result[asset])} hourly records")

    return result


def fetch_dart_forecasts(start_date, end_date, token):
    """
    Fetch generation forecasts from Optimization-Renewable-Forecast.
    Returns: { asset_key: { "YYYY-MM-DD HE##": forecast_mw } }
    """
    headers = {"Authorization": f"Bearer {token}"}
    result = {k: {} for k in ASSET_CONFIG}

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current_dt = start_dt

    logger.info(f"Fetching renewable forecasts from {start_date} to {end_date}")
    all_elements_seen = set()

    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}

        try:
            resp = requests.get(TENASKA_DART_FORECAST_URL, headers=headers, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("data", []):
                    elem = item.get("element", "")
                    all_elements_seen.add(elem)

                    # Map element to asset
                    asset = FORECAST_ELEMENT_MAP.get(elem)
                    if not asset:
                        # Try identify_asset as fallback
                        asset = identify_asset(elem)
                    if not asset or asset == "UNKNOWN":
                        continue

                    for dp in item.get("dataPoints", []):
                        key_name = dp.get("keyName")
                        # Use STPPF_Forecast (Short-Term Power Production Forecast)
                        # or PVGRPP_Forecast for solar
                        if key_name not in ("STPPF_Forecast", "PVGRPP_Forecast"):
                            continue
                        for val_entry in dp.get("values", []):
                            interval_utc = val_entry.get("intervalStartUtc", "")
                            for nested in val_entry.get("data", []):
                                val = float(nested.get("value", 0) or 0)
                                try:
                                    dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                                    dt_cst = dt.replace(tzinfo=UTC_TZ).astimezone(CST_TZ)
                                    date_str = dt_cst.strftime("%Y-%m-%d")
                                    he = dt_cst.hour + 1
                                    if he == 0:
                                        he = 24
                                    hour_key = f"{date_str} HE{he:02d}"
                                    # Take the max of existing (in case both STPPF and PVGRPP exist)
                                    existing = result[asset].get(hour_key, 0)
                                    result[asset][hour_key] = max(existing, val)
                                except:
                                    continue
            else:
                logger.warning(f"Forecast API returned {resp.status_code} for {day_str}")
        except Exception as e:
            logger.warning(f"Error fetching forecasts for {day_str}: {e}")

        current_dt += timedelta(days=1)

    logger.info(f"  All forecast elements seen: {sorted(all_elements_seen)}")
    for asset in ASSET_CONFIG:
        nonzero = sum(1 for v in result[asset].values() if v > 0)
        logger.info(f"  {asset}: {len(result[asset])} intervals, {nonzero} non-zero")

    return result


def compute_trailing_avg_forecast(generation, target_date, lookback_days=7):
    """
    Compute trailing N-day hourly average of actual generation as a naive forecast.
    Returns: { asset_key: { "YYYY-MM-DD HE##": avg_mw } }
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    result = {k: {} for k in ASSET_CONFIG}

    for asset in ASSET_CONFIG:
        # Collect gen by hour-of-day over lookback window
        hourly_sums = defaultdict(list)  # {HE##: [gen_values]}

        for day_offset in range(1, lookback_days + 1):
            lookback_date = (target_dt - timedelta(days=day_offset)).strftime("%Y-%m-%d")
            for he in range(1, 25):
                hour_key = f"{lookback_date} HE{he:02d}"
                rec = generation[asset].get(hour_key)
                if rec:
                    hourly_sums[he].append(rec["gen_mwh"])

        # Compute average for each hour on the target date
        for he in range(1, 25):
            hour_key = f"{target_date} HE{he:02d}"
            values = hourly_sums.get(he, [])
            if values:
                result[asset][hour_key] = sum(values) / len(values)
            else:
                result[asset][hour_key] = 0

    return result


# ============================================================================
# DART CALCULATION
# ============================================================================
def get_hourly_rt_price(market_prices, node, date_str, he):
    """
    Get hourly average RT price from 15-min RTSPP data.
    RTSPP is 15-min; average the 4 intervals for the hour.
    """
    # Hour ending HE12 = 11:00-12:00 CST, so intervals start at 11:00, 11:15, 11:30, 11:45
    hour_start = he - 1  # 0-indexed hour
    prices = []

    rtspp = market_prices.get(node, {}).get("RTSPP", {})
    # Look for 15-min intervals within this hour
    for iso_str, price in rtspp.items():
        try:
            dt = datetime.fromisoformat(iso_str)
            if dt.strftime("%Y-%m-%d") == date_str and dt.hour == hour_start:
                prices.append(price)
        except:
            continue

    if prices:
        return sum(prices) / len(prices)
    return None


def get_da_price(market_prices, node, date_str, he):
    """Get DA settlement point price for a given hour."""
    hour_start = he - 1
    daspp = market_prices.get(node, {}).get("DASPP", {})
    for iso_str, price in daspp.items():
        try:
            dt = datetime.fromisoformat(iso_str)
            if dt.strftime("%Y-%m-%d") == date_str and dt.hour == hour_start:
                return price
        except:
            continue
    return None


def calculate_shadow_dart(market_prices, generation, forecasts, forecast_mode="tenaska"):
    """
    Core shadow DART calculation.
    Returns list of hourly records across all assets.
    """
    records = []

    for asset, config in ASSET_CONFIG.items():
        node = config["settlement_point"]
        asset_gen = generation.get(asset, {})

        for hour_key, gen_data in sorted(asset_gen.items()):
            # Parse hour key: "YYYY-MM-DD HE##"
            parts = hour_key.split(" ")
            if len(parts) != 2:
                continue
            date_str = parts[0]
            he = int(parts[1].replace("HE", ""))

            actual_gen = gen_data["gen_mwh"]
            da_price = get_da_price(market_prices, node, date_str, he)
            rt_price = get_hourly_rt_price(market_prices, node, date_str, he)
            da_hub = get_da_price(market_prices, "HB_WEST", date_str, he)
            rt_hub = get_hourly_rt_price(market_prices, "HB_WEST", date_str, he)

            if da_price is None or rt_price is None:
                continue

            # Get forecast (DA bid quantity)
            forecast_mw = 0
            if forecast_mode == "tenaska":
                forecast_mw = forecasts.get(asset, {}).get(hour_key, 0)
            elif forecast_mode == "trailing_avg":
                forecast_mw = forecasts.get(asset, {}).get(hour_key, 0)

            # Shadow DART calculation
            da_revenue = forecast_mw * da_price
            rt_deviation = actual_gen - forecast_mw
            rt_deviation_settlement = rt_deviation * rt_price
            shadow_dart_total = da_revenue + rt_deviation_settlement

            # Current RT-only revenue
            rt_only_revenue = actual_gen * rt_price

            # DART uplift
            dart_uplift = shadow_dart_total - rt_only_revenue
            # Should equal: forecast_mw * (da_price - rt_price)

            records.append({
                "asset": asset,
                "date": date_str,
                "hour_ending": he,
                "hour_key": hour_key,
                "forecast_mw": round(forecast_mw, 2),
                "actual_gen_mw": round(actual_gen, 2),
                "rt_deviation_mw": round(rt_deviation, 2),
                "da_node_price": round(da_price, 2),
                "rt_node_price": round(rt_price, 2),
                "da_hub_price": round(da_hub, 2) if da_hub else None,
                "rt_hub_price": round(rt_hub, 2) if rt_hub else None,
                "da_revenue": round(da_revenue, 2),
                "rt_deviation_settlement": round(rt_deviation_settlement, 2),
                "shadow_dart_total": round(shadow_dart_total, 2),
                "rt_only_revenue": round(rt_only_revenue, 2),
                "dart_uplift": round(dart_uplift, 2),
                "forecast_mode": forecast_mode,
            })

    return records


def aggregate_dart_results(hourly_records):
    """
    Aggregate hourly DART records into daily/monthly/annual summaries.
    """
    daily = defaultdict(lambda: {
        "da_revenue": 0, "rt_deviation_settlement": 0, "shadow_dart_total": 0,
        "rt_only_revenue": 0, "dart_uplift": 0, "gen_mwh": 0, "forecast_mwh": 0,
        "hours": 0,
    })
    daily_asset = defaultdict(lambda: defaultdict(lambda: {
        "da_revenue": 0, "rt_deviation_settlement": 0, "shadow_dart_total": 0,
        "rt_only_revenue": 0, "dart_uplift": 0, "gen_mwh": 0, "forecast_mwh": 0,
        "hours": 0,
    }))

    for r in hourly_records:
        date = r["date"]
        asset = r["asset"]

        for target in [daily[date], daily_asset[date][asset]]:
            target["da_revenue"] += r["da_revenue"]
            target["rt_deviation_settlement"] += r["rt_deviation_settlement"]
            target["shadow_dart_total"] += r["shadow_dart_total"]
            target["rt_only_revenue"] += r["rt_only_revenue"]
            target["dart_uplift"] += r["dart_uplift"]
            target["gen_mwh"] += r["actual_gen_mw"]
            target["forecast_mwh"] += r["forecast_mw"]
            target["hours"] += 1

    # Round
    for d in daily.values():
        for k in d:
            if k != "hours":
                d[k] = round(d[k], 2)
    for date_data in daily_asset.values():
        for d in date_data.values():
            for k in d:
                if k != "hours":
                    d[k] = round(d[k], 2)

    # Monthly rollup
    monthly = defaultdict(lambda: {
        "da_revenue": 0, "rt_deviation_settlement": 0, "shadow_dart_total": 0,
        "rt_only_revenue": 0, "dart_uplift": 0, "gen_mwh": 0, "forecast_mwh": 0,
        "days": 0,
    })
    for date_str, d in daily.items():
        month_key = date_str[:7]
        for k in d:
            if k == "hours":
                continue
            monthly[month_key][k] = round(monthly[month_key][k] + d[k], 2)
        monthly[month_key]["days"] += 1

    total_uplift = sum(d["dart_uplift"] for d in daily.values())
    total_da_rev = sum(d["da_revenue"] for d in daily.values())
    total_rt_only = sum(d["rt_only_revenue"] for d in daily.values())

    return {
        "daily": dict(daily),
        "daily_asset": {k: dict(v) for k, v in daily_asset.items()},
        "monthly": dict(monthly),
        "total_dart_uplift": round(total_uplift, 2),
        "total_da_revenue": round(total_da_rev, 2),
        "total_rt_only_revenue": round(total_rt_only, 2),
    }


# ============================================================================
# MAIN TEST
# ============================================================================
def main():
    print("=" * 100)
    print("SHADOW DART STRATEGY ANALYSIS - TEST")
    print("=" * 100)

    # Test with last 3 days of data
    end_date = datetime.now(CST_TZ).strftime("%Y-%m-%d")
    start_date = (datetime.now(CST_TZ) - timedelta(days=3)).strftime("%Y-%m-%d")

    print(f"\nDate range: {start_date} to {end_date}")

    # Step 1: Auth
    token = get_tenaska_token()
    if not token:
        print("FAILED: Could not get API token")
        return

    # Step 2: Fetch market prices
    print("\n--- STEP 2: Fetch Market Prices (DASPP + RTSPP) ---")
    prices = fetch_market_prices_da_rt(start_date, end_date, token)

    # Step 3: Fetch generation
    print("\n--- STEP 3: Fetch DART Generation ---")
    generation = fetch_dart_generation(start_date, end_date, token)

    # Step 4: Fetch forecasts
    print("\n--- STEP 4: Fetch Renewable Forecasts ---")
    forecasts = fetch_dart_forecasts(start_date, end_date, token)

    # Step 5: Compute trailing average forecast
    print("\n--- STEP 5: Trailing Average Forecast ---")
    # For trailing avg, we need more history. Use gen data from the last 3 days
    # to compute avg for the most recent day.
    latest_date = end_date
    trailing_forecast = compute_trailing_avg_forecast(generation, latest_date, lookback_days=3)
    for asset in ASSET_CONFIG:
        nonzero = sum(1 for v in trailing_forecast[asset].values() if v > 0)
        print(f"  {asset}: {nonzero}/24 hours with non-zero trailing avg forecast")

    # Step 6: Calculate Shadow DART (Tenaska forecast mode)
    print("\n--- STEP 6: Shadow DART Calculation (Tenaska Forecast) ---")
    dart_records_tenaska = calculate_shadow_dart(prices, generation, forecasts, "tenaska")
    print(f"  Total hourly records: {len(dart_records_tenaska)}")

    # Step 6b: Calculate Shadow DART (Trailing Avg mode)
    print("\n--- STEP 6b: Shadow DART Calculation (Trailing Avg) ---")
    dart_records_trailing = calculate_shadow_dart(prices, generation, trailing_forecast, "trailing_avg")
    print(f"  Total hourly records: {len(dart_records_trailing)}")

    # Step 7: Aggregate
    print("\n--- STEP 7: Aggregate DART Results ---")
    agg_tenaska = aggregate_dart_results(dart_records_tenaska)
    agg_trailing = aggregate_dart_results(dart_records_trailing)

    # Step 8: Print validation report
    print("\n" + "=" * 120)
    print("VALIDATION REPORT - TENASKA FORECAST MODE")
    print("=" * 120)

    print(f"\n{'Date':<12} | {'DA Revenue':>12} | {'RT Dev Sett':>12} | {'DART Total':>12} | {'RT-Only':>12} | {'Uplift':>12} | {'Gen MWh':>10} | {'Fcst MWh':>10}")
    print("-" * 120)
    for date_str in sorted(agg_tenaska["daily"].keys()):
        d = agg_tenaska["daily"][date_str]
        print(f"{date_str:<12} | {d['da_revenue']:>12,.2f} | {d['rt_deviation_settlement']:>12,.2f} | {d['shadow_dart_total']:>12,.2f} | {d['rt_only_revenue']:>12,.2f} | {d['dart_uplift']:>12,.2f} | {d['gen_mwh']:>10,.1f} | {d['forecast_mwh']:>10,.1f}")

    print(f"\n  Total DART Uplift: ${agg_tenaska['total_dart_uplift']:,.2f}")
    print(f"  Total DA Revenue: ${agg_tenaska['total_da_revenue']:,.2f}")
    print(f"  Total RT-Only Revenue: ${agg_tenaska['total_rt_only_revenue']:,.2f}")

    # Per-asset breakdown for most recent day
    latest = sorted(agg_tenaska["daily_asset"].keys())[-1] if agg_tenaska["daily_asset"] else None
    if latest:
        print(f"\n--- Per-Asset Breakdown for {latest} ---")
        print(f"{'Asset':<12} | {'DA Revenue':>12} | {'RT Dev Sett':>12} | {'DART Total':>12} | {'RT-Only':>12} | {'Uplift':>12} | {'Gen MWh':>10}")
        print("-" * 100)
        for asset in ASSET_CONFIG:
            d = agg_tenaska["daily_asset"][latest].get(asset, {})
            if d:
                print(f"{asset:<12} | {d.get('da_revenue',0):>12,.2f} | {d.get('rt_deviation_settlement',0):>12,.2f} | {d.get('shadow_dart_total',0):>12,.2f} | {d.get('rt_only_revenue',0):>12,.2f} | {d.get('dart_uplift',0):>12,.2f} | {d.get('gen_mwh',0):>10,.1f}")

    # Trailing Avg comparison
    print(f"\n{'='*120}")
    print("VALIDATION REPORT - TRAILING AVERAGE FORECAST MODE")
    print("=" * 120)

    print(f"\n{'Date':<12} | {'DA Revenue':>12} | {'RT Dev Sett':>12} | {'DART Total':>12} | {'RT-Only':>12} | {'Uplift':>12} | {'Gen MWh':>10} | {'Fcst MWh':>10}")
    print("-" * 120)
    for date_str in sorted(agg_trailing["daily"].keys()):
        d = agg_trailing["daily"][date_str]
        print(f"{date_str:<12} | {d['da_revenue']:>12,.2f} | {d['rt_deviation_settlement']:>12,.2f} | {d['shadow_dart_total']:>12,.2f} | {d['rt_only_revenue']:>12,.2f} | {d['dart_uplift']:>12,.2f} | {d['gen_mwh']:>10,.1f} | {d['forecast_mwh']:>10,.1f}")

    print(f"\n  Total DART Uplift: ${agg_trailing['total_dart_uplift']:,.2f}")
    print(f"  Total DA Revenue: ${agg_trailing['total_da_revenue']:,.2f}")
    print(f"  Total RT-Only Revenue: ${agg_trailing['total_rt_only_revenue']:,.2f}")

    # Step 9: Sanity check - DART uplift should = forecast * (DA price - RT price)
    print(f"\n{'='*100}")
    print("SANITY CHECK: DART Uplift = Forecast × (DA Price - RT Price)")
    print("=" * 100)
    mismatches = 0
    for r in dart_records_tenaska[:20]:  # Check first 20 records
        expected_uplift = r["forecast_mw"] * (r["da_node_price"] - r["rt_node_price"])
        actual_uplift = r["dart_uplift"]
        diff = abs(expected_uplift - actual_uplift)
        status = "OK" if diff < 0.02 else f"MISMATCH (diff={diff:.4f})"
        if diff >= 0.02:
            mismatches += 1
        print(f"  {r['asset']:<10} {r['hour_key']:<20} Fcst={r['forecast_mw']:>8.2f} DA={r['da_node_price']:>7.2f} RT={r['rt_node_price']:>7.2f} | Expected={expected_uplift:>10.2f} Actual={actual_uplift:>10.2f} {status}")

    if mismatches == 0:
        print(f"\n  ALL SANITY CHECKS PASSED")
    else:
        print(f"\n  WARNING: {mismatches} mismatches found")

    print("\n" + "=" * 100)
    print("TEST COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()
