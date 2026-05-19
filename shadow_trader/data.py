"""Tenaska API data fetchers: DA/RT market prices, hourly generation, generation forecasts."""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from shadow_trader.config import (
    DART_ASSETS,
    DART_FORECAST_ELEMENT_MAP,
    DART_NODES,
    DART_PRICE_KEYS,
    TENASKA_DART_DETAILS_URL,
    TENASKA_DART_FORECAST_URL,
    TENASKA_MARKET_PRICES_URL,
)
from shadow_trader.strategy import identify_asset

logger = logging.getLogger(__name__)

_CST = ZoneInfo("America/Chicago")
_UTC = ZoneInfo("UTC")


def _daterange(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current = start_dt
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def fetch_market_prices(start_date: str, end_date: str, token: str) -> dict:
    """Return {node: {DASPP: {iso_ts: price}, RTSPP: {iso_ts: price}}} for DART_NODES."""
    headers = {"Authorization": f"Bearer {token}"}
    result = {node: {"DASPP": {}, "RTSPP": {}} for node in DART_NODES}
    logger.info("Fetching DA/RT market prices from %s to %s", start_date, end_date)

    for day_str in _daterange(start_date, end_date):
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}
        try:
            resp = requests.get(TENASKA_MARKET_PRICES_URL, headers=headers, params=params, timeout=60)
        except Exception as e:
            logger.warning("Market prices fetch error on %s: %s", day_str, e)
            continue
        if resp.status_code != 200:
            logger.warning("Market prices HTTP %s on %s", resp.status_code, day_str)
            continue
        for item in resp.json().get("data", []):
            elem = item.get("element", "")
            if elem not in DART_NODES:
                continue
            for dp in item.get("dataPoints", []):
                key_name = dp.get("keyName")
                if key_name not in DART_PRICE_KEYS:
                    continue
                for val_entry in dp.get("values", []):
                    interval_utc = val_entry.get("intervalStartUtc", "")
                    for nested in val_entry.get("data", []):
                        price = nested.get("value", 0) or 0
                        try:
                            dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                            dt = dt.replace(tzinfo=_UTC).astimezone(_CST)
                            result[elem][key_name][dt.isoformat()] = float(price)
                        except Exception:
                            continue

    for node in DART_NODES:
        logger.info("  %s: DASPP=%d, RTSPP=%d", node, len(result[node]["DASPP"]), len(result[node]["RTSPP"]))
    return result


def fetch_generation(start_date: str, end_date: str, token: str) -> dict:
    """Return {asset: {'YYYY-MM-DD HE##': {'gen_mwh': float, 'rt_settlement': float}}}."""
    headers = {"Authorization": f"Bearer {token}"}
    result = {k: {} for k in DART_ASSETS}
    logger.info("Fetching hourly generation from %s to %s", start_date, end_date)

    for day_str in _daterange(start_date, end_date):
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}
        try:
            resp = requests.get(TENASKA_DART_DETAILS_URL, headers=headers, params=params, timeout=60)
        except Exception as e:
            logger.warning("Generation fetch error on %s: %s", day_str, e)
            continue
        if resp.status_code != 200:
            logger.warning("Generation HTTP %s on %s", resp.status_code, day_str)
            continue
        for item in resp.json().get("data", []):
            elem = item.get("element", "")
            asset = identify_asset(elem)
            if asset == "UNKNOWN":
                continue
            hourly_gen, hourly_rt_amt = {}, {}
            for dp in item.get("dataPoints", []):
                key_name = dp.get("keyName")
                for val_entry in dp.get("values", []):
                    interval_utc = val_entry.get("intervalStartUtc", "")
                    for nested in val_entry.get("data", []):
                        val = float(nested.get("value", 0) or 0)
                        try:
                            dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                            dt_cst = dt.replace(tzinfo=_UTC).astimezone(_CST)
                            date_str = dt_cst.strftime("%Y-%m-%d")
                            he = dt_cst.hour + 1
                            if he == 0:
                                he = 24
                            hour_key = f"{date_str} HE{he:02d}"
                            if key_name == "GEN_MWH_HRLY":
                                hourly_gen[hour_key] = val
                            elif key_name == "RTEIAMT":
                                hourly_rt_amt[hour_key] = hourly_rt_amt.get(hour_key, 0) + val
                        except Exception:
                            continue
            for hour_key, gen in hourly_gen.items():
                result[asset][hour_key] = {
                    "gen_mwh": gen,
                    "rt_settlement": hourly_rt_amt.get(hour_key, 0),
                }

    for asset in DART_ASSETS:
        logger.info("  %s: %d hourly records", asset, len(result[asset]))
    return result


def fetch_forecasts(start_date: str, end_date: str, token: str) -> tuple[dict, dict]:
    """Return ({asset: {hour_key: forecast_mw}}, {asset: bool_has_data})."""
    headers = {"Authorization": f"Bearer {token}"}
    result = {k: {} for k in DART_ASSETS}
    logger.info("Fetching renewable forecasts from %s to %s", start_date, end_date)

    for day_str in _daterange(start_date, end_date):
        params = {"begin": f"{day_str}T00:00:00Z", "end": f"{day_str}T23:59:59Z"}
        try:
            resp = requests.get(TENASKA_DART_FORECAST_URL, headers=headers, params=params, timeout=60)
        except Exception as e:
            logger.warning("Forecast fetch error on %s: %s", day_str, e)
            continue
        if resp.status_code != 200:
            logger.warning("Forecast HTTP %s on %s", resp.status_code, day_str)
            continue
        for item in resp.json().get("data", []):
            elem = item.get("element", "")
            asset = DART_FORECAST_ELEMENT_MAP.get(elem) or identify_asset(elem)
            if not asset or asset == "UNKNOWN":
                continue
            for dp in item.get("dataPoints", []):
                key_name = dp.get("keyName")
                if key_name not in ("STPPF_Forecast", "PVGRPP_Forecast"):
                    continue
                for val_entry in dp.get("values", []):
                    interval_utc = val_entry.get("intervalStartUtc", "")
                    for nested in val_entry.get("data", []):
                        val = float(nested.get("value", 0) or 0)
                        try:
                            dt = datetime.strptime(interval_utc, "%Y-%m-%dT%H:%M:%SZ")
                            dt_cst = dt.replace(tzinfo=_UTC).astimezone(_CST)
                            date_str = dt_cst.strftime("%Y-%m-%d")
                            he = dt_cst.hour + 1
                            if he == 0:
                                he = 24
                            hour_key = f"{date_str} HE{he:02d}"
                            existing = result[asset].get(hour_key, 0)
                            result[asset][hour_key] = max(existing, val)
                        except Exception:
                            continue

    availability = {}
    for asset in DART_ASSETS:
        nonzero = sum(1 for v in result[asset].values() if v > 0)
        availability[asset] = nonzero > 0
        logger.info("  %s: %d intervals, %d non-zero", asset, len(result[asset]), nonzero)
    return result, availability
