"""Core shadow-DA strategy: forecast-based DA bid simulation, hourly P&L calculation."""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from shadow_trader.config import ASSET_CONFIG, DART_ASSETS, HUB_NODE, TRAILING_AVG_DAYS

logger = logging.getLogger(__name__)


def identify_asset(element_name: str) -> str:
    """Map a Tenaska element name to an internal asset key. Returns 'UNKNOWN' on no match.

    Only matches 'Generation' or '- Gen' elements; net-metering elements are excluded
    because they include both buy and sell legs.
    """
    element_lower = (element_name or "").lower()
    if "- generation" not in element_lower and not element_lower.endswith("- gen"):
        return "UNKNOWN"
    for asset_key in DART_ASSETS:
        config = ASSET_CONFIG.get(asset_key, {})
        for pattern in config.get("element_patterns", []):
            if pattern.lower() in element_lower:
                return asset_key
    return "UNKNOWN"


def _hourly_rt_price(market_prices: dict, node: str, date_str: str, he: int) -> float | None:
    """Average the four 15-min RTSPP intervals into one hourly price (linear scan).

    NOTE: O(n) per call. For batch simulation (simulate_shadow_da) we precompute an index
    via `_build_price_indexes` and use _hourly_rt_price_idx instead. This linear version
    is kept for use by the one-off scripts (record_awards, settle_day) where we look up a
    handful of hours for a single date.
    """
    hour = he - 1
    rtspp = market_prices.get(node, {}).get("RTSPP", {})
    prices = []
    for iso_str, price in rtspp.items():
        try:
            dt = datetime.fromisoformat(iso_str)
        except Exception:
            continue
        if dt.strftime("%Y-%m-%d") == date_str and dt.hour == hour:
            prices.append(price)
    return sum(prices) / len(prices) if prices else None


def _da_price(market_prices: dict, node: str, date_str: str, he: int) -> float | None:
    hour = he - 1
    daspp = market_prices.get(node, {}).get("DASPP", {})
    for iso_str, price in daspp.items():
        try:
            dt = datetime.fromisoformat(iso_str)
        except Exception:
            continue
        if dt.strftime("%Y-%m-%d") == date_str and dt.hour == hour:
            return price
    return None


def _build_price_indexes(market_prices: dict) -> tuple[dict, dict]:
    """Pre-index DA and RT prices by (node, date_str, hour) so the simulator can look up
    each hour in O(1) instead of scanning the full series per lookup.

    Returns (da_idx, rt_idx) where:
        da_idx[node][(date_str, hour)] = float
        rt_idx[node][(date_str, hour)] = mean(15-min interval prices in that hour)
    """
    da_idx: dict[str, dict[tuple[str, int], float]] = {}
    rt_idx: dict[str, dict[tuple[str, int], float]] = {}
    for node, kinds in market_prices.items():
        da_idx[node] = {}
        rt_buckets: dict[tuple[str, int], list[float]] = {}
        for iso_str, price in kinds.get("DASPP", {}).items():
            try:
                dt = datetime.fromisoformat(iso_str)
            except Exception:
                continue
            da_idx[node][(dt.strftime("%Y-%m-%d"), dt.hour)] = float(price)
        for iso_str, price in kinds.get("RTSPP", {}).items():
            try:
                dt = datetime.fromisoformat(iso_str)
            except Exception:
                continue
            rt_buckets.setdefault((dt.strftime("%Y-%m-%d"), dt.hour), []).append(float(price))
        rt_idx[node] = {k: (sum(v) / len(v)) if v else 0.0 for k, v in rt_buckets.items()}
    return da_idx, rt_idx


def trailing_avg_forecast(generation: dict, target_date: str, lookback_days: int = TRAILING_AVG_DAYS) -> dict:
    """Trailing N-day hour-of-day average of actual gen, used as a naive forecast baseline.

    Useful as a control to compare against the Tenaska-provided STPPF/PVGRPP forecast.
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    result = {k: {} for k in DART_ASSETS}
    for asset in DART_ASSETS:
        hourly_samples = defaultdict(list)
        for day_offset in range(1, lookback_days + 1):
            lookback_date = (target_dt - timedelta(days=day_offset)).strftime("%Y-%m-%d")
            for he in range(1, 25):
                hour_key = f"{lookback_date} HE{he:02d}"
                rec = generation.get(asset, {}).get(hour_key)
                if rec:
                    hourly_samples[he].append(rec["gen_mwh"])
        for he in range(1, 25):
            hour_key = f"{target_date} HE{he:02d}"
            values = hourly_samples.get(he, [])
            result[asset][hour_key] = sum(values) / len(values) if values else 0
    return result


def simulate_shadow_da(
    market_prices: dict,
    generation: dict,
    forecasts: dict,
    forecast_mode: str = "tenaska",
    bid_fraction: float = 1.0,
    da_bid_threshold: float | None = None,
) -> list[dict]:
    """Simulate selling `bid_fraction × forecast_mw` into DA each hour, settling deviation in RT.

    Returns a list of hourly records, one per asset×hour. Pure function — no I/O.

    Math per hour:
        da_bid_mw       = bid_fraction × forecast_mw    (0 if da_node_price < da_bid_threshold)
        da_revenue      = da_bid_mw × da_node_price
        rt_deviation    = actual_gen_mw - da_bid_mw      (positive = over-generated, sold extra at RT)
        rt_settlement   = rt_deviation × rt_node_price
        shadow_total    = da_revenue + rt_settlement     (theoretical DART revenue)
        rt_only_revenue = actual_gen_mw × rt_node_price  (counterfactual: no DA bid)
        uplift          = shadow_total - rt_only_revenue (positive = strategy beats RT-only)

    da_bid_threshold: when set, skip the DA bid (set da_bid_mw=0) on any hour where the
        DA clearing price is below the threshold. Lets actual gen flow at RT instead of
        committing MW to a low-DA print. NOTE: backtest uses the actual cleared DA price
        as a perfect proxy for the threshold gate, which is optimistic — in live trading
        you'd gate on the DA price FORECAST at bid time. So treat replayed savings as an
        upper bound; the live savings depend on how accurately you can predict DA prices.
    """
    # Build O(1) price indexes once per call. Previous version did a linear scan of the
    # full DASPP/RTSPP dict for every (asset, hour, lookup) tuple -- 30s+ per /api/strategy.
    da_idx, rt_idx = _build_price_indexes(market_prices)
    hub_da, hub_rt = da_idx.get(HUB_NODE, {}), rt_idx.get(HUB_NODE, {})

    records = []
    for asset in DART_ASSETS:
        config = ASSET_CONFIG.get(asset, {})
        node = config.get("settlement_point", "NBOHR_RN")
        node_da, node_rt = da_idx.get(node, {}), rt_idx.get(node, {})
        ppa_price = float(config.get("ppa_price", 0) or 0)
        ppa_pct = float(config.get("ppa_percent", 0) or 0) / 100.0
        basis_exposure = float(config.get("ppa_basis_exposure", 0) or 0)
        # Accept either 0..1 or 0..100 for basis_exposure for back-compat with the older config
        if basis_exposure > 1:
            basis_exposure = basis_exposure / 100.0
        for hour_key, gen_data in sorted(generation.get(asset, {}).items()):
            parts = hour_key.split(" ")
            if len(parts) != 2:
                continue
            date_str = parts[0]
            he = int(parts[1].replace("HE", ""))
            hour = he - 1
            actual_gen = gen_data["gen_mwh"]
            key = (date_str, hour)
            da_node = node_da.get(key)
            rt_node = node_rt.get(key)
            if da_node is None or rt_node is None:
                continue
            rt_hub_val = hub_rt.get(key, 0)
            forecast_mw = forecasts.get(asset, {}).get(hour_key, 0)
            da_bid_mw = forecast_mw * bid_fraction
            if da_bid_threshold is not None and da_node < da_bid_threshold:
                da_bid_mw = 0.0

            # ERCOT market leg (DA + RT)
            da_revenue = da_bid_mw * da_node
            rt_deviation = actual_gen - da_bid_mw
            rt_settlement = rt_deviation * rt_node
            market_total = da_revenue + rt_settlement
            rt_only_market = actual_gen * rt_node

            # PPA leg (fixed-for-floating swap on the PPA portion of actual gen).
            # Floating leg blends hub and node by basis_exposure -- see config.py header
            # for the derivation. basis_exposure = 1.0 -> pure hub floating leg.
            ppa_volume = actual_gen * ppa_pct
            floating_price = basis_exposure * rt_hub_val + (1.0 - basis_exposure) * rt_node
            ppa_fixed = ppa_volume * ppa_price
            ppa_floating = ppa_volume * floating_price
            net_ppa = ppa_fixed - ppa_floating

            # Total PnL = market revenue (whatever strategy) + PPA net settlement
            shadow_total_pnl = market_total + net_ppa
            rt_only_total_pnl = rt_only_market + net_ppa
            # PPA cancels out of uplift comparison since both scenarios apply the same PPA
            # to the same actual gen. Uplift = market_total - rt_only_market.
            uplift = market_total - rt_only_market

            records.append({
                "asset": asset,
                "date": date_str,
                "hour_ending": he,
                "hour_key": hour_key,
                "forecast_mw": round(forecast_mw, 2),
                "bid_fraction": bid_fraction,
                "da_bid_mw": round(da_bid_mw, 2),
                "actual_gen_mw": round(actual_gen, 2),
                "da_node_price": round(da_node, 2),
                "rt_node_price": round(rt_node, 2),
                "da_hub_price": round(hub_da.get(key, 0), 2),
                "rt_hub_price": round(rt_hub_val, 2),
                # Market leg
                "da_revenue": round(da_revenue, 2),
                "rt_settlement": round(rt_settlement, 2),
                "market_total": round(market_total, 2),
                "rt_only_market": round(rt_only_market, 2),
                # PPA leg
                "ppa_price": ppa_price,
                "ppa_basis_exposure": basis_exposure,
                "ppa_volume_mwh": round(ppa_volume, 2),
                "ppa_floating_price": round(floating_price, 2),
                "ppa_fixed_payment": round(ppa_fixed, 2),
                "ppa_floating_payment": round(ppa_floating, 2),
                "net_ppa": round(net_ppa, 2),
                # Bottom line
                "shadow_total_pnl": round(shadow_total_pnl, 2),
                "rt_only_total_pnl": round(rt_only_total_pnl, 2),
                # Legacy fields kept for the dashboard's uplift/risk views
                "shadow_total": round(market_total, 2),
                "rt_only_revenue": round(rt_only_market, 2),
                "uplift": round(uplift, 2),
                "forecast_mode": forecast_mode,
            })
    return records
