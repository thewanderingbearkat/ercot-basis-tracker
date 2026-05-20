"""Raw-data cache: market_prices, generation, forecasts.

The dashboard needs to recompute the shadow strategy instantly when the bid_fraction
slider moves. Fetching from Tenaska/ERCOT takes minutes; we can't do that on every
slider movement. So we cache the raw inputs to disk and reload them in memory.

scripts/refresh_data.py rebuilds the cache. The Flask app reads from it.
"""
import json
import logging
import os
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from shadow_trader.config import DATA_DIR

logger = logging.getLogger(__name__)

CACHE_FILE = os.path.join(DATA_DIR, "raw_cache.json")


def save_cache(start: str, end: str, market_prices: dict, generation: dict, forecasts: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    blob = {
        "saved_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(),
        "start": start,
        "end": end,
        "market_prices": market_prices,
        "generation": generation,
        "forecasts": forecasts,
    }
    with open(CACHE_FILE, "w") as f:
        json.dump(blob, f, default=str)
    logger.info("Saved cache: %s (start=%s end=%s)", CACHE_FILE, start, end)


def load_cache() -> Optional[dict[str, Any]]:
    if not os.path.exists(CACHE_FILE):
        return None
    with open(CACHE_FILE) as f:
        return json.load(f)


def cache_age_seconds() -> Optional[float]:
    blob = load_cache()
    if not blob:
        return None
    saved_at = datetime.fromisoformat(blob["saved_at"])
    return (datetime.now(saved_at.tzinfo) - saved_at).total_seconds()


def latest_cache_date() -> Optional[str]:
    """Return the latest 'YYYY-MM-DD' present in the cache's generation data, or None
    if the cache is empty. Used by the auto-refresh loop to compute an incremental window.
    """
    blob = load_cache()
    if not blob:
        return None
    all_dates: set[str] = set()
    for asset_data in (blob.get("generation") or {}).values():
        for hk in asset_data:
            all_dates.add(hk.split(" ")[0])
    if not all_dates:
        return None
    return max(all_dates)


def merge_and_save_cache(
    new_market_prices: dict, new_generation: dict, new_forecasts: dict,
    fetch_start: str, fetch_end: str,
) -> dict:
    """Merge newly-fetched data into the existing cache (new wins on key conflict), save,
    return the merged blob. Used by incremental auto-refresh -- preserves older history
    while adding new days and updating any late-settling hours.
    """
    existing = load_cache() or {}
    existing_mp = existing.get("market_prices") or {}
    existing_gen = existing.get("generation") or {}
    existing_fc = existing.get("forecasts") or {}

    # Market prices: dict[node][kind][iso_ts] = price.
    # New entries overwrite existing entries with the same timestamp (settlement reruns).
    merged_mp: dict = {node: {kind: dict(series) for kind, series in kinds.items()}
                      for node, kinds in existing_mp.items()}
    for node, kinds in new_market_prices.items():
        merged_mp.setdefault(node, {})
        for kind, series in kinds.items():
            merged_mp[node].setdefault(kind, {})
            merged_mp[node][kind].update(series)

    # Generation: dict[asset]["YYYY-MM-DD HE##"] = record
    merged_gen: dict = {asset: dict(data) for asset, data in existing_gen.items()}
    for asset, data in new_generation.items():
        merged_gen.setdefault(asset, {})
        merged_gen[asset].update(data)

    # Forecasts: dict[asset]["YYYY-MM-DD HE##"] = mw
    merged_fc: dict = {asset: dict(data) for asset, data in existing_fc.items()}
    for asset, data in new_forecasts.items():
        merged_fc.setdefault(asset, {})
        merged_fc[asset].update(data)

    # Determine the union window for the cache metadata.
    all_dates: set[str] = set()
    for asset_data in merged_gen.values():
        for hk in asset_data:
            all_dates.add(hk.split(" ")[0])
    win_start = min(all_dates) if all_dates else fetch_start
    win_end = max(all_dates) if all_dates else fetch_end

    blob = {
        "saved_at": datetime.now(ZoneInfo("America/Chicago")).isoformat(),
        "start": win_start,
        "end": win_end,
        "market_prices": merged_mp,
        "generation": merged_gen,
        "forecasts": merged_fc,
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(blob, f, default=str)
    logger.info(
        "Merged cache: window now %s -> %s (fetched %s -> %s)",
        win_start, win_end, fetch_start, fetch_end,
    )
    return blob
