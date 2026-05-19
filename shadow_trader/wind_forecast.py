"""ERCOT-derived wind forecasts for assets that Tenaska's renewable-forecast feed doesn't cover.

Approach:
1. Pull hourly regional wind from ERCOT's "Wind Hourly Report by Geographical Region" via
   gridstatus. This gives both GEN_<region> (actuals) and STWPF_<region> (forecasts) per hour.
2. Compute each asset's historical share of its region's wind generation, by hour-of-day,
   over a lookback window. This captures the asset's typical contribution to the regional
   total at each part of the day (e.g. desert winds peak overnight).
3. Apply the historical share to the regional STWPF forecast to produce a per-asset forecast.

This is not as accurate as a per-resource ML forecast, but it uses only public data and is
defensible / auditable. For phase 1 it's the right tradeoff: real forward-looking forecast
without paying a forecasting vendor or training a model.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from gridstatus import Ercot

logger = logging.getLogger(__name__)

_CST = ZoneInfo("America/Chicago")


def _to_cst_hour_key(ts) -> tuple[str, int]:
    """Convert a pandas/datetime timestamp (any tz) to (date_str, HE int) in CST."""
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=ZoneInfo("UTC"))
    cst = ts.astimezone(_CST)
    he = cst.hour + 1
    if he == 0:
        he = 24
    return cst.strftime("%Y-%m-%d"), he


def fetch_ercot_regional_wind(target_date: str | None = None) -> "pandas.DataFrame":
    """Pull the ERCOT 'Wind Hourly Report by Geographical Region'.

    For forward bid generation, pass target_date=None (or 'latest') to get the most
    recently published report, which covers ~7 days of forward forecast plus several
    days of recent actuals -- everything generate_bids needs in one call.

    For historical backtests where you need the report that was published on a specific
    date (e.g. to reproduce what we *would* have seen at the time), pass that publish
    date. gridstatus interprets `date` as the report's publish date.
    """
    e = Ercot()
    key = target_date or "latest"
    logger.info("Fetching ERCOT regional wind report (publish=%s)", key)
    df = e.get_wind_actual_and_forecast_by_geographical_region_hourly(key)
    return df


def compute_region_share_by_hour(
    asset_hourly_gen: dict,
    region_df,
    region: str,
    lookback_days: int = 30,
) -> dict:
    """For one asset, compute its average hourly share of the region's actual wind generation.

    asset_hourly_gen: {'YYYY-MM-DD HE##': {'gen_mwh': float, ...}} as produced by data.fetch_generation
    region_df: DataFrame from fetch_ercot_regional_wind
    region: one of PANHANDLE, COASTAL, SOUTH, WEST, NORTH

    Returns {hour_of_day_int (1..24): share_float}. Uses lookback_days of recent overlap.
    """
    region_gen_col = f"GEN {region}"
    if region_gen_col not in region_df.columns:
        raise ValueError(f"Region GEN column {region_gen_col!r} not in region_df")

    # Index region GEN by (date_str, HE) so we can match asset rows.
    region_by_hour = {}
    for _, row in region_df.iterrows():
        date_str, he = _to_cst_hour_key(row["Interval Start"])
        gen = row[region_gen_col]
        if gen is None or (hasattr(gen, "__iter__") is False and str(gen) == "nan"):
            continue
        try:
            gen = float(gen)
        except Exception:
            continue
        if gen <= 0:
            continue
        region_by_hour[(date_str, he)] = gen

    # Restrict to the last lookback_days of dates that exist in region_by_hour
    available_dates = sorted({d for d, _ in region_by_hour})
    if not available_dates:
        logger.warning("No region GEN actuals found in lookback window")
        return {he: 0.0 for he in range(1, 25)}
    cutoff = available_dates[-lookback_days:][0]

    samples_by_hour = defaultdict(list)
    for hour_key, gen_data in asset_hourly_gen.items():
        try:
            date_str, he_part = hour_key.split(" ")
            he = int(he_part.replace("HE", ""))
        except Exception:
            continue
        if date_str < cutoff:
            continue
        asset_gen = gen_data.get("gen_mwh", 0)
        if asset_gen <= 0:
            continue
        region_gen = region_by_hour.get((date_str, he))
        if not region_gen:
            continue
        samples_by_hour[he].append(asset_gen / region_gen)

    shares = {}
    for he in range(1, 25):
        vals = samples_by_hour.get(he, [])
        shares[he] = sum(vals) / len(vals) if vals else 0.0
    logger.info(
        "  share-by-hour samples (last %d days): %s",
        lookback_days,
        {h: round(shares[h], 4) for h in range(1, 25, 4)},
    )
    return shares


def regional_stwpf_to_asset_forecast(
    region_df,
    region: str,
    share_by_hour: dict,
    only_future: bool = False,
) -> dict:
    """Apply per-hour-of-day share factor to the regional STWPF to produce {hour_key: mw}.

    only_future=True keeps rows that have no GEN (NaN) in the region — i.e. forward forecast
    rows that haven't been realized yet. Used by the bid-generation flow.
    """
    stwpf_col = f"STWPF {region}"
    gen_col = f"GEN {region}"
    if stwpf_col not in region_df.columns:
        raise ValueError(f"STWPF column {stwpf_col!r} not in region_df")

    out = {}
    for _, row in region_df.iterrows():
        stwpf = row[stwpf_col]
        if stwpf is None:
            continue
        try:
            stwpf = float(stwpf)
        except Exception:
            continue
        if only_future:
            gen = row[gen_col]
            try:
                if gen is not None and not (isinstance(gen, float) and str(gen) == "nan") and float(gen) >= 0:
                    continue  # has actual, skip
            except Exception:
                pass
        date_str, he = _to_cst_hour_key(row["Interval Start"])
        share = share_by_hour.get(he, 0.0)
        out[f"{date_str} HE{he:02d}"] = round(stwpf * share, 2)
    return out


def build_wind_forecast(
    asset_keys: list[str],
    asset_gen_by_key: dict[str, dict],
    region: str,
    target_date: str | None = None,
    lookback_days: int = 30,
    only_future: bool = False,
) -> dict:
    """Top-level: produce {asset_key: {hour_key: forecast_mw}} using ERCOT regional STWPF
    scaled by each asset's historical share of regional gen.

    Only emits forecasts for hours covered by the fetched ERCOT report (typically a
    ~10-day rolling window around the publish date). No derived/computed fallback --
    if a real STWPF isn't available for a given hour, that hour isn't forecast.
    """
    region_df = fetch_ercot_regional_wind(target_date)
    out = {}
    for key in asset_keys:
        gen = asset_gen_by_key.get(key, {})
        if not gen:
            logger.warning("No historical gen for %s; cannot derive share factor. Skipping.", key)
            out[key] = {}
            continue
        shares = compute_region_share_by_hour(gen, region_df, region, lookback_days=lookback_days)
        out[key] = regional_stwpf_to_asset_forecast(region_df, region, shares, only_future=only_future)
        nonzero = sum(1 for v in out[key].values() if v > 0)
        logger.info("  wind forecast %s: %d hours, %d non-zero, region=%s", key, len(out[key]), nonzero, region)
    return out
