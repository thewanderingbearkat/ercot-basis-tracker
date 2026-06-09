"""Shadow Trading dashboard, exposed as a Flask Blueprint.

When mounted on the main ERCOT basis tracker app (see app.py's `register_blueprint`):
    GET  /shadow                       -- HTML dashboard tab
    GET  /api/shadow/strategy          -- recompute strategy at ?bid_fraction=X
    GET  /api/shadow/asset_day         -- per-asset daily detail
    GET  /api/shadow/ledger            -- ledger entries
    GET  /api/shadow/health            -- cache age + ledger counts
    POST /api/shadow/refresh           -- trigger cache refresh (background thread)

When run standalone via `scripts/serve.py` (local-only dev mode), the
`create_app()` helper at the bottom wraps the blueprint in a fresh Flask app
with the same URL paths under `/api/...` for backwards compatibility.
"""
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from flask import Blueprint, Flask, jsonify, redirect, render_template, request, session, url_for
from flask_cors import CORS

from shadow_trader.aggregate import aggregate
from shadow_trader.cache import CACHE_FILE, cache_age_seconds, latest_cache_date, load_cache, merge_and_save_cache
from shadow_trader.config import ASSET_CONFIG, DART_ASSETS
from shadow_trader.ledger import entries_by_status
from shadow_trader.risk import summarize
from shadow_trader.strategy import simulate_shadow_da

logger = logging.getLogger(__name__)

# Blueprint that can be registered on any Flask app. Routes here use paths
# WITHOUT the /shadow or /api/shadow prefix -- those get added at register time
# in the host application (see app.py:register_blueprint).
shadow_bp = Blueprint(
    "shadow",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)

# In-memory raw-data cache. Loaded on first request, refreshed when client POSTs /api/refresh.
_cache_lock = threading.Lock()
_cache_blob: dict[str, Any] | None = None
_refresh_thread: threading.Thread | None = None
_refresh_status = {"running": False, "last_error": None, "last_finished_at": None, "running_since": None}
# If a refresh appears to have been running for longer than this, assume the worker
# died mid-flight and the running=True flag got stuck. The auto-refresh loop and the
# manual /api/refresh endpoint both ignore the flag in that case.
REFRESH_STUCK_THRESHOLD_SECONDS = 15 * 60

# Auto-refresh background thread (matches NWOH dashboard's background_data_fetch pattern).
# Periodically calls the same refresh worker /api/refresh uses, so cached data tracks the
# upstream APIs without anyone clicking buttons. Configurable via env vars; default 30 min.
#   SHADOW_AUTO_REFRESH_INTERVAL  -- seconds between refresh ticks (default 1800)
#   SHADOW_AUTO_REFRESH_DAYS_BACK -- how many days of history to refresh each tick. Default
#                                   is the full BACKTEST_START_DATE window (slow but always
#                                   complete). Set to e.g. "21" to only refresh the last
#                                   three weeks each tick -- that way the refresh finishes
#                                   well inside the interval.
AUTO_REFRESH_INTERVAL_SECONDS = int(os.getenv("SHADOW_AUTO_REFRESH_INTERVAL", "1800"))
# Incremental auto-refresh: only fetch dates not already in the cache, plus a small overlap
# to catch late RT settlement updates on the most recent days. Default 2 days of overlap.
# Set higher if your data is taking longer than that to fully settle upstream.
AUTO_REFRESH_OVERLAP_DAYS = int(os.getenv("SHADOW_AUTO_REFRESH_OVERLAP_DAYS", "2"))
# When the cache is completely empty (first deploy on Render, fresh local clone),
# the auto-refresh would otherwise pull from BACKTEST_START_DATE all the way to today,
# which is ~140 days x 3 endpoints x 1s/day -> 15-25 min before /shadow shows anything.
# This env var caps the first seed to the last N days so the dashboard becomes usable
# in ~1-2 min. For full historical backfill, run `python scripts/refresh_data.py --start ...`
# manually after deploy, OR just wait several days for the cache to accumulate via the
# normal incremental ticks.
AUTO_REFRESH_INITIAL_SEED_DAYS = int(os.getenv("SHADOW_AUTO_REFRESH_INITIAL_SEED_DAYS", "21"))
# Refresh on startup if cache is older than this (so a stale overnight cache gets refreshed
# immediately when the server comes up, instead of waiting up to AUTO_REFRESH_INTERVAL_SECONDS)
AUTO_REFRESH_STALE_THRESHOLD_SECONDS = AUTO_REFRESH_INTERVAL_SECONDS
_auto_refresh_thread: threading.Thread | None = None
_auto_refresh_started = False


def _ensure_cache_loaded():
    """Load the on-disk cache into _cache_blob if missing OR if disk is newer than memory.

    On Render, the auto-refresh worker writes raw_cache.json to disk every ~30 min, then
    reloads _cache_blob in the same worker. But if the worker is killed mid-flight (Render
    recycle, OOM after writing the 800KB+ file), _cache_blob keeps pointing at whatever
    was loaded at boot while the disk file silently advances. Strategy + asset_day endpoints
    read _cache_blob, so the dashboard ends up showing hours-old actuals even though
    cache_age_seconds (which stats the file) looks fresh.

    Compare st_mtime to the in-memory saved_at and reload when disk is newer. Cheap (one
    stat call per request) and self-correcting whether the refresh thread is alive or dead.
    """
    global _cache_blob
    with _cache_lock:
        if _cache_blob is None:
            _cache_blob = load_cache()
            return
        try:
            disk_mtime = os.path.getmtime(CACHE_FILE)
        except OSError:
            return
        in_mem_saved_at = _cache_blob.get("saved_at")
        if not in_mem_saved_at:
            _cache_blob = load_cache()
            return
        try:
            in_mem_dt = datetime.fromisoformat(in_mem_saved_at).timestamp()
        except ValueError:
            _cache_blob = load_cache()
            return
        # Allow 1s slop for filesystem timestamp resolution.
        if disk_mtime > in_mem_dt + 1:
            logger.info("Reloading _cache_blob: disk is %.0fs newer than memory",
                        disk_mtime - in_mem_dt)
            _cache_blob = load_cache()


def _common_forecast_window(forecasts: dict) -> dict | None:
    """Return the date range where EVERY asset in DART_ASSETS has at least one non-zero
    forecast. This is the only window where portfolio-level shadow-strategy numbers are
    meaningful -- for any other dates, the wind assets contribute zero uplift and skew
    the aggregates. Returns None if any asset has no forecasts at all.
    """
    per_asset_dates = {}
    for asset in DART_ASSETS:
        dates = {hk.split(" ")[0] for hk, v in forecasts.get(asset, {}).items() if (v or 0) > 0}
        if not dates:
            return None
        per_asset_dates[asset] = dates
    common = set.intersection(*per_asset_dates.values())
    if not common:
        return None
    s = sorted(common)
    return {"start": s[0], "end": s[-1], "days": len(s)}


def _filter_to_window(market_prices, generation, forecasts, start, end):
    """If user specifies a sub-window via the slider's date inputs, filter cached data."""
    if not start and not end:
        return market_prices, generation, forecasts

    def in_window(date_str: str) -> bool:
        if start and date_str < start:
            return False
        if end and date_str > end:
            return False
        return True

    filt_gen = {
        asset: {hk: v for hk, v in d.items() if in_window(hk.split(" ")[0])}
        for asset, d in generation.items()
    }
    filt_fc = {
        asset: {hk: v for hk, v in d.items() if in_window(hk.split(" ")[0])}
        for asset, d in forecasts.items()
    }
    # Market prices are keyed by ISO timestamp; the date portion is the first 10 chars.
    filt_mp = {}
    for node, kinds in market_prices.items():
        filt_mp[node] = {}
        for kind, series in kinds.items():
            filt_mp[node][kind] = {ts: v for ts, v in series.items() if in_window(ts[:10])}
    return filt_mp, filt_gen, filt_fc


@shadow_bp.route("/shadow")
def index():
    return render_template("dashboard.html")


@shadow_bp.route("/api/shadow/health")
def health():
    _ensure_cache_loaded()
    blob = _cache_blob
    age = cache_age_seconds()
    common = _common_forecast_window(blob["forecasts"]) if blob else None
    # Route the running flag through the stuck-state self-heal helper so a refresh
    # that was killed mid-flight (worker recycle, OOM, etc.) doesn't permanently
    # grey out the dashboard's Refresh button. The helper clears the flag if it's
    # been "running" longer than REFRESH_STUCK_THRESHOLD_SECONDS.
    refresh_snapshot = dict(_refresh_status)
    refresh_snapshot["running"] = _refresh_is_actually_running()
    return jsonify({
        "auto_refresh_interval_seconds": AUTO_REFRESH_INTERVAL_SECONDS,
        "auto_refresh_started": _auto_refresh_started,
        "common_forecast_window": common,
        "cache_loaded": blob is not None,
        "cache_age_seconds": age,
        "cache_saved_at": blob["saved_at"] if blob else None,
        "cache_window": {"start": blob["start"], "end": blob["end"]} if blob else None,
        "ledger_counts": {
            "BID": len(entries_by_status("BID")),
            "AWARDED": len(entries_by_status("AWARDED")),
            "SETTLED": len(entries_by_status("SETTLED")),
        },
        "refresh": refresh_snapshot,
        "now": datetime.now(ZoneInfo("America/Chicago")).isoformat(),
    })


@shadow_bp.route("/api/shadow/strategy")
def strategy():
    """Recompute shadow strategy at the requested bid_fraction. All math in-process."""
    _ensure_cache_loaded()
    if _cache_blob is None:
        return jsonify({"error": "No data cache. Run `python scripts/refresh_data.py` first."}), 503

    bid_fraction = float(request.args.get("bid_fraction", 1.0))
    start = request.args.get("start")
    end = request.args.get("end")
    # DA bid threshold: skip the DA bid when DA price < threshold. Empty/missing = no gate.
    raw_thr = request.args.get("bid_threshold", "").strip()
    da_bid_threshold = float(raw_thr) if raw_thr else None

    # If the caller didn't pin a window, default to the intersection of all assets'
    # real-forecast dates. Outside this window, BKI/BKII have no STWPF and contribute
    # zero uplift, which skews portfolio aggregates / hit rates.
    if not start and not end:
        common = _common_forecast_window(_cache_blob["forecasts"])
        if common:
            start, end = common["start"], common["end"]

    market_prices, generation, forecasts = _filter_to_window(
        _cache_blob["market_prices"], _cache_blob["generation"], _cache_blob["forecasts"], start, end,
    )

    records = simulate_shadow_da(market_prices, generation, forecasts, "cached",
                                 bid_fraction=bid_fraction, da_bid_threshold=da_bid_threshold)
    agg = aggregate(records)
    risk = summarize(records)
    risk_per_asset = {
        asset: summarize([r for r in records if r["asset"] == asset])
        for asset in DART_ASSETS
    }

    # Per-asset daily uplift series for the cumulative chart.
    daily_by_asset: dict[str, dict[str, float]] = {a: {} for a in DART_ASSETS}
    for date, asset_buckets in agg["daily_asset"].items():
        for asset, b in asset_buckets.items():
            daily_by_asset[asset][date] = b["uplift"]
    dates = sorted({d for series in daily_by_asset.values() for d in series})
    series = {
        asset: [round(daily_by_asset[asset].get(d, 0), 2) for d in dates]
        for asset in DART_ASSETS
    }

    # Asset-config metadata for the cards
    assets_meta = {
        a: {
            "display_name": ASSET_CONFIG[a]["display_name"],
            "tech": ASSET_CONFIG[a]["tech"],
            "settlement_point": ASSET_CONFIG[a]["settlement_point"],
            "ppa_percent": ASSET_CONFIG[a]["ppa_percent"],
            "forecast_source": ASSET_CONFIG[a]["forecast_source"],
        } for a in DART_ASSETS
    }

    return jsonify({
        "bid_fraction": bid_fraction,
        "window": {"start": start or _cache_blob["start"], "end": end or _cache_blob["end"]},
        "assets_meta": assets_meta,
        "summary": agg,
        "risk_portfolio": risk,
        "risk_per_asset": risk_per_asset,
        "daily_dates": dates,
        "daily_uplift_by_asset": series,
    })


@shadow_bp.route("/api/shadow/ledger")
def ledger():
    return jsonify({"entries": entries_by_status(None)})


@shadow_bp.route("/api/shadow/asset_day")
def asset_day():
    """Return hour-by-hour bid/forecast/actual/prices/uplift for one asset on one date.

    Powers the per-asset "Today's Performance" panel: DA bid vs actual MW bars with
    over-gen / under-gen coloring, plus DA revenue + RT settlement summary.

    Query params:
        asset           required (BKI/BKII/HOLSTEIN)
        date            required (YYYY-MM-DD)
        bid_fraction    optional, default 1.0
    """
    from shadow_trader.strategy import _build_price_indexes

    _ensure_cache_loaded()
    if _cache_blob is None:
        return jsonify({"error": "No data cache. Run scripts/refresh_data.py first."}), 503

    asset = request.args.get("asset")
    date = request.args.get("date")
    if not asset or asset not in ASSET_CONFIG:
        return jsonify({"error": "missing/unknown asset"}), 400
    if not date:
        return jsonify({"error": "missing date (YYYY-MM-DD)"}), 400
    bid_fraction = float(request.args.get("bid_fraction", 1.0))
    raw_thr = request.args.get("bid_threshold", "").strip()
    da_bid_threshold = float(raw_thr) if raw_thr else None

    from shadow_trader.config import PPA_HUB_NODE
    cfg = ASSET_CONFIG[asset]
    node = cfg["settlement_point"]
    da_idx, rt_idx = _build_price_indexes(_cache_blob["market_prices"])
    node_da, node_rt = da_idx.get(node, {}), rt_idx.get(node, {})
    hub_rt = rt_idx.get(PPA_HUB_NODE, {})

    ppa_price = float(cfg.get("ppa_price", 0) or 0)
    ppa_pct = float(cfg.get("ppa_percent", 0) or 0) / 100.0
    basis_exposure = float(cfg.get("ppa_basis_exposure", 0) or 0)
    if basis_exposure > 1:
        basis_exposure = basis_exposure / 100.0

    asset_gen = _cache_blob["generation"].get(asset, {})
    asset_fc = _cache_blob["forecasts"].get(asset, {})

    hourly = []
    sum_da_bid = sum_actual = sum_da_rev = sum_rt_settle = sum_uplift = sum_rt_only = 0.0
    sum_ppa_fixed = sum_ppa_floating = sum_net_ppa = 0.0
    awarded_hours = 0
    for he in range(1, 25):
        hour = he - 1
        hk = f"{date} HE{he:02d}"
        forecast_mw = float(asset_fc.get(hk, 0))
        gen_data = asset_gen.get(hk)
        actual_mw = float(gen_data["gen_mwh"]) if gen_data else None
        da_price = node_da.get((date, hour))
        rt_price = node_rt.get((date, hour))
        rt_hub_price = hub_rt.get((date, hour))
        da_bid_mw = forecast_mw * bid_fraction
        # Apply the same DA-bid threshold gate as simulate_shadow_da so the per-hour
        # detail panel matches what the strategy view shows.
        skipped_by_threshold = False
        if (da_bid_threshold is not None and da_price is not None
                and da_price < da_bid_threshold):
            da_bid_mw = 0.0
            skipped_by_threshold = True

        row = {
            "he": he,
            "forecast_mw": round(forecast_mw, 2),
            "da_bid_mw": round(da_bid_mw, 2),
            "actual_mw": round(actual_mw, 2) if actual_mw is not None else None,
            "da_price": round(da_price, 2) if da_price is not None else None,
            "rt_price": round(rt_price, 2) if rt_price is not None else None,
            "rt_hub_price": round(rt_hub_price, 2) if rt_hub_price is not None else None,
            "da_revenue": None,
            "rt_deviation_mw": None,
            "rt_settlement": None,
            "shadow_total": None,
            "rt_only_revenue": None,
            "uplift": None,
            "ppa_fixed": None,
            "ppa_floating": None,
            "net_ppa": None,
            "direction": "future",
            "skipped_by_threshold": skipped_by_threshold,
        }

        if da_price is not None and da_bid_mw > 0:
            row["da_revenue"] = round(da_bid_mw * da_price, 2)
            sum_da_rev += row["da_revenue"]
            awarded_hours += 1

        if actual_mw is not None and rt_price is not None:
            deviation = actual_mw - da_bid_mw
            rt_settle = deviation * rt_price
            da_rev = row["da_revenue"] or 0.0
            shadow = da_rev + rt_settle
            rt_only = actual_mw * rt_price
            row["rt_deviation_mw"] = round(deviation, 2)
            row["rt_settlement"] = round(rt_settle, 2)
            row["shadow_total"] = round(shadow, 2)
            row["rt_only_revenue"] = round(rt_only, 2)
            row["uplift"] = round(shadow - rt_only, 2)
            sum_actual += actual_mw
            sum_rt_settle += rt_settle
            sum_uplift += (shadow - rt_only)
            sum_rt_only += rt_only

            # PPA fixed-for-floating swap, settled at RT prices (matches NWOH model)
            if ppa_price > 0 and ppa_pct > 0:
                hub_for_swap = rt_hub_price if rt_hub_price is not None else rt_price
                ppa_volume = actual_mw * ppa_pct
                floating_price = basis_exposure * hub_for_swap + (1.0 - basis_exposure) * rt_price
                ppa_fixed = ppa_volume * ppa_price
                ppa_floating = ppa_volume * floating_price
                net_ppa = ppa_fixed - ppa_floating
                row["ppa_fixed"] = round(ppa_fixed, 2)
                row["ppa_floating"] = round(ppa_floating, 2)
                row["net_ppa"] = round(net_ppa, 2)
                sum_ppa_fixed += ppa_fixed
                sum_ppa_floating += ppa_floating
                sum_net_ppa += net_ppa

            if deviation > 1:
                row["direction"] = "over"
            elif deviation < -1:
                row["direction"] = "under"
            else:
                row["direction"] = "matched"
        sum_da_bid += da_bid_mw

        hourly.append(row)

    shadow_market = sum_da_rev + sum_rt_settle
    total_pnl_shadow = shadow_market + sum_net_ppa
    total_pnl_rt_only = sum_rt_only + sum_net_ppa

    return jsonify({
        "asset": asset,
        "asset_meta": {
            "display_name": cfg["display_name"],
            "tech": cfg["tech"],
            "settlement_point": node,
            "ppa_percent": cfg["ppa_percent"],
            "forecast_source": cfg["forecast_source"],
            "ppa_price": cfg.get("ppa_price"),
            "ppa_basis_exposure": basis_exposure,
        },
        "date": date,
        "bid_fraction": bid_fraction,
        "da_bid_threshold": da_bid_threshold,
        "hourly": hourly,
        "summary": {
            "total_da_bid_mw": round(sum_da_bid, 2),
            "awarded_hours": awarded_hours,
            "total_actual_mw": round(sum_actual, 2),
            "total_rt_deviation_mw": round(sum_actual - sum_da_bid, 2),
            # Market leg (ERCOT DA + RT)
            "da_revenue": round(sum_da_rev, 2),
            "rt_settlement": round(sum_rt_settle, 2),
            "shadow_total": round(shadow_market, 2),
            "rt_only_revenue": round(sum_rt_only, 2),
            "uplift": round(sum_uplift, 2),
            # PPA leg (fixed-for-floating swap)
            "ppa_fixed": round(sum_ppa_fixed, 2),
            "ppa_floating": round(sum_ppa_floating, 2),
            "net_ppa": round(sum_net_ppa, 2),
            # Bottom-line PnL = market + PPA
            "total_pnl_shadow": round(total_pnl_shadow, 2),
            "total_pnl_rt_only": round(total_pnl_rt_only, 2),
        },
    })


def _refresh_worker(start: str | None, end: str | None, merge: bool = False):
    """Fetch market_prices / generation / forecasts and update the cache.

    merge=False (default): overwrite the cache with whatever was fetched in [start, end].
                           Used by manual `/api/refresh` and one-shot `refresh_data.py`.
    merge=True:            merge the new fetch into the existing cache so older days stay.
                           Used by the auto-refresh loop to incrementally extend the cache
                           without re-fetching settled history.
    """
    global _cache_blob
    _refresh_status["running"] = True
    _refresh_status["last_error"] = None
    _refresh_status["running_since"] = datetime.now(ZoneInfo("America/Chicago")).isoformat()
    try:
        # Import here so we don't pay the import cost on app startup.
        from shadow_trader.config import ASSET_CONFIG, BACKTEST_START_DATE, DART_ASSETS
        from shadow_trader.data import fetch_forecasts, fetch_generation, fetch_market_prices
        from shadow_trader.tenaska import get_tenaska_token
        from shadow_trader.wind_forecast import build_wind_forecast
        from shadow_trader.cache import save_cache

        s = start or BACKTEST_START_DATE
        e = end or datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
        logger.info("Refreshing data %s -> %s (merge=%s)", s, e, merge)
        token = get_tenaska_token()
        if not token:
            raise RuntimeError("Could not obtain Tenaska token")
        market_prices = fetch_market_prices(s, e, token)
        generation = fetch_generation(s, e, token)
        forecasts = {k: {} for k in DART_ASSETS}
        tenaska_assets = [a for a in DART_ASSETS if ASSET_CONFIG[a]["forecast_source"] == "tenaska"]
        ercot_assets = [a for a in DART_ASSETS if ASSET_CONFIG[a]["forecast_source"] == "ercot_regional"]
        if tenaska_assets:
            tf, _ = fetch_forecasts(s, e, token)
            for a in tenaska_assets:
                forecasts[a] = tf.get(a, {})
        if ercot_assets:
            # For the wind share factor we want the FULL recent generation history so the
            # per-hour share factor is stable. If we're doing an incremental merge we only
            # just fetched the recent window, so combine with what's already in the cache.
            gen_for_share = generation
            if merge:
                existing = load_cache() or {}
                merged_gen = {a: dict(existing.get("generation", {}).get(a, {})) for a in DART_ASSETS}
                for a in DART_ASSETS:
                    merged_gen[a].update(generation.get(a, {}))
                gen_for_share = merged_gen
            by_region: dict[str, list[str]] = {}
            for a in ercot_assets:
                by_region.setdefault(ASSET_CONFIG[a]["ercot_region"], []).append(a)
            for region, assets in by_region.items():
                # Wind forecasts depend on gridstatus's ERCOT regional wind report, which
                # has changed method names between versions. If the call fails for any
                # reason (gridstatus version mismatch, ERCOT MIS outage, etc.), don't kill
                # the whole refresh -- leave wind asset forecasts empty for this tick and
                # let everything else (prices, generation, Holstein forecast) still cache.
                try:
                    wf = build_wind_forecast(asset_keys=assets, asset_gen_by_key=gen_for_share, region=region, target_date=None, lookback_days=30)
                    for a in assets:
                        forecasts[a] = wf.get(a, {})
                except Exception as wf_err:
                    logger.exception("Wind forecast for region %s failed; continuing with empty wind forecasts: %s", region, wf_err)
                    for a in assets:
                        forecasts[a] = {}
        if merge:
            merge_and_save_cache(market_prices, generation, forecasts, s, e)
        else:
            save_cache(s, e, market_prices, generation, forecasts)
        with _cache_lock:
            _cache_blob = load_cache()
        _refresh_status["last_finished_at"] = datetime.now(ZoneInfo("America/Chicago")).isoformat()
        logger.info("Cache refresh complete (merge=%s)", merge)
    except Exception as ex:
        _refresh_status["last_error"] = str(ex)
        logger.exception("Refresh failed: %s", ex)
    finally:
        _refresh_status["running"] = False
        _refresh_status["running_since"] = None


def _refresh_is_actually_running() -> bool:
    """Return True only if a refresh worker is genuinely in flight, not stuck.

    A 'stuck' state happens when the worker process is killed mid-refresh (Render
    worker recycle, container OOM, etc.) -- the in-memory running=True flag never
    gets cleared because the finally block didn't execute. Without this check, both
    the auto-refresh loop and the manual refresh button would be permanently blocked.
    """
    if not _refresh_status["running"]:
        return False
    started = _refresh_status.get("running_since")
    if not started:
        return True  # Flag is on but no timestamp -- treat as running (conservative)
    try:
        started_dt = datetime.fromisoformat(started)
        age = (datetime.now(started_dt.tzinfo) - started_dt).total_seconds()
        if age > REFRESH_STUCK_THRESHOLD_SECONDS:
            logger.warning(
                "Refresh has been 'running' for %.0fs (> %ds threshold) -- assuming the "
                "worker died and clearing the stuck flag.",
                age, REFRESH_STUCK_THRESHOLD_SECONDS,
            )
            _refresh_status["running"] = False
            _refresh_status["running_since"] = None
            _refresh_status["last_error"] = f"Previous refresh appears stuck (running {age:.0f}s); reset."
            return False
        return True
    except Exception:
        return True


@shadow_bp.route("/api/shadow/refresh", methods=["POST"])
def refresh():
    """Manual refresh trigger.

    Default behavior (empty body / no start/end): incremental merge from
    (latest_cache_date - overlap_days) through today. Same lightweight refresh
    as the auto-loop -- finishes in ~30-60s.

    To force a full historical rebuild, POST with explicit `start` (and optional
    `end`) and `mode: 'overwrite'` in the JSON body. That uses save_cache (not
    merge) and is much slower (~15-20 min for a YTD window) -- only useful when
    you want to nuke and re-fetch the entire cache.
    """
    global _refresh_thread
    if _refresh_is_actually_running():
        return jsonify({"status": "already_running"}), 200

    body = request.get_json(silent=True) or {}
    explicit_start = body.get("start")
    explicit_end = body.get("end")
    overwrite_mode = body.get("mode") == "overwrite"

    if explicit_start or overwrite_mode:
        # Caller wants a custom window or a full overwrite rebuild. Use the explicit
        # dates as-is (start defaults to BACKTEST_START_DATE inside the worker).
        kwargs = {"start": explicit_start, "end": explicit_end, "merge": not overwrite_mode}
    else:
        # Default: same incremental merge the auto-loop does. Compute the window
        # from the on-disk cache so this is cheap (~3-5 days).
        from datetime import timedelta
        from shadow_trader.config import BACKTEST_START_DATE
        today = datetime.now(ZoneInfo("America/Chicago")).date()
        latest = latest_cache_date()
        if latest is None:
            seed_start = (today - timedelta(days=AUTO_REFRESH_INITIAL_SEED_DAYS)).strftime("%Y-%m-%d")
            start = max(seed_start, BACKTEST_START_DATE)
        else:
            latest_dt = datetime.strptime(latest, "%Y-%m-%d").date()
            start = (latest_dt - timedelta(days=AUTO_REFRESH_OVERLAP_DAYS)).strftime("%Y-%m-%d")
        kwargs = {"start": start, "end": today.strftime("%Y-%m-%d"), "merge": True}

    _refresh_thread = threading.Thread(
        target=lambda: _refresh_worker(**kwargs), daemon=True,
    )
    _refresh_thread.start()
    return jsonify({"status": "started", **kwargs}), 202


def _periodic_refresh_loop():
    """Background loop: refresh the data cache when it crosses the staleness threshold.

    Checks the on-disk cache age on every tick rather than scheduling refreshes from
    process-start time. That way if Render recycles the worker (and kills this thread),
    the new thread spawned by module import on the new worker sees the same on-disk
    cache and picks up where the previous one left off. The previous pattern (sleep 30
    min before first refresh) meant a worker that gets recycled every ~20 min would
    never actually trigger a refresh.

    Tick cadence: poll every CHECK_INTERVAL seconds (default 60s). On each tick, if
    cache age > AUTO_REFRESH_INTERVAL_SECONDS, fire a refresh. Otherwise sleep and
    re-check next tick.
    """
    CHECK_INTERVAL = 60  # seconds between staleness checks; cheap because it's just a stat()
    logger.info("Auto-refresh loop starting (interval=%ds, check=%ds)",
                AUTO_REFRESH_INTERVAL_SECONDS, CHECK_INTERVAL)

    while True:
        if _refresh_is_actually_running():
            # A manual or previous auto-refresh is still in flight. Skip this tick and
            # try again next interval rather than queueing a second concurrent refresh.
            logger.info("Auto-refresh tick skipped (a refresh is already running)")
            time.sleep(CHECK_INTERVAL)
            continue

        age = cache_age_seconds()
        if age is not None and age < AUTO_REFRESH_INTERVAL_SECONDS:
            # Cache still fresh. Sleep just long enough for it to potentially age out,
            # then loop back to re-check. Bounded by CHECK_INTERVAL so we never sleep
            # past a worker-recycle without checking.
            time.sleep(min(CHECK_INTERVAL, AUTO_REFRESH_INTERVAL_SECONDS - age + 1))
            continue

        # Cache is stale (or missing) — refresh now.
        if True:
            try:
                # Incremental refresh: only fetch dates that aren't already in the cache,
                # plus a small overlap to catch late RT settlement updates on the most
                # recent days. If the cache is empty (first deploy / fresh machine), seed
                # just the last AUTO_REFRESH_INITIAL_SEED_DAYS days so the dashboard becomes
                # responsive quickly. Full history can be backfilled later via refresh_data.py.
                from datetime import timedelta
                from shadow_trader.config import BACKTEST_START_DATE
                today = datetime.now(ZoneInfo("America/Chicago")).date()
                today_str = today.strftime("%Y-%m-%d")
                latest = latest_cache_date()
                if latest is None:
                    seed_start = (today - timedelta(days=AUTO_REFRESH_INITIAL_SEED_DAYS)).strftime("%Y-%m-%d")
                    # Don't go further back than BACKTEST_START_DATE (forecast/gen data
                    # may not exist beyond that)
                    start = max(seed_start, BACKTEST_START_DATE)
                    logger.info("Auto-refresh: cache empty, seeding last %dd (%s -> %s)",
                                AUTO_REFRESH_INITIAL_SEED_DAYS, start, today_str)
                else:
                    latest_dt = datetime.strptime(latest, "%Y-%m-%d").date()
                    start_dt = latest_dt - timedelta(days=AUTO_REFRESH_OVERLAP_DAYS)
                    start = start_dt.strftime("%Y-%m-%d")
                    logger.info("Auto-refresh: latest cache date %s, fetching %s -> %s (overlap=%dd)",
                                latest, start, today_str, AUTO_REFRESH_OVERLAP_DAYS)
                _refresh_worker(start, today_str, merge=True)
            except Exception as e:
                logger.exception("Auto-refresh tick raised: %s", e)
        # After a refresh attempt (success or failure), sleep one CHECK_INTERVAL
        # before re-evaluating. The age-based loop above handles further sleeping
        # if the just-saved cache is fresh enough to wait on.
        time.sleep(CHECK_INTERVAL)


def _start_auto_refresh():
    """Idempotent: spawn the periodic refresh thread exactly once per process."""
    global _auto_refresh_thread, _auto_refresh_started
    if _auto_refresh_started:
        return
    _auto_refresh_started = True
    logger.info("Shadow auto-refresh: spawning daemon thread")
    _auto_refresh_thread = threading.Thread(target=_periodic_refresh_loop, daemon=True, name="auto-refresh")
    _auto_refresh_thread.start()


# Start the auto-refresh thread EAGERLY at module import time, not on first request.
# Lazy-start via @before_app_request is brittle: it depends on a request actually
# routing through this blueprint before the host app's own before_request handlers
# short-circuit (e.g. a login redirect). On Render with gunicorn workers, that
# can fail to ever trigger the thread.
logger.info("shadow_trader.web imported -- starting auto-refresh thread now")
_start_auto_refresh()


@shadow_bp.before_request
def _require_login_for_shadow():
    """All /shadow + /api/shadow/* routes require the same session-cookie login as
    the host app's other protected views. Matches the behavior of `login_required`
    in ercot-basis-tracker/app.py (session key 'authenticated', login route 'login').
    Standalone mode (scripts/serve.py) has no login route registered, so we skip
    the check there. We detect standalone mode by absence of the 'login' endpoint.
    """
    # If running under standalone create_app() the host hasn't registered a /login
    # route, so don't try to redirect to it.
    if "login" not in (request.url_rule.endpoint if request.url_rule else "") \
       and not request.endpoint == "shadow.health" \
       and not session.get("authenticated"):
        # Only enforce if a /login endpoint exists in the app (i.e. host mode).
        from flask import current_app
        if "login" in current_app.view_functions:
            return redirect(url_for("login"))


@shadow_bp.before_app_request
def _ensure_auto_refresh_running():
    """Lazy-start the auto-refresh thread on the first request to the host app.
    Works under gunicorn/uwsgi without explicit boot hooks."""
    _start_auto_refresh()


def create_app():
    """Local-only standalone mode for scripts/serve.py. Wraps the blueprint in a
    fresh Flask app and re-exposes the routes WITHOUT the /api/shadow prefix, so
    the local dashboard's existing JS still works unchanged."""
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    )
    CORS(app)
    # Standalone mode: register the blueprint at root so URLs like /api/shadow/strategy
    # work as a superset of the old /api/strategy. The local dashboard template uses
    # /api/shadow/... paths so this is consistent with how it runs inside ercot-basis-tracker.
    app.register_blueprint(shadow_bp)
    _start_auto_refresh()
    return app
