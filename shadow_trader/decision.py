"""DART trader decision engine: when to be in the DA market, when to stay out, how much to bid.

Why this exists: the naive strategy (bid = fraction x forecast, every hour, every day) is
how nobody actually trades. Per hour, the uplift of a shadow DA sale over RT-only is exactly

    uplift = da_bid_mw x (DA - RT)

so the P&L driver is the DART spread on the hours you choose to be in, and the tail risk is
holding a DA position into an RT price blowout (scarcity pricing). Forecast error compounds
this: bidding MW you don't physically generate is a naked RT short, which is precisely the
position that gets run over at the cap. A real desk therefore does three things this module
implements:

1. EDGE FILTER -- only participate in hours where DA has recently cleared at a premium to RT
   (trailing DART by node and hour-of-day). No expected edge, no position.
2. WEATHER RISK GATES -- stand down (or size down) ahead of conditions that historically
   produce RT blowouts and forecast busts:
     - extreme heat/cold at the plant (proxy for system scarcity conditions, when RT can
       print at multiples of DA and a short position is catastrophic),
     - large hour-over-hour swings in hub-height wind (frontal passages: the hardest days
       for wind forecasting, and the classic DART widow-maker in West Texas),
     - near-cut-out wind speeds / extreme gusts (turbines drop offline en masse),
     - high cloud-cover volatility or model disagreement for solar (irradiance busts).
3. CONFIDENCE-AWARE SIZING -- track our own forecast error (bias and sigma by asset and
   hour-of-day, trailing window), bias-correct the forecast, and bid a conservative
   quantile (forecast - k*sigma) rather than the mean. Under-delivery is the expensive
   side, so the bid should be MW we are confident will show up.

Every per-hour decision carries machine- and human-readable reasons so the ledger reads
like a trader's blotter: you can always answer "why didn't we bid HE17 on the 14th?".

All functions are pure (no I/O); callers supply prices/generation/forecasts/weather.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import mean, pstdev

from shadow_trader.config import ASSET_CONFIG, DART_ASSETS, TRADER_CONFIG
from shadow_trader.strategy import _build_price_indexes

logger = logging.getLogger(__name__)

LEVEL_OK = "ok"
LEVEL_ELEVATED = "elevated"
LEVEL_STANDDOWN = "standdown"

# Daylight window (hour-ending) used for solar cloud-volatility assessment
_SOLAR_HE_RANGE = range(7, 21)


def _prior_dates(target_date: str, lookback_days: int) -> list[str]:
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return [(dt - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, lookback_days + 1)]


def build_dart_index(market_prices: dict) -> dict:
    """Pre-compute hourly DART spreads: {node: {(date_str, hour): da_price - rt_price}}."""
    da_idx, rt_idx = _build_price_indexes(market_prices)
    out = {}
    for node, da_map in da_idx.items():
        rt_map = rt_idx.get(node, {})
        out[node] = {k: da - rt_map[k] for k, da in da_map.items() if k in rt_map}
    return out


def trailing_dart_edge(
    dart_node: dict,
    target_date: str,
    he: int,
    lookback_days: int,
    min_samples: int,
) -> tuple[float | None, int]:
    """Mean DART ($/MWh) for this hour-of-day over the trailing window, using only days
    strictly before target_date (information available at bid time).

    Returns (edge, n_samples); edge is None when there aren't enough samples to trust.
    """
    hour = he - 1
    samples = [
        dart_node[(d, hour)]
        for d in _prior_dates(target_date, lookback_days)
        if (d, hour) in dart_node
    ]
    if len(samples) < min_samples:
        return None, len(samples)
    return mean(samples), len(samples)


def forecast_error_stats(
    generation: dict,
    forecasts: dict,
    asset: str,
    target_date: str,
    lookback_days: int,
    min_samples: int,
) -> dict:
    """Trailing forecast-error stats by hour-of-day, using only days before target_date.

    Error convention: err = actual - forecast, so positive bias means we under-forecast.
    Returns {he: {'bias': float, 'sigma': float, 'n': int}} for hours with enough samples.
    """
    gen = generation.get(asset, {})
    fc = forecasts.get(asset, {})
    samples = defaultdict(list)
    for d in _prior_dates(target_date, lookback_days):
        for he in range(1, 25):
            hour_key = f"{d} HE{he:02d}"
            g = gen.get(hour_key)
            f = fc.get(hour_key)
            if g is None or f is None:
                continue
            actual = g["gen_mwh"] if isinstance(g, dict) else float(g)
            # Skip hours where both sides are ~zero (solar overnight) -- no information
            if actual <= 0.1 and float(f) <= 0.1:
                continue
            samples[he].append(actual - float(f))
    out = {}
    for he, errs in samples.items():
        if len(errs) < min_samples:
            continue
        out[he] = {
            "bias": mean(errs),
            "sigma": pstdev(errs) if len(errs) > 1 else 0.0,
            "n": len(errs),
        }
    return out


def assess_weather_day(weather_day: dict, tech: str, cfg: dict) -> dict:
    """Grade one operating day's weather per hour: {he: {'level': str, 'reasons': [str]}}.

    weather_day: {he: {temp_f, wind_mph, gust_mph, cloud_pct, wind_spread, cloud_spread}}
    Missing weather data produces no gates (level 'ok') -- the strategy degrades to
    forecast-only rather than refusing to trade on a data outage.
    """
    result = {he: {"level": LEVEL_OK, "reasons": []} for he in range(1, 25)}
    if not weather_day:
        return result

    def flag(he: int, level: str, reason: str):
        rec = result[he]
        rec["reasons"].append(reason)
        if level == LEVEL_STANDDOWN or rec["level"] == LEVEL_STANDDOWN:
            rec["level"] = LEVEL_STANDDOWN
        else:
            rec["level"] = LEVEL_ELEVATED

    # --- Day-level wind ramp screen (frontal passage detection) -------------------
    # Large hour-over-hour swings in hub-height wind mean a front is moving through:
    # timing errors of 1-2 hours produce enormous MW busts, and ERCOT-wide wind swings
    # move RT prices against the position at the same time.
    if tech == "wind":
        speeds = [(he, weather_day[he].get("wind_mph")) for he in sorted(weather_day)]
        speeds = [(he, w) for he, w in speeds if w is not None]
        max_ramp, ramp_he = 0.0, None
        for (he_a, w_a), (he_b, w_b) in zip(speeds, speeds[1:]):
            if he_b - he_a == 1 and abs(w_b - w_a) > max_ramp:
                max_ramp, ramp_he = abs(w_b - w_a), he_b
        if max_ramp >= cfg["wind_ramp_standdown_mph"]:
            for he in result:
                flag(he, LEVEL_STANDDOWN,
                     f"frontal passage: {max_ramp:.0f} mph/hr wind ramp near HE{ramp_he:02d}")
        elif max_ramp >= cfg["wind_ramp_elevated_mph"]:
            for he in result:
                flag(he, LEVEL_ELEVATED,
                     f"wind ramp risk: {max_ramp:.0f} mph/hr near HE{ramp_he:02d}")

    # --- Day-level solar cloud volatility screen -----------------------------------
    # Partly-cloudy, fast-moving days are the hardest irradiance forecasts. A stable
    # overcast or stable clear day is fine; a 0-100% oscillation is not.
    if tech == "solar":
        clouds = [
            weather_day[he]["cloud_pct"]
            for he in _SOLAR_HE_RANGE
            if he in weather_day and weather_day[he].get("cloud_pct") is not None
        ]
        if len(clouds) >= 6:
            vol = pstdev(clouds)
            if vol >= cfg["cloud_volatility_standdown_pct"]:
                for he in _SOLAR_HE_RANGE:
                    flag(he, LEVEL_STANDDOWN, f"cloud volatility: {vol:.0f}% stdev across daylight hours")
            elif vol >= cfg["cloud_volatility_elevated_pct"]:
                for he in _SOLAR_HE_RANGE:
                    flag(he, LEVEL_ELEVATED, f"cloud volatility: {vol:.0f}% stdev across daylight hours")

    # --- Per-hour screens -----------------------------------------------------------
    for he, wx in weather_day.items():
        if he not in result:
            continue
        temp = wx.get("temp_f")
        if temp is not None:
            # Extreme temperature at a West Texas plant is a proxy for system-wide
            # scarcity conditions: RT can print at multiples of DA, and any MW we bid
            # but fail to deliver is bought back at the worst possible price.
            if temp >= cfg["extreme_heat_f"]:
                flag(he, LEVEL_STANDDOWN, f"scarcity risk: extreme heat {temp:.0f}F")
            elif temp <= cfg["extreme_cold_f"]:
                flag(he, LEVEL_STANDDOWN, f"scarcity risk: extreme cold {temp:.0f}F")

        if tech == "wind":
            wind_mph = wx.get("wind_mph")
            gust = wx.get("gust_mph")
            if wind_mph is not None and wind_mph >= cfg["wind_cutout_mph"]:
                flag(he, LEVEL_STANDDOWN, f"cut-out risk: {wind_mph:.0f} mph hub wind")
            elif gust is not None and gust >= cfg["gust_standdown_mph"]:
                flag(he, LEVEL_STANDDOWN, f"cut-out risk: {gust:.0f} mph gusts")
            spread = wx.get("wind_spread")
            if spread is not None and spread >= cfg["wind_model_spread_mph"]:
                flag(he, LEVEL_ELEVATED, f"model disagreement: {spread:.0f} mph wind spread")

        if tech == "solar" and he in _SOLAR_HE_RANGE:
            cspread = wx.get("cloud_spread")
            if cspread is not None and cspread >= cfg["cloud_model_spread_pct"]:
                flag(he, LEVEL_ELEVATED, f"model disagreement: {cspread:.0f}% cloud spread")

    return result


def decide_day(
    asset: str,
    target_date: str,
    forecasts: dict,
    generation: dict,
    dart_index: dict,
    weather: dict,
    base_fraction: float = 1.0,
    cfg: dict | None = None,
) -> dict:
    """Build one asset-day of trader decisions: {hour_key: decision_record}.

    decision_record:
        bid_mw       final DA bid (0 = stay out)
        participate  bool
        level        ok | elevated | standdown
        reasons      list[str] (empty when participating at full size with edge)
        edge         trailing DART $/MWh for the hour (None if insufficient history)
        sigma        trailing forecast-error sigma used in the haircut (None if none)

    Uses only information available before the DA bid deadline for target_date:
    trailing windows end the day before, and weather comes from forecasts (not actuals).
    """
    cfg = cfg or TRADER_CONFIG
    asset_cfg = ASSET_CONFIG.get(asset, {})
    tech = asset_cfg.get("tech", "wind")
    node = asset_cfg.get("settlement_point")
    dart_node = dart_index.get(node, {})

    err_stats = forecast_error_stats(
        generation, forecasts, asset, target_date,
        cfg["error_lookback_days"], cfg["min_error_samples"],
    )
    weather_day = {}
    for he in range(1, 25):
        wx = weather.get(asset, {}).get(f"{target_date} HE{he:02d}")
        if wx:
            weather_day[he] = wx
    wx_grades = assess_weather_day(weather_day, tech, cfg)

    out = {}
    for he in range(1, 25):
        hour_key = f"{target_date} HE{he:02d}"
        forecast_mw = float(forecasts.get(asset, {}).get(hour_key, 0) or 0)
        grade = wx_grades[he]
        reasons = list(grade["reasons"])
        edge, n_edge = trailing_dart_edge(
            dart_node, target_date, he, cfg["dart_lookback_days"], cfg["min_dart_samples"],
        )
        stats = err_stats.get(he)
        sigma = stats["sigma"] if stats else None

        rec = {
            "bid_mw": 0.0,
            "participate": False,
            "zero_forecast": forecast_mw <= 0,
            "level": grade["level"],
            "reasons": reasons,
            "edge": round(edge, 2) if edge is not None else None,
            "sigma": round(sigma, 2) if sigma is not None else None,
        }
        out[hour_key] = rec

        if forecast_mw <= 0:
            # Nothing to sell (solar overnight, dead-calm wind hour). Not a stand-down.
            continue
        if grade["level"] == LEVEL_STANDDOWN:
            continue
        # Edge filter: a trailing DA discount to RT means selling DA has been a losing
        # trade in this hour lately -- collect RT instead. Insufficient history is
        # treated as neutral (edge requirement of <= 0 still allows participation).
        if edge is not None and edge < cfg["dart_min_edge"]:
            reasons.append(f"no DA edge: trailing DART {edge:+.2f} $/MWh")
            continue

        # Confidence-aware sizing: bias-correct, then haircut by k*sigma so the bid is
        # MW we expect to physically deliver. Bias correction is clamped so a noisy
        # trailing window can't more than halve or 1.5x the underlying forecast.
        bid_base = forecast_mw
        if stats:
            max_adj = cfg["bias_correction_clamp"] * forecast_mw
            bias_adj = max(-max_adj, min(max_adj, stats["bias"]))
            bid_base = max(0.0, forecast_mw + bias_adj - cfg["sigma_haircut_k"] * stats["sigma"])

        risk_mult = 0.5 if grade["level"] == LEVEL_ELEVATED else 1.0
        bid_mw = bid_base * base_fraction * risk_mult
        if bid_mw <= 0:
            reasons.append("sigma haircut zeroed bid (forecast confidence too low)")
            continue

        rec["bid_mw"] = round(bid_mw, 2)
        rec["participate"] = True
    return out


def build_bid_plan(
    dates: list[str],
    forecasts: dict,
    generation: dict,
    market_prices: dict,
    weather: dict,
    base_fraction: float = 1.0,
    cfg: dict | None = None,
) -> dict:
    """Full bid plan over a backtest window: {asset: {hour_key: decision_record}}.

    Walks dates in order; every per-day decision uses trailing windows that end the prior
    day, so early dates with thin history simply trade with fewer filters active.
    """
    cfg = cfg or TRADER_CONFIG
    dart_index = build_dart_index(market_prices)
    plan: dict[str, dict] = {a: {} for a in DART_ASSETS}
    for asset in DART_ASSETS:
        for d in sorted(dates):
            plan[asset].update(
                decide_day(asset, d, forecasts, generation, dart_index, weather,
                           base_fraction=base_fraction, cfg=cfg)
            )
    return plan


def summarize_plan(plan: dict) -> dict:
    """Roll a bid plan up into participation stats for reporting.

    Returns {asset: {'hours_with_forecast', 'hours_bid', 'hours_stood_down',
                     'standdown_reasons': {reason_prefix: count}}}.
    """
    out = {}
    for asset, hours in plan.items():
        considered = bid = stood = 0
        reasons_count: dict[str, int] = defaultdict(int)
        for rec in hours.values():
            if rec.get("zero_forecast"):
                continue
            considered += 1
            if rec["participate"]:
                bid += 1
            else:
                stood += 1
                for reason in rec["reasons"]:
                    # Bucket by the part before the colon so counts group naturally
                    reasons_count[reason.split(":")[0]] += 1
        out[asset] = {
            "hours_with_forecast": considered,
            "hours_bid": bid,
            "hours_stood_down": stood,
            "standdown_reasons": dict(sorted(reasons_count.items(), key=lambda kv: -kv[1])),
        }
    return out
