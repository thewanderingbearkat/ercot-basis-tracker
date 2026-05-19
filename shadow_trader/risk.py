"""Risk metrics for evaluating shadow-DA bid strategies.

All functions take a list of hourly records from `simulate_shadow_da` and return
scalar (or scalar-per-asset) summaries. Pure functions, no I/O.
"""
from collections import defaultdict
from statistics import mean, pstdev


def hourly_uplifts(records: list[dict]) -> list[float]:
    return [r["uplift"] for r in records]


def daily_uplifts(records: list[dict]) -> list[float]:
    by_day = defaultdict(float)
    for r in records:
        by_day[r["date"]] += r["uplift"]
    return list(by_day.values())


def running_cumulative(values: list[float]) -> list[float]:
    out = []
    total = 0.0
    for v in values:
        total += v
        out.append(total)
    return out


def max_drawdown(cumulative: list[float]) -> float:
    """Peak-to-trough drawdown over a cumulative-uplift series. Returns a non-positive number."""
    if not cumulative:
        return 0.0
    peak = cumulative[0]
    worst = 0.0
    for v in cumulative:
        peak = max(peak, v)
        worst = min(worst, v - peak)
    return worst


def percentile(values: list[float], pct: float) -> float:
    """Linear-interpolated percentile. pct in [0, 100]."""
    if not values:
        return 0.0
    s = sorted(values)
    if pct <= 0:
        return s[0]
    if pct >= 100:
        return s[-1]
    k = (len(s) - 1) * pct / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    frac = k - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def value_at_risk(values: list[float], pct: float = 5.0) -> float:
    """VaR at the given percentile (e.g. 5 = 5th-percentile worst outcome)."""
    return percentile(values, pct)


def conditional_value_at_risk(values: list[float], pct: float = 5.0) -> float:
    """CVaR: mean of the worst `pct`% of outcomes. Tail expectation."""
    if not values:
        return 0.0
    threshold = percentile(values, pct)
    tail = [v for v in values if v <= threshold]
    return mean(tail) if tail else threshold


def summarize(records: list[dict]) -> dict:
    """Return a flat dict of risk/return metrics for a list of hourly records."""
    hourly = hourly_uplifts(records)
    daily = daily_uplifts(records)
    if not hourly:
        return {
            "hours": 0, "days": 0, "total_uplift": 0,
            "mean_hourly": 0, "std_hourly": 0,
            "mean_daily": 0, "std_daily": 0,
            "worst_hour": 0, "worst_day": 0,
            "hit_rate_hourly": 0, "hit_rate_daily": 0,
            "max_drawdown_daily": 0,
            "var_5_hourly": 0, "cvar_5_hourly": 0,
            "var_5_daily": 0, "cvar_5_daily": 0,
            "sharpe_like_daily": 0,
        }

    total = sum(hourly)
    mh, mh_std = mean(hourly), pstdev(hourly) if len(hourly) > 1 else 0.0
    md, md_std = mean(daily), pstdev(daily) if len(daily) > 1 else 0.0
    hit_h = sum(1 for v in hourly if v > 0) / len(hourly)
    hit_d = sum(1 for v in daily if v > 0) / len(daily) if daily else 0
    cum_daily = running_cumulative(daily)

    return {
        "hours": len(hourly),
        "days": len(daily),
        "total_uplift": round(total, 2),
        "mean_hourly": round(mh, 2),
        "std_hourly": round(mh_std, 2),
        "mean_daily": round(md, 2),
        "std_daily": round(md_std, 2),
        "worst_hour": round(min(hourly), 2),
        "worst_day": round(min(daily) if daily else 0, 2),
        "hit_rate_hourly": round(hit_h, 3),
        "hit_rate_daily": round(hit_d, 3),
        "max_drawdown_daily": round(max_drawdown(cum_daily), 2),
        "var_5_hourly": round(value_at_risk(hourly, 5), 2),
        "cvar_5_hourly": round(conditional_value_at_risk(hourly, 5), 2),
        "var_5_daily": round(value_at_risk(daily, 5), 2),
        "cvar_5_daily": round(conditional_value_at_risk(daily, 5), 2),
        # Sharpe-like ratio on daily uplifts. Not annualized — just a unitless
        # risk-adjusted return signal for ranking bid_fraction scenarios.
        "sharpe_like_daily": round(md / md_std, 3) if md_std > 0 else 0.0,
    }
