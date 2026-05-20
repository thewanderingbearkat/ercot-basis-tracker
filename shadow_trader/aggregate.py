"""Roll hourly shadow-DA records into daily, monthly, and per-asset summaries."""
from collections import defaultdict


_HOURLY_SUM_FIELDS = (
    "da_revenue", "rt_settlement", "shadow_total", "rt_only_revenue", "uplift",
    "ppa_fixed_payment", "ppa_floating_payment", "net_ppa",
    "shadow_total_pnl", "rt_only_total_pnl",
)


def _zero_bucket(extra_field: str):
    bucket = {f: 0 for f in _HOURLY_SUM_FIELDS}
    bucket["gen_mwh"] = 0
    bucket["forecast_mwh"] = 0
    bucket[extra_field] = 0
    return bucket


def aggregate(records: list[dict]) -> dict:
    """Return {daily, daily_asset, monthly, total_uplift, total_da_revenue, total_rt_only_revenue}."""
    daily = defaultdict(lambda: _zero_bucket("hours"))
    daily_asset = defaultdict(lambda: defaultdict(lambda: _zero_bucket("hours")))

    for r in records:
        for target in (daily[r["date"]], daily_asset[r["date"]][r["asset"]]):
            for f in _HOURLY_SUM_FIELDS:
                target[f] += r[f]
            target["gen_mwh"] += r["actual_gen_mw"]
            target["forecast_mwh"] += r["forecast_mw"]
            target["hours"] += 1

    def _round_bucket(b):
        for k, v in b.items():
            if k != "hours" and k != "days":
                b[k] = round(v, 2)

    for b in daily.values():
        _round_bucket(b)
    for date_data in daily_asset.values():
        for b in date_data.values():
            _round_bucket(b)

    monthly = defaultdict(lambda: _zero_bucket("days"))
    for date_str, d in daily.items():
        month_key = date_str[:7]
        for k, v in d.items():
            if k in ("hours", "days"):
                continue
            monthly[month_key][k] = round(monthly[month_key][k] + v, 2)
        monthly[month_key]["days"] += 1

    sum_field = lambda f: round(sum(d[f] for d in daily.values()), 2)
    return {
        "daily": dict(daily),
        "daily_asset": {k: dict(v) for k, v in daily_asset.items()},
        "monthly": dict(monthly),
        "total_uplift": sum_field("uplift"),
        "total_da_revenue": sum_field("da_revenue"),
        "total_rt_settlement": sum_field("rt_settlement"),
        "total_market": sum_field("shadow_total"),
        "total_rt_only_revenue": sum_field("rt_only_revenue"),
        "total_ppa_fixed": sum_field("ppa_fixed_payment"),
        "total_ppa_floating": sum_field("ppa_floating_payment"),
        "total_net_ppa": sum_field("net_ppa"),
        "total_pnl_shadow": sum_field("shadow_total_pnl"),
        "total_pnl_rt_only": sum_field("rt_only_total_pnl"),
    }
