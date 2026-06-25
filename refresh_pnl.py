"""Refresh pnl_history.json via the INCREMENTAL Tenaska pipeline.

First run (no raw cache) does a full YTD backfill and seeds pnl_raw_cache.json.gz;
every run after that fetches only the trailing few days and re-aggregates. Fast
enough to wire into a daily scheduled task (Windows Task Scheduler / cron) that
commits + pushes pnl_history.json so the Render dashboard stays current.

    python refresh_pnl.py            # incremental (or full backfill if no cache)
    python refresh_pnl.py --full     # force a full YTD re-pull
"""
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import app

force_full = "--full" in sys.argv

print(f"Refreshing PnL ({'full backfill' if force_full else 'incremental'})...")
aggregated = app.compute_pnl_incremental(force_full=force_full)
if not aggregated:
    raise SystemExit("ABORT: fetch returned no records; pnl_history.json left unchanged.")

# Load existing cache so non-aggregated fields are preserved, then overwrite the
# aggregated ones exactly like the app's own refresh does.
cached = app.load_pnl_data()
if cached:
    app.pnl_data.update(cached)
app.pnl_data["daily_pnl"] = aggregated["daily"]
app.pnl_data["monthly_pnl"] = aggregated["monthly"]
app.pnl_data["annual_pnl"] = aggregated["annual"]
app.pnl_data["total_pnl"] = aggregated["total_pnl"]
app.pnl_data["total_volume"] = aggregated["total_volume"]
app.pnl_data["record_count"] = aggregated["record_count"]
app.pnl_data["assets"] = aggregated.get("assets", {})
app.pnl_data["worst_basis_intervals"] = aggregated.get("worst_basis_intervals", [])
app.pnl_data["last_tenaska_update"] = datetime.now(ZoneInfo("America/New_York")).isoformat()
app.save_pnl_data(app.pnl_data)

print("\nSaved pnl_history.json. Per-asset coverage:")
for k, v in aggregated.get("assets", {}).items():
    dp = v.get("daily_pnl", {})
    ds = sorted(dp.keys())
    rng = f"{ds[0]} -> {ds[-1]} ({len(ds)}d)" if ds else "none"
    print(f"  {k:10} recs={v.get('record_count')} vol={round(v.get('total_volume') or 0, 1)} daily: {rng}")
print("last_tenaska_update:", app.pnl_data["last_tenaska_update"])
