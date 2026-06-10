"""Shadow-bid ledger: one entry per (operating_date, asset), enriched in three stages.

Lifecycle:
    1. BID      -- created by generate_bids: forecast + bid quantity per hour
    2. AWARDED  -- enriched by record_awards: DA clearing price + theoretical revenue per hour
    3. SETTLED  -- enriched by settle_day: actual gen + RT price + final uplift

Stored as a single JSON file (LEDGER_FILE). Operations are read-modify-write of the whole
file. That's fine for this scale (one row per asset per day -> a few thousand rows over a
year) and keeps the on-disk format diff-friendly so it's easy to audit by hand.
"""
import json
import logging
import os
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from shadow_trader.config import DATA_DIR

logger = logging.getLogger(__name__)

LEDGER_FILE = os.path.join(DATA_DIR, "bid_ledger.json")

STATUS_BID = "BID"
STATUS_AWARDED = "AWARDED"
STATUS_SETTLED = "SETTLED"

_CST = ZoneInfo("America/Chicago")


def _now_iso() -> str:
    return datetime.now(_CST).isoformat()


def entry_id(operating_date: str, asset: str) -> str:
    return f"{operating_date}::{asset}"


def load() -> dict:
    if not os.path.exists(LEDGER_FILE):
        return {"entries": {}}
    with open(LEDGER_FILE) as f:
        data = json.load(f)
    if "entries" not in data:
        data["entries"] = {}
    return data


def save(ledger: dict) -> None:
    os.makedirs(os.path.dirname(LEDGER_FILE), exist_ok=True)
    with open(LEDGER_FILE, "w") as f:
        json.dump(ledger, f, indent=2, sort_keys=True, default=str)


def upsert_bid(
    operating_date: str,
    asset: str,
    bid_fraction: float,
    forecast_source: str,
    hourly_bids: list[dict],
    overwrite: bool = False,
    strategy: str = "naive",
) -> dict:
    """Create or replace a BID entry. Returns the saved entry.

    hourly_bids: list of {'he': int, 'forecast_mw': float, 'da_bid_mw': float}
    Trader-strategy rows additionally carry 'level', 'reasons', and 'trailing_edge'
    so the ledger doubles as a decision blotter.
    """
    ledger = load()
    eid = entry_id(operating_date, asset)
    existing = ledger["entries"].get(eid)
    if existing and not overwrite and existing.get("status") not in (None,):
        logger.warning(
            "Ledger entry %s already exists with status=%s; pass overwrite=True to replace",
            eid, existing.get("status"),
        )
        return existing
    entry = {
        "id": eid,
        "operating_date": operating_date,
        "asset": asset,
        "status": STATUS_BID,
        "bid": {
            "generated_at": _now_iso(),
            "bid_fraction": bid_fraction,
            "forecast_source": forecast_source,
            "strategy": strategy,
            "hourly": hourly_bids,
        },
        "awards": None,
        "settlement": None,
    }
    ledger["entries"][eid] = entry
    save(ledger)
    return entry


def attach_awards(
    operating_date: str,
    asset: str,
    hourly_awards: list[dict],
    total_da_revenue: float,
) -> Optional[dict]:
    """Attach DA clearing prices and theoretical revenue to an existing BID entry.

    hourly_awards: list of {'he': int, 'da_clearing_price': float, 'da_revenue': float}
    """
    ledger = load()
    eid = entry_id(operating_date, asset)
    entry = ledger["entries"].get(eid)
    if not entry:
        logger.error("attach_awards: no ledger entry %s", eid)
        return None
    entry["awards"] = {
        "recorded_at": _now_iso(),
        "hourly": hourly_awards,
        "total_da_revenue": round(total_da_revenue, 2),
    }
    entry["status"] = STATUS_AWARDED
    save(ledger)
    return entry


def attach_settlement(
    operating_date: str,
    asset: str,
    hourly_settlement: list[dict],
    summary: dict,
) -> Optional[dict]:
    """Attach actual gen + RT settlement + final uplift summary to an existing entry.

    hourly_settlement: list of {'he': int, 'actual_gen_mw': float, 'rt_node_price': float,
                                 'rt_settlement': float, 'shadow_total': float,
                                 'rt_only_revenue': float, 'uplift': float}
    summary: aggregated dict for the day
    """
    ledger = load()
    eid = entry_id(operating_date, asset)
    entry = ledger["entries"].get(eid)
    if not entry:
        logger.error("attach_settlement: no ledger entry %s", eid)
        return None
    entry["settlement"] = {
        "settled_at": _now_iso(),
        "hourly": hourly_settlement,
        "summary": summary,
    }
    entry["status"] = STATUS_SETTLED
    save(ledger)
    return entry


def entries_by_status(status: Optional[str] = None) -> list[dict]:
    """Return entries filtered by status, sorted by (operating_date, asset)."""
    ledger = load()
    entries = list(ledger["entries"].values())
    if status:
        entries = [e for e in entries if e.get("status") == status]
    entries.sort(key=lambda e: (e["operating_date"], e["asset"]))
    return entries


def get_entry(operating_date: str, asset: str) -> Optional[dict]:
    ledger = load()
    return ledger["entries"].get(entry_id(operating_date, asset))
