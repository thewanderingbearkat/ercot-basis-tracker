"""Load the M&A seller-screen plant table into Snowflake for the dashboard tab.

Reads the latest scored CSV produced by the screener pipeline (assemble_v3.py),
derives the categorical facet columns the tab filters on (contract status, PTC
status, seller-signal flags, top-50 tier), and full-replaces
SKYVEST.DBO.MA_SCREEN_PLANTS. Run locally after each screener refresh:

    SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/constraint_map_rsa_key.p8 \
        python load_to_snowflake.py [path-to-scores.csv]

Facet philosophy: every derived facet keeps an explicit "Not assessed"/"Unknown"
value instead of dropping rows — the tab's job is to show data gaps, not hide them.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from constraint_map import db

TABLE = "SKYVEST.DBO.MA_SCREEN_PLANTS"
DEFAULT_POINTER = os.path.join(
    os.environ.get("TEMP", ""), "claude",
    "C--Users-TylerMartin", "be345640-8abd-4677-a763-c3972730811c",
    "scratchpad", "latest_scores_path.txt")


def contract_status(r):
    if r.get("eqr_rolloff") == 1 or r.get("eqr_label") == "MERCHANT" or r.get("ppa_status") == "No":
        return "Merchant"
    if r.get("eqr_label") == "HYBRID" or (
            r.get("ppa_status") == "Yes" and pd.notna(r.get("contracted_pct"))
            and r.get("contracted_pct") < 0.5):
        return "Hybrid / partial"
    if r.get("ppa_status") == "Yes" or r.get("eqr_label") == "CONTRACTED":
        return "Contracted"
    return "Not assessed"


def ptc_status(r):
    if r.get("tech") != "Wind":
        return "N/A (non-wind)"
    if r.get("repowered") == 1:
        return "Repowered"
    if pd.isna(r.get("cod_yr")):
        return "Unknown"
    return "PTC expired" if r["cod_yr"] <= 2016 else "PTC active"


def load(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    out = pd.DataFrame({
        "PLANT": df.plant, "TECH": df.tech, "MW": df.capacity_mw, "ISO": df.iso,
        "STATE": df.state, "COUNTY": df.county, "COD_YR": df.cod_yr,
        "LAT": df.lat, "LON": df.lon,
        "CONTRACT_STATUS": df.apply(contract_status, axis=1),
        "PPA_STATUS": df.ppa_status, "PPA_EXPIRY": df.ppa_expiry,
        "PPA_COUNTERPARTY": df.ppa_counterparty, "CONTRACT_REASON": df.contract_reason,
        "EQR_LABEL": df.eqr_label, "EQR_ROLLOFF": df.eqr_rolloff,
        "PTC_STATUS": df.apply(ptc_status, axis=1), "REPOWERED": df.repowered,
        "SELLER_LIVE": df.seller_live_process.fillna(0).astype(int),
        "SELLER_DISTRESSED": df.seller_distressed.fillna(0).astype(int),
        "SELLER_REPEAT": (df.seller_deal_count.fillna(0) >= 2).astype(int),
        "SELLER_ONCE": (df.seller_deal_count.fillna(0) == 1).astype(int),
        "FINANCIAL_OWNER": df.financial_owner.fillna(0).astype(int),
        "FUND_LIFE": ((df.financial_owner == 1)
                      & (df.oldest_current_stake_yr <= 2018)).astype(int),
        "ASSET_TRADE_YEAR": df.asset_trade_year, "SELLER_REASON": df.seller_reason,
        "CF_2023": df.cf_2023, "CF_2024": df.cf_2024, "CF_2324": df.bf_cf_2324,
        "COHORT_P50": df.bf_cohort_p50,
        "CF_RATIO": (df.bf_cf_2324 / df.bf_cohort_p50).round(3),
        "CF_COLLAPSE": df.cf_collapse,
        "CONG_SCORE": df.cong_score, "CONG_DA_25": df.cong_da_25,
        "CONG_TREND": df.cong_trend, "NEGP_DA_25": df.negp_da_25,
        "DC_SCORE": df.dc_score, "DC_MW_50": df.dc_mw_50,
        "TAGS": df.opportunity_tags.fillna(""),
        "TOP50_STATUS": np.select(
            [df.top50_stressed == 1, df.top50_parent == 1],
            ["Top-50 (capital-stressed)", "Top-50"], default="Independent"),
        "IN_FOOTPRINT": df.in_footprint,
        "N_OWNERS": df.n_owners, "TE_OWNER": df.bf_te_owner,
        "FLIP_EVENT": (df.flip_event.fillna(0).astype(int) if "flip_event" in df
                       else pd.Series(0, index=df.index)),
        "OEM_ORPHAN": df.oem_orphan, "OEM_MFRS": df.oem_mfrs,
        "OWNERS": df.owners, "ULT_PARENTS": df.ult_parents,
        "S_CONTRACT": df.score_contract, "S_FLIP": df.score_flip,
        "S_VINTAGE": df.score_vintage, "S_SELLER": df.score_seller,
        "S_CF": df.score_cf, "S_TAGS": df.score_tags,
        "COMPOSITE": df.composite, "RANK": df["rank"], "FILTER_PASS": df.filter_pass,
    })
    return out.replace({np.nan: None})


def push(out: pd.DataFrame) -> None:
    cols = list(out.columns)
    typemap = {"PLANT": "VARCHAR", "TECH": "VARCHAR", "ISO": "VARCHAR", "STATE": "VARCHAR",
               "COUNTY": "VARCHAR", "CONTRACT_STATUS": "VARCHAR", "PPA_STATUS": "VARCHAR",
               "PPA_EXPIRY": "DATE", "PPA_COUNTERPARTY": "VARCHAR", "CONTRACT_REASON": "VARCHAR",
               "EQR_LABEL": "VARCHAR", "PTC_STATUS": "VARCHAR", "SELLER_REASON": "VARCHAR",
               "TAGS": "VARCHAR", "TOP50_STATUS": "VARCHAR", "TE_OWNER": "VARCHAR",
               "OEM_MFRS": "VARCHAR", "OWNERS": "VARCHAR", "ULT_PARENTS": "VARCHAR"}
    ddl = ", ".join(f'"{c}" {typemap.get(c, "FLOAT")}' for c in cols)
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE OR REPLACE TABLE {TABLE} ({ddl}, LOADED_AT TIMESTAMP_NTZ "
                    f"DEFAULT CURRENT_TIMESTAMP())")
        ph = ", ".join(["%s"] * len(cols))
        rows = [tuple(r) for r in out.itertuples(index=False, name=None)]
        for i in range(0, len(rows), 500):
            cur.executemany(
                f'INSERT INTO {TABLE} ({", ".join(chr(34)+c+chr(34) for c in cols)}) '
                f"VALUES ({ph})", rows[i:i + 500])
        n = cur.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        print(f"loaded {n} rows into {TABLE}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        with open(DEFAULT_POINTER) as fh:
            path = fh.read().strip()
    print("source:", path)
    push(load(path))
