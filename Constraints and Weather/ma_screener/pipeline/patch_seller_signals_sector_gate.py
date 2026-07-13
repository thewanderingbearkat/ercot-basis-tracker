"""Sector-gate the live-sale-process flag in seller_signals.csv.

Why this exists: the component that builds seller_signals.csv flags a plant when
any of its owners is a Seller on a transaction.csv row tagged 'Project M&A
Process' with Live Deal = Yes. Two failure modes were found (2026-07, Kingfisher
Wind): (1) the live deal can be in a different sector entirely — BlackRock's only
live process was a GAS sale (Moxie Freedom/Guernsey -> Talen) yet every
BlackRock-owned wind farm inherited "live process"; (2) some flagged entities
were not on ANY live row (loose matching upstream). This patch keeps the flag
only when the matched seller entity appears on a live row whose Sectors include
wind/solar/storage/renewable. Run BETWEEN the component build and assemble_v3.py:

    python patch_seller_signals_sector_gate.py <workdir-with-seller_signals.csv>
"""
import re
import sys

import pandas as pd

TXN = r"C:\Users\TylerMartin\Downloads\transaction.csv"
GEN = {"LLC", "INC", "LP", "GROUP", "HOLDINGS", "ENERGY", "POWER", "RENEWABLES",
       "RENEWABLE", "CAPITAL", "PARTNERS", "INFRA", "INFRASTRUCTURE", "THE", "CO", "CORP"}


def norm(s):
    return {w for w in re.sub(r"[^A-Z0-9 ]", " ", str(s).upper()).split()
            if len(w) > 2 and w not in GEN}


def match(a, b):
    ta, tb = norm(a), norm(b)
    return bool(ta and tb and (ta <= tb or tb <= ta
                or (ta & tb and len(ta & tb) / min(len(ta), len(tb)) >= 0.6)))


def main(workdir):
    t = pd.read_csv(TXN, encoding="utf-8-sig", low_memory=False)
    live = t[(t["Live Deal?"] == "Yes")
             & t["Type | Sub Type"].fillna("").str.contains("Project M&A Process")]
    ren = live[live.Sectors.fillna("").str.contains("Wind|Solar|Storage|Renewable", case=False)]
    ren_sellers = {s.strip() for ss in ren.Seller.dropna() for s in ss.split("|") if s.strip()}

    path = f"{workdir}\\seller_signals.csv"
    s = pd.read_csv(path, encoding="utf-8-sig")
    flagged = s[s.seller_live_process == 1].copy()
    flagged["entity"] = flagged.seller_owner_matched.fillna("").str.split("->").str[-1].str.strip()
    keep = flagged.entity.apply(lambda e: any(match(e, rs) for rs in ren_sellers))
    s.loc[flagged[~keep].index, "seller_live_process"] = 0
    s.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"live flags: {len(flagged)} -> {int(keep.sum())} "
          f"(cleared {len(flagged) - int(keep.sum())}: non-renewable or stale)")


if __name__ == "__main__":
    main(sys.argv[1])
