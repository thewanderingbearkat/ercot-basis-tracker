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


# Owner->seller pairs that share no distinctive token but ARE legitimate (corporate
# aliases: subsidiary, rename, or fund platform). Anything else with zero distinctive
# overlap is treated as a false match and cleared entirely -- e.g. 'E.ON Climate &
# Renewables -> TPG Rise Climate' matched on the generic word 'Climate' (2026-07,
# Settlers Trail incident) and credited TPG's deals to 25 RWE-legacy plants.
ALIAS_WHITELIST = [
    ("SHELL", "SAVION"), ("PINE GATE", "MORNINGSKY"),
    ("BROOKFIELD", "SCOUT"), ("DERIVA", "SCOUT"),
]

# 2026-07 public-information audit of all matched pairs (4 web-verification agents,
# sources in session notes): pairs verified FALSE — pure name coincidences. Cleared
# outright and kept here as a permanent blocklist.
FALSE_PAIRS = [
    "E.ON Climate & Renewables North America Inc. -> TPG Rise Climate",
    "Tokyo Gas America Ltd. -> Tokyo Electric Power Company Holdings (TEPCO)",
    "Tri-State Generation and Transmission Association Inc. -> Tri Global Energy",
    "Atlantic Wind, LLC -> Global Atlantic Financial Group",
    "Columbia Universal Corp. -> New Columbia Solar",
    "E&P Financial Group Limited -> Global Atlantic Financial Group",
    "House Mountain LLC -> New Mountain Capital",
    "Pacific Wind Development LLC -> Pacific Gas and Electric Company (PG&E)",
    "Rocky Mountain Power, Inc. -> New Mountain Capital",
    "Star Point Wind Project LLC -> East Point Energy",
    "Sun Life Financial Inc. -> New York Life",
    "GE Energy Financial Services -> Global Atlantic Financial Group",
    "Qualitas Equity Partners -> New Energy Equity",
]

# Pairs verified as real-but-SIBLING relationships (same group, different platform):
# attributing the entity's deals to these owners' plants only holds at group level.
# Rewriting the owner side makes assemble_v3's direct-vs-parent logic score them at
# parent strength automatically (matched name no longer appears in the owners list).
SIBLING_REWRITES = {
    "Deriva Energy, LLC -> Scout Clean Energy":
        "Brookfield group (Deriva sibling) -> Scout Clean Energy",
    "Pine Gate Renewables, LLC -> MorningSky Power":
        "Fundamental Advisors successor (Pine Gate pipeline) -> MorningSky Power",
    "JERA Americas Inc. -> JERA Nex":
        "JERA group (sibling) -> JERA Nex",
    "Mitsubishi Heavy Industries America, Inc. -> Mitsubishi Power Americas":
        "MHI group (sibling) -> Mitsubishi Power Americas",
    "RWE Renewables Europe & Australia GmbH -> RWE Clean Energy":
        "RWE group (sibling region) -> RWE Clean Energy",
}
GENERIC_EXTRA = {"CLIMATE", "CLEAN", "GREEN", "SUSTAINABLE", "NEW", "RISE", "SOLAR",
                 "WIND", "STORAGE", "DEVELOPMENT", "MANAGEMENT", "INVESTMENTS",
                 "INVESTMENT", "GLOBAL", "NORTH", "AMERICA", "AMERICAN"}


def _whitelisted(owner, ent):
    ou, eu = owner.upper(), ent.upper()
    return any(a in ou or a in eu for a, b in ALIAS_WHITELIST
               if (a in ou and b in eu) or (b in ou and a in eu) or (a in eu and b in ou))


def main(workdir):
    t = pd.read_csv(TXN, encoding="utf-8-sig", low_memory=False)
    live = t[(t["Live Deal?"] == "Yes")
             & t["Type | Sub Type"].fillna("").str.contains("Project M&A Process")]
    ren = live[live.Sectors.fillna("").str.contains("Wind|Solar|Storage|Renewable", case=False)]
    ren_sellers = {s.strip() for ss in ren.Seller.dropna() for s in ss.split("|") if s.strip()}

    path = f"{workdir}\\seller_signals.csv"
    s = pd.read_csv(path, encoding="utf-8-sig")

    # Gate 0: audited blocklist — verified-false pairs cleared; sibling pairs
    # rewritten so they score at parent strength (live flags don't cross siblings).
    matched0 = s.seller_owner_matched.fillna("")
    blocked = matched0.isin(FALSE_PAIRS)
    for col in ("seller_owner_matched", "seller_last_sold", "seller_deal_count",
                "seller_op_deal_count", "seller_live_process", "seller_distressed"):
        s.loc[blocked, col] = pd.NA
    n_sib = 0
    for old, new in SIBLING_REWRITES.items():
        m = s.seller_owner_matched.fillna("") == old
        s.loc[m, "seller_owner_matched"] = new
        s.loc[m, "seller_live_process"] = 0
        n_sib += int(m.sum())
    print(f"audited blocklist cleared: {int(blocked.sum())} plants; "
          f"sibling pairs downgraded to parent strength: {n_sib} plants")

    # Gate 1: kill matches whose owner and seller entity share NO distinctive token
    # (generic-token accidents), unless whitelisted as a known corporate alias.
    strict_gen = GEN | GENERIC_EXTRA
    def strict_toks(x):
        return {w for w in re.sub(r"[^A-Z0-9 ]", " ", str(x).upper()).split()
                if len(w) > 2 and w not in strict_gen}
    matched = s.seller_owner_matched.fillna("")
    is_false = matched.apply(lambda p: "->" in p and not _whitelisted(*[
        x.strip() for x in p.split("->", 1)]) and not (
        strict_toks(p.split("->", 1)[0]) & strict_toks(p.split("->", 1)[1])))
    for col in ("seller_owner_matched", "seller_last_sold", "seller_deal_count",
                "seller_op_deal_count", "seller_live_process", "seller_distressed"):
        s.loc[is_false, col] = pd.NA
    print(f"generic-token false matches cleared: {int(is_false.sum())} plants")

    # Gate 2: live-process flag only survives if the entity is on a RENEWABLES live row.
    flagged = s[s.seller_live_process == 1].copy()
    flagged["entity"] = flagged.seller_owner_matched.fillna("").str.split("->").str[-1].str.strip()
    keep = flagged.entity.apply(lambda e: any(match(e, rs) for rs in ren_sellers))
    s.loc[flagged[~keep].index, "seller_live_process"] = 0
    s.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"live flags: {len(flagged)} -> {int(keep.sum())} "
          f"(cleared {len(flagged) - int(keep.sum())}: non-renewable or stale)")


if __name__ == "__main__":
    main(sys.argv[1])
