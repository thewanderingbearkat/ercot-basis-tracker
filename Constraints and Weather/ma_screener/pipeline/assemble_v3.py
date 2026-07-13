"""Assemble Seller Screen v3: merge component tables onto the S&P plant universe and score.

Pillars (0-100, weights): contract posture .25 | busted-flip .20 | vintage/PTC .20 |
willing-seller .20 | CF vs P50 .15. Filters (flags, not row drops): >=50MW (universe cut),
in_footprint, top50_parent.
"""
import os, re
import numpy as np
import pandas as pd

DESK = r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop"
SCRATCH = os.environ.get("MA_SCREEN_WORKDIR", "C:/ma_screen_work")  # workdir with component CSVs
OUT = os.path.join(DESK, "Seller_Screen_v3_Scores.csv")
ASOF = pd.Timestamp("2026-07-09")

# ---------- universe ----------
asset = pd.read_excel(os.path.join(DESK, "Asset Screen_7.1.2026.xlsx"), header=2)
stakes = asset.groupby("POWER_PLANT").agg(
    n_owners=("OWNER", "nunique"),
    owners=("OWNER", lambda s: "; ".join(sorted(set(s.dropna())))),
    ult_parents=("ULT_PARENT", lambda s: "; ".join(sorted(set(s.dropna())))),
    max_stake_pct=("OPER_OWN", "max"),
)
p = asset.drop_duplicates("POWER_PLANT").merge(stakes, on="POWER_PLANT")
p = p[p.OPER_CAPACITY_PLANT >= 50].copy()
p["tech"] = p.TECH_TYPE.map({"Wind Turbine": "Wind", "Solar": "Solar", "Battery": "Battery"})
p["cod_yr"] = pd.to_numeric(p.YR_1ST_UNIT_IN_SVC, errors="coerce")
FOOT = ("ERCOT", "CAISO", "MISO", "SPP", "PJM")
p["in_footprint"] = p.ISO_NAME.fillna("").apply(lambda s: any(f in s for f in FOOT)).astype(int)

# ---------- repower (unit file) ----------
unit = pd.read_excel(os.path.join(DESK, "Unit Screen_7.1.2026.xlsx.xls"), header=2)
u = unit.drop_duplicates(["POWER_PLANT", "UNIT_CODE"])
uy = u.groupby("POWER_PLANT")["YR_UNIT_IN_SVC"].agg(unit_min="min", unit_max="max")
p = p.merge(uy, on="POWER_PLANT", how="left")
p["repowered"] = (
    ((p.unit_min - p.cod_yr) >= 5)
    | p.POWER_PLANT.str.contains(r"\(Repower", case=False, na=False)
    | ((p.unit_max - p.unit_min) >= 8)
).astype(int)
p["ptc_expired"] = ((p.tech == "Wind") & (p.cod_yr <= 2016)).astype(int)

# orphaned turbine platforms: dead/exited OEMs = parts scarcity, O&M cost spiral
ORPHAN_OEM = ["SUZLON", "CLIPPER", "SENVION", "REPOWER", "MITSUBISHI", "DEWIND",
              "NEG MICON", "MICON", "BONUS", "ZOND", "KENETECH", "FUHRLAND", "ALSTOM",
              "ACCIONA WINDPOWER", "NORDTANK"]
mfr = unit.groupby("POWER_PLANT")["EQUIPMENT_MANUFACTURER"].apply(
    lambda s: " ; ".join(sorted({t.strip().upper() for v in s.dropna()
                                 for t in str(v).split(";") if t.strip()})))
p["oem_mfrs"] = p.POWER_PLANT.map(mfr).fillna("")
p["oem_orphan"] = p.oem_mfrs.apply(lambda s: int(any(k in s for k in ORPHAN_OEM)))

# CF trajectory from ownership-records export (validated vs raw EIA-923: 29/31 confirmed)
own_cf = pd.read_excel(r"C:\Users\TylerMartin\Downloads\SkyVest_Ownership_Records_AllUS_ISO.xlsx",
                       sheet_name="Ownership Records").drop_duplicates("Plant")
own_cf = own_cf.rename(columns={"Plant": "POWER_PLANT", "CF 2023 (%)": "cf_2023",
                                "CF 2024 (%)": "cf_2024"})[["POWER_PLANT", "cf_2023", "cf_2024"]]
p = p.merge(own_cf, on="POWER_PLANT", how="left")

# ---------- top-50 parent match ----------
GENERIC = {"LLC", "INC", "LP", "SA", "SE", "PLC", "CORP", "CORPORATION", "COMPANY", "CO",
           "GROUP", "HOLDINGS", "HOLDING", "ENERGY", "ENERGIES", "POWER", "RENEWABLES",
           "RENEWABLE", "RESOURCES", "NORTH", "AMERICA", "AMERICAN", "US", "USA", "THE",
           "PARTNERS", "MANAGEMENT", "GLOBAL", "INTERNATIONAL", "NA", "SOLUTIONS", "NEW"}
def toks(s):
    return {t for t in re.sub(r"[^A-Z0-9 ]", " ", str(s).upper()).split() if len(t) > 1 and t not in GENERIC}

t50 = pd.read_excel(os.path.join(DESK, "Top 50 Asset Owners_7.1.2026.xlsx"),
                    sheet_name="Top 50 Owners", header=4)
t50_names = list(t50["Owner"].dropna()) + list(t50["Ultimate Parent"].dropna())
ALIASES = ["NEXTERA", "FLORIDA POWER", "BERKSHIRE", "MIDAMERICAN", "RWE", "ENEL", "IBERDROLA",
           "AVANGRID", "BROOKFIELD", "DERIVA", "EDP", "EDF", "AES", "ENGIE", "CLEARWAY",
           "XPLR", "INVENERGY", "APEX CLEAN", "TOTALENERGIES", "DESRI", "HECATE", "DUKE",
           "ORSTED", "ØRSTED", "PATTERN", "SOUTHERN", "DOMINION", "XCEL", "ALLIANT",
           "WEC ", "DTE ", "CONSOLIDATED EDISON", "ALGONQUIN", "LIBERTY UTILITIES",
           "LIGHTSOURCE", "SCOUT CLEAN", "TERRA-GEN", "CYPRESS CREEK", "SILICON RANCH",
           "PINE GATE", "NATIONAL GRID", "CMS ", "ENBRIDGE", "GEENEX"]
t50_toksets = [toks(n) for n in t50_names if toks(n)]
def is_top50(parents_str):
    up = str(parents_str).upper()
    if any(a in up for a in ALIASES):
        return 1
    for part in re.split(r";", str(parents_str)):
        pt = toks(part)
        if not pt:
            continue
        for tt in t50_toksets:
            inter = pt & tt
            if inter and (pt <= tt or tt <= pt or len(inter) / min(len(pt), len(tt)) >= 0.8):
                return 1
    return 0
p["top50_parent"] = (p.ult_parents + "; " + p.owners).apply(is_top50)

# capital-stressed public vehicles: technically top-50 but motivated sellers — carved back in
STRESSED = ["XPLR", "CLEARWAY", "ATLANTICA", "ALGONQUIN", "TRANSALTA", "INNERGEX",
            "NORTHLAND POWER"]
p["top50_stressed"] = ((p.top50_parent == 1) & (p.ult_parents + "; " + p.owners)
                       .str.upper().apply(lambda s: any(k in s for k in STRESSED))).astype(int)

FIN = ["MANULIFE", "JOHN HANCOCK", "MUFG", "GOLDMAN", "JPMORGAN", "MORGAN STANLEY",
       "TORONTO-DOMINION", "TD BANK", "SUN LIFE", "SOFTBANK", "BLACKROCK", "KKR", "ARES",
       "AXA", "OMERS", "CPPIB", "CANADA PENSION", "APG", "AUSTRALIAN RETIREMENT", "CALPERS",
       "AIMCO", "MACQUARIE", "AXIUM", "HARBERT", "GLOBAL INFRASTRUCTURE", "CDPQ",
       "CAISSE", "PSP INVEST", "ALLIANZ", "PRUDENTIAL", "METLIFE", "NUVEEN", "GREENCOAT",
       "FENGATE", "CAPITAL DYNAMICS", "PENSION", "INSURANCE", "TEACHERS", "MITSUI",
       "SUMITOMO", "MARUBENI", "ITOCHU", "GLOBAL ATLANTIC", "UBS", "DIF ", "ARDIAN",
       "AXIUM", "IFM ", "STONEPEAK", "ECP", "ENERGY CAPITAL PARTNERS"]
p["financial_owner"] = (p.ult_parents + "; " + p.owners).str.upper().apply(
    lambda s: int(any(f in s for f in FIN)))

# ---------- components ----------
def load(name):
    f = os.path.join(SCRATCH, name)
    if os.path.exists(f):
        df = pd.read_csv(f, encoding="utf-8-sig")
        df = df.drop_duplicates("POWER_PLANT")
        print(f"{name}: {len(df)} rows")
        return df
    print(f"{name}: MISSING")
    return None

eqr = load("eqr_plant_labels.csv")
sell = load("seller_signals.csv")
dc = load("dc_proximity.csv")
bf = load("bustedflip_port.csv")
cong = load("congestion_overlay.csv")
flipx = load("flip_precision.csv")
negp = load("negprice_overlay.csv")
recon = load("ercot_revealed_econ.csv")
for c in (eqr, sell, dc, bf, cong, flipx, negp, recon):
    if c is not None:
        overlap = [col for col in c.columns if col != "POWER_PLANT" and col in p.columns]
        p = p.merge(c.drop(columns=overlap), on="POWER_PLANT", how="left")

# ---------- pillar: contract posture ----------
p["ppa_status"] = p.ACTIVE_POWER_PURCH_AGR.map({"Yes": "Yes", "No": "No"}).fillna("NotAssessed")
p["contracted_pct"] = p.TOTAL_CURRENT_CONTRACTED_CAPACITY / p.OPER_CAPACITY_PLANT
exp = pd.to_datetime(p.LARGEST_PPA_CONTRACTED_EXPIRATION_DATE, errors="coerce")
p["ppa_expiry"] = exp.dt.date

def contract_row(r):
    """Contract-cliff pillar: contract STATE (merchant vs PPA) is a characteristic,
    not a quality rating — it stays a facet. Only revenue EVENTS score: roll-offs,
    expiry cliffs, and paper-vs-reality conflicts. Everything stable is neutral 40."""
    lab = r.get("eqr_label")
    beh = r.get("behavior_label")
    end = pd.to_datetime(r.get("eqr_bilat_max_end"), errors="coerce")
    dark = pd.notna(end) and end < ASOF - pd.DateOffset(months=12)
    e = pd.to_datetime(r.ppa_expiry) if pd.notna(r.ppa_expiry) else None
    score, why = 40, None
    if lab == "MERCHANT" and r.get("eqr_rolloff") == 1:
        score, why = 100, "EQR: bilateral sales ended, now selling to ISO (PPA roll-off)"
    elif lab == "CONTRACTED" and dark and r.ppa_status != "Yes":
        score, why = 90, (f"EQR: bilateral sales ended {end.date()}, no successor visible "
                          "(roll-off, gone dark)")
    elif r.ppa_status == "Yes" and e is not None and e <= ASOF + pd.DateOffset(years=3):
        score, why = 85, f"PPA expiring {e.date()} — re-contract / exit decision point"
    elif r.ppa_status == "Yes" and e is not None and e <= ASOF + pd.DateOffset(years=5):
        score, why = 60, f"PPA expiring {e.date()}"
    elif lab == "CONTRACTED" and dark and r.ppa_status == "Yes":
        score, why = 55, (f"VERIFY: EQR bilateral sales ended {end.date()} but S&P shows active "
                          "PPA (stale record or mismatched EQR entity)")
    elif r.ppa_status == "Yes" and lab == "MERCHANT":
        score, why = 55, ("VERIFY: S&P shows a PPA but EQR shows >=80% of MWh sold to the ISO "
                          "(hub-settled hedge, buyer resale, or stale record)")
    elif r.ppa_status == "Yes" and beh == "merchant-behaving":
        score, why = 55, ("VERIFY: S&P shows a PPA but dispatch is merchant-exposed at the node "
                          "(buyer curtailment rights, financial hedge, or stale record)")
    elif r.ppa_status == "No" and (lab == "CONTRACTED" or beh == "must-take"):
        score, why = 55, ("VERIFY: S&P says no PPA but bilateral sales / must-take dispatch "
                          "observed (hidden offtake or PTC)")
    elif r.ppa_status == "Yes" and pd.notna(r.contracted_pct) and r.contracted_pct < 0.5:
        score, why = 50, f"only {r.contracted_pct:.0%} of capacity contracted (partial merchant exposure)"
    if why is None:  # stable states — described, not scored
        if r.ppa_status == "No" or lab == "MERCHANT" or beh == "merchant-behaving":
            why = "merchant, stable (characteristic — not scored; filter by contract status instead)"
        elif r.ppa_status == "Yes" or lab in ("CONTRACTED", "HYBRID") or beh in ("must-take", "ptc-economics"):
            why = "contracted, stable (characteristic — not scored)"
        else:
            why = "not assessed — no revenue event visible"
        if isinstance(beh, str) and beh not in ("no-sced-match", "insufficient-data", "n/a-battery"):
            why += f"; SCED: {beh} (floor {r.get('shutdown_est')})"
    return pd.Series({"score_contract": score, "contract_reason": why})

p = pd.concat([p, p.apply(contract_row, axis=1)], axis=1)

# explicit state flags for tags/audit (state is a characteristic; tags may still USE it,
# with user-editable points, but the pillar above no longer rates it)
p["merchant_exposed"] = ((p.ppa_status == "No") | (p.eqr_label == "MERCHANT")
                         | (p.eqr_rolloff == 1)
                         | (p.behavior_label == "merchant-behaving")).astype(int)
p["ppa_exp_3yr"] = ((p.ppa_status == "Yes")
                    & (pd.to_datetime(p.ppa_expiry, errors="coerce")
                       <= ASOF + pd.DateOffset(years=3))).astype(int)

# ---------- pillar: vintage / PTC ----------
def vint(r):
    y = r.cod_yr
    if r.tech == "Wind":
        if r.repowered:
            return 15
        if pd.notna(y) and y <= 2016:
            return 100
        if pd.notna(y) and y <= 2018:
            return 50
        return 10
    if r.tech == "Solar":
        if pd.isna(y):
            return 10
        return 70 if y <= 2016 else 55 if y <= 2019 else 45 if y <= 2021 else 10
    return 10
p["score_vintage"] = p.apply(vint, axis=1)

# ---------- pillar: willing seller ----------
def seller(r):
    score, why = 20, ""
    m = r.get("seller_owner_matched")
    # direct = the matched entity is a plant owner; parent-only matches (signal inherited
    # through a mega-parent, e.g. every Brookfield plant "inheriting" Scout's sale process)
    # carry the signal at reduced strength
    direct = isinstance(m, str) and m.split("->")[0].strip() in str(r.owners)
    lvl = "owner" if direct else "parent"
    if r.get("seller_live_process") == 1:
        score, why = (100 if direct else 70), f"{lvl} in live sale process ({m})"
    elif r.get("seller_distressed") == 1:
        score, why = (95 if direct else 80), f"distressed {lvl} ({m})"
    elif pd.notna(r.get("seller_deal_count")) and r.seller_deal_count >= 2:
        score, why = (80 if direct else 50), f"repeat seller via {lvl} ({m}: {int(r.seller_deal_count)} deals)"
    elif pd.notna(r.get("seller_deal_count")) and r.seller_deal_count == 1:
        score, why = (60 if direct else 40), f"sold once via {lvl} ({m})"
    if score < 55 and r.financial_owner:
        score = 55
        why = (why + "; " if why else "") + "financial owner/parent"
    if (r.financial_owner and pd.notna(r.get("oldest_current_stake_yr"))
            and r.oldest_current_stake_yr <= 2018 and score < 65):
        score = 65
        why = (why + "; " if why else "") + (
            f"financial owner in since {int(r.oldest_current_stake_yr)} (fund past typical term)")
    if r.get("co_owner_recent_exit") == 1:
        score = max(score, 65)
        why = (why + "; " if why else "") + "a co-owner exited since 2024 (partner already left)"
    ty = r.get("asset_trade_year")
    if pd.notna(ty):
        if ty >= 2024:
            score = min(score, 30)
            why = (why + "; " if why else "") + f"asset traded {int(ty)} (recently acquired)"
        else:
            score = max(score, 65)
            why = (why + "; " if why else "") + f"asset traded {int(ty)} (fund-recycle candidate)"
    return pd.Series({"score_seller": score, "seller_reason": why})
p = pd.concat([p, p.apply(seller, axis=1)], axis=1)

# ---------- pillar: busted-flip (v2 port w/ fallback) ----------
INST = FIN
def coown_fallback(r):
    n = r.n_owners
    if n >= 4:
        return 100
    if n == 3:
        return 85
    if n == 2:
        return 65
    up = str(r.ult_parents).upper() + "; " + str(r.owners).upper()
    if r.top50_parent:
        return 5
    if any(f in up for f in INST):
        return 45
    return 15
p["score_flip"] = p.get("bf_coown_score", pd.Series(index=p.index, dtype=float))
p["flip_source"] = np.where(p.score_flip.notna(), "v2", "fallback")
p.loc[p.score_flip.isna(), "score_flip"] = p[p.score_flip.isna()].apply(coown_fallback, axis=1)

# ownership-event precision (stake-level fingerprints beat name-count heuristics)
p["flip_reason"] = ""
if "flip_event" in p.columns:
    m = p.flip_event == 1
    p.loc[m, "score_flip"] = np.maximum(p.loc[m, "score_flip"], 80)
    p.loc[m, "flip_reason"] = ("co-owners entered same date " + p.loc[m, "flip_event_date"].astype(str)
                               + " (single financing event)")
if "sponsor_small_stake" in p.columns:
    m = p.sponsor_small_stake == 1
    p.loc[m, "score_flip"] = np.maximum(p.loc[m, "score_flip"], 90)
    p.loc[m, "flip_reason"] = (p.loc[m, "flip_reason"].where(p.loc[m, "flip_reason"] == "",
                               p.loc[m, "flip_reason"] + "; ")
                               + "sponsor " + p.loc[m, "sponsor_name"].astype(str).str.slice(0, 40)
                               + " holds <=10% beside " + p.loc[m, "te_candidate_name"].astype(str).str.slice(0, 40)
                               + " >=60% (classic flip structure)")

# ---------- pillar: CF vs P50 ----------
p["score_cf"] = p.get("bf_cf_score", pd.Series(index=p.index, dtype=float)).fillna(40)

# ---------- CF collapse (guardrails: needs full 2023 baseline, not repowered) ----------
p["cf_collapse"] = ((p.cf_2023 > 5) & p.cf_2024.notna() & (p.cf_2024 < 0.75 * p.cf_2023)
                    & (p.cod_yr <= 2022) & (p.repowered == 0)).astype(int)

# ---------- opportunity tags as explicit booleans (mirrored 1:1 by Tag Audit formulas) ----------
TAG_POINTS = {  # canonical order — Read Me tag-points block and Tag Audit flag columns follow it
    "capitulation": 40, "cf-collapse": 35, "mechanical-fix": 25, "curtailment-play": 25,
    "neg-price-bleed": 30, "yieldco-stress": 30, "fund-life-exit": 25, "oem-orphan": 20,
    "busted-flip": 25, "ptc-merchant": 30}
p["tag_capitulation"] = (((p.merchant_exposed == 1) | (p.ppa_exp_3yr == 1))
                         & p.cong_trend.notna() & (p.cong_trend < -1)).astype(int)
p["tag_cf_collapse"] = p.cf_collapse
p["tag_mechanical_fix"] = ((p.score_cf >= 65) & p.cong_da_25.notna()
                           & (p.cong_da_25 > -3)).astype(int)
p["tag_curtailment_play"] = ((p.score_cf >= 65) & p.cong_da_25.notna()
                             & (p.cong_da_25 <= -3)).astype(int)
p["tag_neg_price_bleed"] = ((p.ptc_expired == 1)
                            & (p.negp_da_25.fillna(0) >= 0.15)).astype(int)
p["tag_yieldco_stress"] = p.top50_stressed
p["tag_fund_life_exit"] = ((p.financial_owner == 1) & p.oldest_current_stake_yr.notna()
                           & (p.oldest_current_stake_yr <= 2018)).astype(int)
p["tag_oem_orphan"] = p.oem_orphan
p["tag_busted_flip"] = (p.score_flip >= 85).astype(int)
p["tag_ptc_merchant"] = ((p.ptc_expired == 1) & (p.merchant_exposed == 1)).astype(int)
TAG_COL = {"capitulation": "tag_capitulation", "cf-collapse": "tag_cf_collapse",
           "mechanical-fix": "tag_mechanical_fix", "curtailment-play": "tag_curtailment_play",
           "neg-price-bleed": "tag_neg_price_bleed", "yieldco-stress": "tag_yieldco_stress",
           "fund-life-exit": "tag_fund_life_exit", "oem-orphan": "tag_oem_orphan",
           "busted-flip": "tag_busted_flip", "ptc-merchant": "tag_ptc_merchant"}
p["score_tags"] = np.minimum(100, sum(p[c] * TAG_POINTS[t] for t, c in TAG_COL.items()))
p["opportunity_tags"] = p.apply(
    lambda r: "; ".join(t for t, c in TAG_COL.items() if r[c] == 1), axis=1)

# ---------- composite (6 pillars; tag pillar weighted 0.15) ----------
W = {"score_contract": .21, "score_flip": .17, "score_vintage": .17,
     "score_seller": .17, "score_cf": .13, "score_tags": .15}
p["composite"] = sum(p[c] * w for c, w in W.items()).round(1)
p["filter_pass"] = ((p.in_footprint == 1)
                    & ((p.top50_parent == 0) | (p.top50_stressed == 1))).astype(int)
p["rank"] = np.nan
mask = p.filter_pass == 1
p.loc[mask, "rank"] = p.loc[mask, "composite"].rank(ascending=False, method="min")

cols = ["rank", "composite", "filter_pass", "POWER_PLANT", "tech", "OPER_CAPACITY_PLANT",
        "ISO_NAME", "STATE", "COUNTY", "cod_yr", "opportunity_tags", "n_owners",
        "max_stake_pct", "owners",
        "ult_parents", "top50_parent", "top50_stressed", "in_footprint", "financial_owner",
        "repowered", "ptc_expired", "oem_orphan", "cf_2023", "cf_2024", "cf_collapse",
        "negp_da_25", "negp_hours_flag", "oldest_current_stake_yr",
        "score_contract", "contract_reason", "ppa_status", "contracted_pct",
        "LARGEST_PPA_COUNTERPARTY", "ppa_expiry",
        "eqr_label", "eqr_seller", "eqr_iso_share", "eqr_top_bilat_buyer",
        "eqr_bilat_avg_px", "eqr_bilat_max_end", "eqr_rolloff",
        "behavior_label", "shutdown_est", "n_neg_hours", "merchant_exposed", "ppa_exp_3yr",
        "score_seller", "seller_reason", "seller_live_process", "seller_distressed",
        "seller_deal_count", "asset_traded", "asset_trade_year", "asset_trade_buyer",
        "score_flip", "flip_source", "flip_reason", "bf_te_owner", "bf_rank",
        "flip_event", "flip_event_date", "sponsor_small_stake", "sponsor_name",
        "te_candidate_name", "last_own_event_date", "co_owner_recent_exit",
        "pending_transfer",
        "score_vintage", "score_cf", "score_tags", "bf_cf_2324", "bf_cohort_p50", "bf_cf_basis",
        "tag_capitulation", "tag_cf_collapse", "tag_mechanical_fix", "tag_curtailment_play",
        "tag_neg_price_bleed", "tag_yieldco_stress", "tag_fund_life_exit", "tag_oem_orphan",
        "tag_busted_flip", "tag_ptc_merchant", "oem_mfrs",
        "cong_score", "cong_da_24", "cong_da_25", "cong_trend", "pnode",
        "eia_site_code", "map_method",
        "dc_score", "dc_mw_50", "dc_n_50", "dc_mw_100", "nearest_dc_mi",
        "LATITUDE", "LONGITUDE"]
cols = [c for c in cols if c in p.columns]
out = p[cols].rename(columns={"OPER_CAPACITY_PLANT": "capacity_mw", "POWER_PLANT": "plant",
                              "ISO_NAME": "iso", "STATE": "state", "COUNTY": "county",
                              "LARGEST_PPA_COUNTERPARTY": "ppa_counterparty",
                              "LATITUDE": "lat", "LONGITUDE": "lon"})
out = out.sort_values(["filter_pass", "composite"], ascending=[False, False])
for suffix in ("", "_v2", "_v3", "_v4"):
    try:
        path = OUT.replace(".csv", f"{suffix}.csv")
        out.to_csv(path, index=False, encoding="utf-8-sig")
        OUT = path
        break
    except PermissionError:  # file open in Excel
        continue
with open(os.path.join(SCRATCH, "latest_scores_path.txt"), "w") as fh:
    fh.write(OUT)
print(f"\nwrote {OUT}: {len(out)} plants, {int(mask.sum())} pass filters")
print("tag counts:", dict(pd.Series("; ".join(out[out.filter_pass == 1].opportunity_tags
      .dropna()).split("; ")).value_counts().drop("", errors="ignore")))
print("\nComposite distribution (filter_pass):")
print(pd.cut(out[out.filter_pass == 1].composite, bins=range(0, 101, 10)).value_counts().sort_index())
print("\nTop 25 (filter_pass):")
top = out[out.filter_pass == 1].head(25)
for _, r in top.iterrows():
    print(f"  #{int(r['rank'])} {r.composite:5.1f} | {r.plant[:42]:42} | {r.tech:7}"
          f"| {str(r.iso)[:5]:5} {r.state} | {r.capacity_mw:6.0f} MW | COD {r.cod_yr and int(r.cod_yr)} "
          f"| C{r.score_contract:.0f}/F{r.score_flip:.0f}/V{r.score_vintage:.0f}/S{r.score_seller:.0f}/CF{r.score_cf:.0f}")
