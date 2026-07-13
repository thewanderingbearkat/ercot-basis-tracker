"""Validate S&P CF (2023/2024) against independently derived EIA-923 CF from Yes Energy.

Cohorts: (a) 31 CF-collapse plants, (b) 20 worst CF-vs-P50 underperformers,
(c) 20 random controls with normal CF. For each: CF_yes = annual net gen /
(nameplate * hours). Divergence >3pp = flag. Collapse plants also get a monthly
zero-month count (casualty vs curtailment vs artifact) and an S&P-vs-YES
nameplate check (phase-aggregation artifact detector).
"""
import sys, json
import numpy as np
import pandas as pd

sys.path.insert(0, r"C:\Users\TylerMartin\ercot-basis-tracker\Constraints and Weather\constraint_map")
import db

SCRATCH = r"C:\Users\TYLERM~1\AppData\Local\Temp\claude\C--Users-TylerMartin\be345640-8abd-4677-a763-c3972730811c\scratchpad"

own = pd.read_excel(r"C:\Users\TylerMartin\Downloads\SkyVest_Ownership_Records_AllUS_ISO.xlsx",
                    sheet_name="Ownership Records")
p = own.drop_duplicates("Plant").copy()
p = p[p["Cap (MW)"] >= 50]
p["cf23"], p["cf24"] = p["CF 2023 (%)"], p["CF 2024 (%)"]

final = pd.read_csv(r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop\Seller_Screen_v3_Scores_v2.csv",
                    encoding="utf-8-sig")
cong = pd.read_csv(f"{SCRATCH}\\congestion_overlay.csv", encoding="utf-8-sig")
eia = cong.set_index("POWER_PLANT")["eia_site_code"]

ok = p.cf23.notna() & p.cf24.notna() & (p.cf23 > 5)
collapse = p[ok & (p.cf24 < 0.75 * p.cf23)].copy(); collapse["cohort"] = "collapse"
und_names = final[(final.score_cf >= 65)].nsmallest(20, "bf_cf_2324")["plant"]
under = p[p.Plant.isin(set(und_names))].copy(); under["cohort"] = "underperf"
ctrl = p[ok & (p.cf24 >= 0.9 * p.cf23) & (p.cf24 > 20)].sample(20, random_state=7).copy()
ctrl["cohort"] = "control"
sample = pd.concat([collapse, under, ctrl]).drop_duplicates("Plant")
sample["eia_code"] = sample["EIA Site Code"].astype("Int64")
sample = sample[sample.eia_code.notna()]
print(f"sample: {len(sample)} plants ({sample.cohort.value_counts().to_dict()})")

codes = ",".join(str(c) for c in sample.eia_code.unique())
oid = pd.DataFrame(db.query(
    f"SELECT PLANT_CODE, OBJECTID, PLANT_NAME FROM YES_ENERGY__FULL_DATASET.YESDATA.DS_PLANTS "
    f"WHERE PLANT_CODE IN ({codes})"))
print(f"DS_PLANTS hits: {len(oid)}/{sample.eia_code.nunique()}")
oids = ",".join(str(o) for o in oid.OBJECTID.unique())

npl = pd.DataFrame(db.query(
    "SELECT r.OBJECTID2 PLANT_OID, SUM(u.NAMEPLATE) NAMEPLATE "
    "FROM YES_ENERGY__FULL_DATASET.YESDATA.OBJECT_RELATIONSHIPS r "
    "JOIN YES_ENERGY__FULL_DATASET.YESDATA.DS_UNITS u ON u.OBJECTID = r.OBJECTID1 "
    f"WHERE r.RELTYPE = 'UNIT2PLANT' AND r.OBJECTID2 IN ({oids}) GROUP BY 1"))

gen = pd.DataFrame(db.query(
    "SELECT OBJECTID, YEAR(DATETIME) YR, MONTH(DATETIME) MO, SUM(VALUE) GEN "
    "FROM YES_ENERGY__FULL_DATASET.YESDATA.TS_EIA923_GEN_V "
    f"WHERE DATATYPEID = 10685 AND OBJECTID IN ({oids}) "
    "AND DATETIME >= '2023-01-01' AND DATETIME < '2025-01-01' GROUP BY 1,2,3"))
print(f"gen rows: {len(gen)}")
gen["GEN"] = gen.GEN.astype(float)
npl["NAMEPLATE"] = npl.NAMEPLATE.astype(float)

ann = gen.groupby(["OBJECTID", "YR"]).GEN.sum().unstack()
zero_mo = gen[(gen.YR == 2024) & (gen.GEN <= 0)].groupby("OBJECTID").size().rename("zero_mo_24")
mo_n = gen[gen.YR == 2024].groupby("OBJECTID").size().rename("n_mo_24")

m = sample.merge(oid.rename(columns={"PLANT_CODE": "eia_code"}), on="eia_code", how="left") \
          .merge(npl, left_on="OBJECTID", right_on="PLANT_OID", how="left")
m = m.merge(ann, left_on="OBJECTID", right_index=True, how="left") \
     .merge(zero_mo, left_on="OBJECTID", right_index=True, how="left") \
     .merge(mo_n, left_on="OBJECTID", right_index=True, how="left")
m["cf23_yes"] = m[2023] / (m.NAMEPLATE * 8760) * 100
m["cf24_yes"] = m[2024] / (m.NAMEPLATE * 8784) * 100
m["d23"], m["d24"] = m.cf23_yes - m.cf23, m.cf24_yes - m.cf24
m["np_ratio"] = m["Cap (MW)"] / m.NAMEPLATE  # >1.5 => S&P merged phases S&P cap vs EIA plant

for coh in ("collapse", "underperf", "control"):
    g = m[(m.cohort == coh) & m.cf24_yes.notna()]
    agree = ((g.d23.abs() <= 3) & (g.d24.abs() <= 3)).mean() * 100
    print(f"\n== {coh}: n={len(g)}, agree(<=3pp both yrs)={agree:.0f}%, "
          f"median |d24|={g.d24.abs().median():.1f}pp")
    bad = g[(g.d23.abs() > 3) | (g.d24.abs() > 3)]
    if len(bad):
        print(bad[["Plant", "ISO", "Cap (MW)", "NAMEPLATE", "np_ratio",
                   "cf23", "cf23_yes", "cf24", "cf24_yes", "zero_mo_24"]]
              .round(1).to_string(index=False, max_colwidth=34))

g = m[(m.cohort == "collapse") & m.cf24_yes.notna()]
print("\n== collapse-plant diagnosis (confirmed = YES CF also collapsed):")
g = g.assign(confirmed=(g.cf24_yes < 0.75 * g.cf23_yes))
print(g[["Plant", "ISO", "cf23", "cf24", "cf23_yes", "cf24_yes", "zero_mo_24", "confirmed"]]
      .round(1).to_string(index=False, max_colwidth=34))
print("\nconfirmed collapse:", int(g.confirmed.sum()), "/", len(g))
m.to_csv(f"{SCRATCH}\\cf_validation_detail.csv", index=False, encoding="utf-8-sig")
