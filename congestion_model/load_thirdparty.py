"""Load nFront's ATC basis forecast (the third-party data point) into Snowflake so the
/model budget chart can overlay it against our model and realized history.

Source: the nFront "ERCOT SCED A&R Tables" workbook (.xlsb), sheet "5A SCED Simple Avg LMP"
= ATC (around-the-clock / simple-average) monthly LMP by location, per scenario. nFront's
ATC basis = McCrae II LMP - ERCOT West (HB_WEST) LMP, monthly. (GWA / production-weighted is
their other cut -- site-specific, a later layer.) Scenarios map to representative years.

The file lives on OneDrive and is often locked open -> we copy to a temp path first, then read.
Writes SKYVEST.DBO.CM_BASIS_THIRDPARTY (idempotent for SOURCE='nFront').

    python load_thirdparty.py          # parse + preview
    python load_thirdparty.py --log    # also write to Snowflake
"""
import os
import subprocess
import sys
import tempfile

import pandas as pd

SRC = r"C:\Users\TylerMartin\OneDrive - ArcLight Renewable Services\Desktop\ERCOT SCED A&R Tables_ArcLight_McCrae II Wind_20251121_To Client.xlsb"
NODE = "NBOHR_RN"
SOURCE = "nFront"
MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
          "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
# nFront scenario label -> representative calendar year (for aligning to our forward months).
SCEN_YEAR = {"2026 Base": 2026, "2029 Base": 2029, "2030 Upgrade": 2030,
             "2032 Upgrade": 2032, "Out Year": 2035}


def read_5A():
    """Copy past any OneDrive lock, read sheet 5A, return rows of (scenario, year, month, atc_basis)."""
    tmp = os.path.join(tempfile.gettempdir(), "nfront_5A.xlsb")
    # OneDrive keeps the file open with a share mode Python's open() can't satisfy, but
    # PowerShell's Copy-Item can read it -- shell out for the copy, then read the copy.
    subprocess.run(["powershell", "-NoProfile", "-Command",
                    f"Copy-Item -LiteralPath '{SRC}' -Destination '{tmp}' -Force"], check=True)
    df = pd.read_excel(tmp, sheet_name="5A SCED Simple Avg LMP", header=None, engine="pyxlsb")

    # Scenario labels sit on the row above the "Month | McCrae II | ... | ERCOT West" header.
    hdr = next(i for i in range(df.shape[0]) if str(df.iloc[i, 0]).strip() == "Month")
    scen_row = df.iloc[hdr - 1]
    scenarios = {int(c): str(scen_row[c]).strip() for c in range(df.shape[1])
                 if pd.notna(scen_row[c]) and str(scen_row[c]).strip()}
    # Within each scenario block the columns are McCrae II, Houston, North, South, ERCOT West.
    out = []
    for c, name in scenarios.items():
        mc_col, west_col = c, c + 4                      # McCrae at the label col, West +4
        if name not in SCEN_YEAR:
            continue
        for r in range(hdr + 1, df.shape[0]):
            mlabel = str(df.iloc[r, 0]).strip()[:3]
            if mlabel in MONTHS:
                mc, west = df.iloc[r, mc_col], df.iloc[r, west_col]
                if pd.notna(mc) and pd.notna(west):
                    out.append({"scenario": name, "year": SCEN_YEAR[name],
                                "month": MONTHS[mlabel], "atc_basis": round(float(mc) - float(west), 3)})
    return out


def log_to_snowflake(rows):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Constraints and Weather"))
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    from constraint_map.db import query
    query("""CREATE TABLE IF NOT EXISTS SKYVEST.DBO.CM_BASIS_THIRDPARTY (
        NODE STRING, SOURCE STRING, SCENARIO STRING, SCEN_YEAR INT, MONTH INT,
        BASIS_ATC FLOAT, LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""")
    query(f"DELETE FROM SKYVEST.DBO.CM_BASIS_THIRDPARTY WHERE NODE='{NODE}' AND SOURCE='{SOURCE}'")
    vals = ",".join(f"('{NODE}','{SOURCE}','{r['scenario']}',{r['year']},{r['month']},{r['atc_basis']})"
                    for r in rows)
    query("INSERT INTO SKYVEST.DBO.CM_BASIS_THIRDPARTY (NODE,SOURCE,SCENARIO,SCEN_YEAR,MONTH,BASIS_ATC) "
          "VALUES " + vals)
    print(f"logged {len(rows)} nFront ATC basis rows to Snowflake")


if __name__ == "__main__":
    rows = read_5A()
    df = pd.DataFrame(rows)
    print("nFront ATC basis (McCrae - West, $/MWh) by scenario:")
    piv = df.pivot_table(index="month", columns="scenario", values="atc_basis")
    print(piv.round(1).to_string())
    print("\nscenario annual mean ATC basis:")
    print(df.groupby(["scenario", "year"])["atc_basis"].mean().round(2).to_string())
    if "--log" in sys.argv:
        log_to_snowflake(rows)
