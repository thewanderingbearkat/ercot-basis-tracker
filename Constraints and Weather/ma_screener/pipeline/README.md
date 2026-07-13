# M&A Seller Screener — scoring pipeline

Produces the scored plant universe behind the `/ma-screener` dashboard tab and the
`Seller_Screen_v3*.xlsx` workbook. The dashboard reads `SKYVEST.DBO.MA_SCREEN_PLANTS`;
nothing here runs on Render — refreshes are run locally and pushed to Snowflake.

## Refresh sequence

Set `MA_SCREEN_WORKDIR` to a working directory holding the component CSVs, then:

1. **Component CSVs** (into the workdir) — built from the source exports; see each
   file's header for provenance:
   `eqr_plant_labels.csv` (FERC EQR contract classifier), `seller_signals.csv`
   (transaction.csv deal intel), `dc_proximity.csv`, `bustedflip_port.csv`,
   `congestion_overlay.csv` (Yes Energy plant→node→DA cong), `flip_precision.csv`
   (ownership-records stake fingerprints), `negprice_overlay.csv`.
2. `python patch_seller_signals_sector_gate.py %MA_SCREEN_WORKDIR%` — keeps the
   live-sale-process flag only for owners on *renewables* live processes
   (see file docstring for the Kingfisher/BlackRock incident that motivated it).
3. `python assemble_v3.py` — merges components onto the S&P Asset/Unit screens,
   computes the 6 pillars + opportunity tags, writes `Seller_Screen_v3_Scores*.csv`
   to the Desktop and a `latest_scores_path.txt` pointer to the workdir.
4. `python ../load_to_snowflake.py <scores.csv>` — full-replaces
   `SKYVEST.DBO.MA_SCREEN_PLANTS` (the dashboard picks it up within its 15-min cache).
5. `python build_workbook.py` — optional: rebuilds the auditable Excel workbook
   (Read Me weights/tag points, Screen, Tag Audit formulas, source tabs). Recalc and
   verify in Excel afterwards (COM snippet in the repo history / session notes).

## Source exports expected

- Desktop: `Asset Screen_*.xlsx`, `Unit Screen_*.xls`, `Top 50 Asset Owners_*.xlsx`
- Downloads: `SkyVest_Ownership_Records_AllUS_ISO.xlsx` (needs EIA Site Code),
  `SkyVest_BustedFlip_Screen_v2.xlsx`, `transaction.csv`, `projects-export.csv`,
  `data-center.csv`
- Snowflake: `AMA.EQR.*`, `YES_ENERGY__FULL_DATASET.YESDATA.*` (key-pair auth via
  `SNOWFLAKE_PRIVATE_KEY_PATH`, same as constraint_map/db.py)

## Known data caveats (mirrors the workbook Read Me)

CF validated vs raw EIA-923 (~90% within 3pp; collapse flags 94% confirmed); EQR is
structurally blind in ERCOT; ownership export lacks batteries/non-ISO plants and
ended stakes; congestion is a time-average; DC pipeline MW is announcement-inflated.
