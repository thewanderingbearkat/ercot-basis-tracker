"""Congestion-model dashboard blueprint.

    GET /model                    -- the page (live-vs-forecast tracker + walkthrough)
    GET /api/model/tracking       -- latest stored forecast joined to realized basis
    GET /api/model/scorecard      -- accumulated accuracy across all elapsed forecast hours
    GET /api/model/drivers        -- latest run's largest drivers
    GET /api/model/structural     -- GTC shift-factor sensitivities (scenario calculator)
    GET /api/model/budget         -- multi-horizon monthly budget curve (3mo / 3y)

READ-ONLY. Forecasts are written to SKYVEST.DBO.CM_CONGEST_FORECAST / _DRIVERS by
congestion_model/forecast_demo.py --log (run locally or on a schedule); this app only reads
them and joins realized basis from DART_PRICES. So the Render app needs no sklearn -- it just
shows how the live grid is tracking against what the model projected, and a growing track record.
"""
import datetime as dt
import decimal
import logging
import os
import sys

from flask import Blueprint, jsonify, render_template

from constraint_map.db import YES, query

logger = logging.getLogger(__name__)

model_bp = Blueprint("model_dashboard", __name__, template_folder="templates")

# structural.py lives in congestion_model/ at the repo root (pure arithmetic, no sklearn).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "congestion_model"))
try:
    import structural as _structural
    _GTC = _structural.GTC
    _SENS = _structural.SENSITIVITIES
    _BASE = {"atc": _structural.BASE_ATC, "gwa": _structural.BASE_GWA}
except Exception as _se:                       # fall back to a static copy so the tab still loads
    logger.warning("structural.py import failed (%s); using static copy", _se)
    _structural = None
    _GTC = {"6965__A": {"rel_sf": 0.267, "avg_shadow": 77.0, "p90_shadow": 155.0},
            "6056__A": {"rel_sf": 0.226, "avg_shadow": 84.0, "p90_shadow": 202.0},
            "16050__B": {"rel_sf": 0.206, "avg_shadow": 350.0, "p90_shadow": 669.0}}
    _SENS, _BASE = {}, {"atc": 0.6, "gwa": -2.2}

NODE = "NBOHR_RN"
NBOHR, HB_WEST = 10004202409, 10000697080
BLOWOUT = -20          # $/MWh basis threshold the model flags
FLAG = 0.20            # P_blowout at/above which we count the model as "flagging" the hour

# Realized hourly basis (NBOHR - HB_WEST) from DART_PRICES, from `since` onward.
_REALIZED = f"""
  SELECT DATE_TRUNC('hour', DATETIME) AS HR,
         AVG(IFF(OBJECTID={NBOHR}, RTLMP, NULL)) - AVG(IFF(OBJECTID={HB_WEST}, RTLMP, NULL)) AS BASIS
  FROM {YES}.DART_PRICES
  WHERE OBJECTID IN ({NBOHR},{HB_WEST}) AND RTLMP IS NOT NULL AND DATETIME >= {{since}}
  GROUP BY 1"""


def _clean(rows):
    """Coerce Snowflake Decimal -> float and datetime -> ISO string for jsonify."""
    out = []
    for r in rows:
        d = {}
        for k, v in r.items():
            if isinstance(v, decimal.Decimal):
                d[k] = float(v)
            elif isinstance(v, (dt.datetime, dt.date)):
                d[k] = v.isoformat()
            else:
                d[k] = v
        out.append(d)
    return out


@model_bp.route("/model")
def model_page():
    return render_template("model_dashboard.html")


@model_bp.route("/api/model/tracking")
def api_tracking():
    """Latest run's hourly forecast band + per-hour blowout prob, with realized basis joined
    in for the hours that have already elapsed (NULL ahead of now)."""
    try:
        sql = f"""
        WITH latest AS (SELECT MAX(RUN_DATE) rd FROM SKYVEST.DBO.CM_CONGEST_FORECAST WHERE NODE='{NODE}'),
        realized AS ({_REALIZED.format(since="(SELECT rd FROM latest)")})
        SELECT f.TARGET_HOUR, f.LEAD_H, f.Q10, f.Q50, f.Q90, f.P_BLOWOUT,
               f.XW_WIND, f.XW_GHI, r.BASIS AS REALIZED
        FROM SKYVEST.DBO.CM_CONGEST_FORECAST f
        LEFT JOIN realized r ON r.HR = f.TARGET_HOUR
        WHERE f.NODE='{NODE}' AND f.RUN_DATE=(SELECT rd FROM latest)
        ORDER BY f.TARGET_HOUR"""
        rows = _clean(query(sql))
        run_date = rows[0]["TARGET_HOUR"][:10] if rows else None
        return jsonify({"node": NODE, "run_date": run_date, "blowout": BLOWOUT, "hours": rows})
    except Exception as e:
        logger.exception("model tracking failed: %s", e)
        return jsonify({"node": NODE, "run_date": None, "hours": [], "error": str(e)})


@model_bp.route("/api/model/scorecard")
def api_scorecard():
    """Accumulated track record across EVERY elapsed forecast hour (all runs): how often
    realized landed in the band, median-forecast error, and the blowout flag's hit-rate."""
    try:
        sql = f"""
        WITH realized AS ({_REALIZED.format(since="(SELECT MIN(TARGET_HOUR) FROM SKYVEST.DBO.CM_CONGEST_FORECAST)")}),
        scored AS (
          SELECT f.Q10, f.Q50, f.Q90, f.P_BLOWOUT, r.BASIS,
                 IFF(r.BASIS BETWEEN f.Q10 AND f.Q90, 1, 0) AS in_band,
                 ABS(f.Q50 - r.BASIS) AS ae,
                 IFF(r.BASIS < {BLOWOUT}, 1, 0) AS actual_blowout,
                 IFF(f.P_BLOWOUT >= {FLAG}, 1, 0) AS flagged
          FROM SKYVEST.DBO.CM_CONGEST_FORECAST f
          JOIN realized r ON r.HR = f.TARGET_HOUR
          WHERE f.NODE='{NODE}')
        SELECT COUNT(*) AS n, AVG(in_band) AS coverage, AVG(ae) AS mae,
               SUM(actual_blowout) AS n_blowout, SUM(flagged) AS n_flagged,
               SUM(actual_blowout*flagged) AS hits, AVG(BASIS) AS avg_basis
        FROM scored"""
        r = _clean(query(sql))[0]
        n = r.get("N") or 0
        prec = (r["HITS"] / r["N_FLAGGED"]) if r.get("N_FLAGGED") else None
        rec = (r["HITS"] / r["N_BLOWOUT"]) if r.get("N_BLOWOUT") else None
        return jsonify({"n": n, "coverage": r.get("COVERAGE"), "mae": r.get("MAE"),
                        "n_blowout": r.get("N_BLOWOUT"), "n_flagged": r.get("N_FLAGGED"),
                        "precision": prec, "recall": rec, "avg_basis": r.get("AVG_BASIS"),
                        "flag_threshold": FLAG, "blowout": BLOWOUT})
    except Exception as e:
        logger.exception("model scorecard failed: %s", e)
        return jsonify({"n": 0, "error": str(e)})


@model_bp.route("/api/model/drivers")
def api_drivers():
    """Latest run's largest drivers (permutation AUC importance)."""
    try:
        rows = _clean(query(f"""
            SELECT FEATURE, LABEL, IMPORTANCE FROM SKYVEST.DBO.CM_CONGEST_DRIVERS
            WHERE NODE='{NODE}' AND RUN_DATE=(SELECT MAX(RUN_DATE) FROM SKYVEST.DBO.CM_CONGEST_DRIVERS WHERE NODE='{NODE}')
            ORDER BY IMPORTANCE DESC"""))
        return jsonify({"drivers": rows})
    except Exception as e:
        logger.exception("model drivers failed: %s", e)
        return jsonify({"drivers": [], "error": str(e)})


@model_bp.route("/api/model/structural")
def api_structural():
    """The GTC shift-factor sensitivities for the client-side scenario calculator.
    Delta_basis = -(shadow_price * rel_sf) per constraint; new load -> -(slope*MW*rel_sf)."""
    return jsonify({"node": NODE, "constraints": _GTC, "blowout": BLOWOUT,
                    "sensitivities": _SENS, "baseline": _BASE,
                    "note": "NBOHR basis when the West-TX export constraints bind at a given "
                            "shadow price. rel_sf is the (near-constant) relative shift factor."})


@model_bp.route("/api/model/budget")
def api_budget():
    """Multi-horizon monthly budget curve (latest run) -- climatology basis out to 3 years.
    One 36-month curve; the UI slices the first 3 (medium) vs all (long)."""
    try:
        rows = _clean(query(f"""
            SELECT PERIOD, MONTHS_AHEAD, P10, P50, P90, EXPECTED, BLOWOUT_PCT, HIST_BASIS
            FROM SKYVEST.DBO.CM_CONGEST_BUDGET
            WHERE NODE='{NODE}' AND RUN_DATE=(SELECT MAX(RUN_DATE) FROM SKYVEST.DBO.CM_CONGEST_BUDGET WHERE NODE='{NODE}')
            ORDER BY MONTHS_AHEAD"""))
        # Overlay nFront's ATC basis: for each budget month use the scenario whose year is the
        # latest at/before that month (2026 Base covers 2026-28, 2029 Base covers 2029, ...).
        tp = _clean(query(f"SELECT SCEN_YEAR, MONTH, BASIS_ATC FROM SKYVEST.DBO.CM_BASIS_THIRDPARTY "
                          f"WHERE NODE='{NODE}' AND SOURCE='nFront'"))
        tp_map = {(int(r["SCEN_YEAR"]), int(r["MONTH"])): r["BASIS_ATC"] for r in tp}
        scen_years = sorted({int(r["SCEN_YEAR"]) for r in tp})
        for m in rows:
            yr, mo = int(m["PERIOD"][:4]), int(m["PERIOD"][5:7])
            elig = [y for y in scen_years if y <= yr]
            m["NFRONT"] = tp_map.get((max(elig), mo)) if elig else None
        return jsonify({"node": NODE, "run_date": (rows[0].get("PERIOD") if rows else None),
                        "months": rows, "thirdparty": "nFront (ATC)" if tp else None})
    except Exception as e:
        logger.exception("model budget failed: %s", e)
        return jsonify({"months": [], "error": str(e)})
