"""Energy Waterfall tab, mounted on the host Flask app (auth is enforced app-wide by app.require_authentication).

    GET /energy-waterfall                                   -- HTML dashboard tab
    GET /api/energy-waterfall/meta                          -- plants with data + date range
    GET /api/energy-waterfall/period?plant=&d0=&d1=         -- step / sub-category / detail totals, top units, context
    GET /api/energy-waterfall/trend?plant=&grain=           -- per-period step totals (Weekly | Monthly | Yearly)
Reads SkyVest Snowflake GOLD.PERFORMANCE (written weekly by energy_waterfall.refresh, the Render cron job).
"""
import faulthandler
import logging
import os
import sys
import threading
import time
from datetime import date

from flask import Blueprint, jsonify, render_template, request

from energy_waterfall import config as C
from energy_waterfall import db

log = logging.getLogger(__name__)
# A native crash (exit 139 / SIGSEGV) leaves no Python traceback; this prints every thread's stack to the Render log.
if not faulthandler.is_enabled():
    faulthandler.enable(file=sys.stderr, all_threads=True)
ew_bp = Blueprint("energy_waterfall", __name__,
                  template_folder=os.path.join(os.path.dirname(__file__), "templates"))
FACT = f"{db.SCHEMA}.ENERGY_WATERFALL_HOURLY"
SITE = f"{db.SCHEMA}.ENERGY_WATERFALL_SITE_HOURLY"
TTL = 900
_cache: dict = {}
_cache_lock = threading.Lock()


def _cached(key, fn):
    with _cache_lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < TTL:
            return hit[1]
    val = fn()
    with _cache_lock:
        _cache[key] = (time.time(), val)
    return val


def _num(v):
    return None if v is None else float(v)


def _args():
    plant = request.args.get("plant", "")
    if plant not in C.PLANTS:
        raise ValueError("unknown plant")
    d0, d1 = date.fromisoformat(request.args["d0"]), date.fromisoformat(request.args["d1"])
    return plant, d0, d1


@ew_bp.route("/energy-waterfall")
def page():
    return render_template("energy_waterfall.html")


@ew_bp.route("/api/energy-waterfall/meta")
def meta():
    def q():
        rows = db.query(f"SELECT PLANT_CODE, MIN(LOCAL_DATE) D0, MAX(LOCAL_DATE) D1 FROM {FACT} GROUP BY 1")
        out = []
        for r in rows:
            p = C.PLANTS.get(r["PLANT_CODE"])
            if not p:
                continue
            out.append({"plant": r["PLANT_CODE"], "name": p["name"], "tech": p["tech"],
                        "compensation": bool(p.get("compensation")), "curtail_label": p.get("curtail_label"),
                        "d0": str(r["D0"]), "d1": str(r["D1"])})
        order = list(C.PLANTS)
        return sorted(out, key=lambda x: order.index(x["plant"]))
    try:
        return jsonify(_cached("meta", q))
    except Exception as e:
        log.exception("energy waterfall meta failed")
        return jsonify({"error": str(e)[:300]}), 500


@ew_bp.route("/api/energy-waterfall/period")
def period():
    try:
        plant, d0, d1 = _args()
    except (KeyError, ValueError) as e:
        return jsonify({"error": f"bad request: {e}"}), 400

    def q():
        prm = [plant, str(d0), str(d1)]
        rows = db.query(f"""SELECT STEP_ORDER, SUBCATEGORY, DETAIL, COALESCE(COMPENSATION, '') COMPENSATION,
                                   SUM(MWH) MWH, SUM(TURBINE_HOURS) TH,
                                   COUNT(DISTINCT IFF(ABS(MWH) > 1e-6, OBJECT_KEY, NULL)) UNITS
                            FROM {FACT} WHERE PLANT_CODE = ? AND LOCAL_DATE BETWEEN ? AND ?
                            GROUP BY 1, 2, 3, 4""", prm)
        subs = db.query(f"""SELECT STEP_ORDER, SUBCATEGORY, COUNT(DISTINCT IFF(ABS(MWH) > 1e-6, OBJECT_KEY, NULL)) UNITS
                            FROM {FACT} WHERE PLANT_CODE = ? AND LOCAL_DATE BETWEEN ? AND ? AND STEP_ORDER BETWEEN 3 AND 8
                            GROUP BY 1, 2""", prm)
        top = db.query(f"""SELECT STEP_ORDER, OBJECT_KEY, SUM(MWH) MWH, SUM(TURBINE_HOURS) TH
                           FROM {FACT} WHERE PLANT_CODE = ? AND LOCAL_DATE BETWEEN ? AND ? AND STEP_ORDER BETWEEN 3 AND 8
                           GROUP BY 1, 2 QUALIFY ROW_NUMBER() OVER (PARTITION BY STEP_ORDER ORDER BY SUM(MWH) DESC) <= 10
                           ORDER BY 1, 3 DESC""", prm)
        steps_units = db.query(f"""SELECT STEP_ORDER, COUNT(DISTINCT IFF(ABS(MWH) > 1e-6, OBJECT_KEY, NULL)) UNITS
                                   FROM {FACT} WHERE PLANT_CODE = ? AND LOCAL_DATE BETWEEN ? AND ?
                                   AND STEP_ORDER BETWEEN 3 AND 8 GROUP BY 1""", prm)
        ctx = db.query(f"""SELECT SUM(LT_POTENTIAL_MWH) LT, SUM(BUDGET_GROSS_MWH) BUDGET, AVG(RESOURCE_ACTUAL) RES,
                                  AVG(RESOURCE_LT) RES_LT, ANY_VALUE(RESOURCE_UNIT) RES_UNIT, SUM(BZ_ACTUAL_MWH) BZ_ACT,
                                  SUM(BZ_THEORETICAL_MWH) BZ_THEO, SUM(BZ_UNAVAILABLE_MWH) BZ_UNAV,
                                  SUM(BZ_LOST_ALL_MWH) BZ_LOST, AVG(TURBINES_MEASURED) MEAS, MAX(TURBINES_TOTAL) TOTAL,
                                  COUNT(*) HOURS, MIN(LOCAL_DATE) FIRST, MAX(LOCAL_DATE) LAST
                           FROM {SITE} WHERE PLANT_CODE = ? AND LOCAL_DATE BETWEEN ? AND ?""", prm)[0]
        return {
            "rows": [{"step": int(r["STEP_ORDER"]), "sub": r["SUBCATEGORY"] or "", "detail": r["DETAIL"] or "",
                      "comp": r["COMPENSATION"] or "", "mwh": _num(r["MWH"]), "th": _num(r["TH"]),
                      "units": int(r["UNITS"] or 0)} for r in rows],
            "sub_units": [{"step": int(r["STEP_ORDER"]), "sub": r["SUBCATEGORY"] or "", "units": int(r["UNITS"] or 0)}
                          for r in subs],
            "step_units": {int(r["STEP_ORDER"]): int(r["UNITS"] or 0) for r in steps_units},
            "top": [{"step": int(r["STEP_ORDER"]), "unit": r["OBJECT_KEY"].split("-")[-1], "mwh": _num(r["MWH"]),
                     "th": _num(r["TH"])} for r in top],
            "ctx": {k.lower(): (str(v) if k in ("FIRST", "LAST", "RES_UNIT") else _num(v)) for k, v in ctx.items()},
        }
    try:
        return jsonify(_cached(("period", plant, d0, d1), q))
    except Exception as e:
        log.exception("energy waterfall period failed")
        return jsonify({"error": str(e)[:300]}), 500


@ew_bp.route("/api/energy-waterfall/trend")
def trend():
    plant, grain = request.args.get("plant", ""), request.args.get("grain", "Weekly")
    if plant not in C.PLANTS or grain not in ("Weekly", "Monthly", "Yearly"):
        return jsonify({"error": "bad request"}), 400
    trunc = {"Weekly": "DATEADD('day', 1 - DAYOFWEEKISO(LOCAL_DATE), LOCAL_DATE)",
             "Monthly": "DATE_TRUNC('MONTH', LOCAL_DATE)", "Yearly": "DATE_TRUNC('YEAR', LOCAL_DATE)"}[grain]

    def q():
        rows = db.query(f"""SELECT {trunc} PERIOD, STEP_ORDER, SUM(MWH) MWH,
                                   SUM(IFF(STARTSWITH(COMPENSATION, 'Presumed'), MWH, 0)) COMP
                            FROM {FACT} WHERE PLANT_CODE = ? GROUP BY 1, 2 ORDER BY 1""", [plant])
        return [{"period": str(r["PERIOD"]), "step": int(r["STEP_ORDER"]), "mwh": _num(r["MWH"]),
                 "comp": _num(r["COMP"])} for r in rows]
    try:
        return jsonify(_cached(("trend", plant, grain), q))
    except Exception as e:
        log.exception("energy waterfall trend failed")
        return jsonify({"error": str(e)[:300]}), 500
