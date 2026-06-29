"""Canadian Hills (SPP) dashboard tab -- the curtailment + basis story.

    GET /canadian-hills        -- the page
    GET /api/cdnhills/summary  -- yearly + monthly summary (read from Snowflake)

READ-ONLY. The numbers are computed locally by congestion_model/cdnhills_log.py --log
(which needs the Anemoi generation file + Yes Energy) and written to SKYVEST.DBO.CM_CDNHILLS_*;
this just serves them. Canadian Hills' damage is CURTAILMENT (volume), not price -- the tab
leads with that, in contrast to McCrae's price-suppression story on /model.
"""
import datetime as dt
import decimal
import logging

from flask import Blueprint, jsonify, render_template

from constraint_map.db import query

logger = logging.getLogger(__name__)
cdnhills_bp = Blueprint("cdnhills_dashboard", __name__, template_folder="templates")
NODE = "CANADIAN_HILLS"


def _clean(rows):
    out = []
    for r in rows:
        o = {}
        for k, v in r.items():
            o[k] = float(v) if isinstance(v, decimal.Decimal) else (
                v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else v)
        out.append(o)
    return out


@cdnhills_bp.route("/canadian-hills")
def cdnhills_page():
    return render_template("cdnhills_dashboard.html")


@cdnhills_bp.route("/api/cdnhills/summary")
def api_summary():
    try:
        yearly = _clean(query(f"""
            SELECT YR, ATC, GWA, DELIV_MWH, CURT_MWH, CURT_PCT, HUB_PRICE, NODE_PRICE, LOST_VALUE
            FROM SKYVEST.DBO.CM_CDNHILLS_YEARLY WHERE NODE='{NODE}' ORDER BY YR"""))
        monthly = _clean(query(f"""
            SELECT PERIOD, ATC, GWA, DELIV_MWH, CURT_MWH, CURT_PCT
            FROM SKYVEST.DBO.CM_CDNHILLS_MONTHLY WHERE NODE='{NODE}' ORDER BY PERIOD"""))
        return jsonify({"node": NODE, "yearly": yearly, "monthly": monthly})
    except Exception as e:
        logger.exception("cdnhills summary failed: %s", e)
        return jsonify({"node": NODE, "yearly": [], "monthly": [], "error": str(e)})
