"""M&A Seller Screener tab -- self-service faceted screen over the plant universe.

    GET /ma-screener            -- the page (filter rail + table + coverage panel)
    GET /api/mascreen/plants    -- full plant table from SKYVEST.DBO.MA_SCREEN_PLANTS
                                   (cached; ?fresh=1 bypasses)

    GET  /api/mascreen/notes    -- full per-plant notes ledger (append-only)
    POST /api/mascreen/notes    -- add a note {plant, note, author}

Plant data is READ-ONLY, produced by the screener pipeline (assemble_v3.py) and pushed
by ma_screener/load_to_snowflake.py. Notes live in their own table
(SKYVEST.DBO.MA_SCREEN_NOTES) precisely so the pipeline's full-replace of the plants
table never touches the ledger. Design intent: the tab is NOT prescriptive -- every
facet carries an explicit "Not assessed"/"Unknown" bucket so data gaps are visible
and selectable, and the composite score is just one optional sort among many.
"""
import datetime as dt
import decimal
import logging
import time

from flask import Blueprint, jsonify, render_template, request

from constraint_map import db
from constraint_map.db import query

logger = logging.getLogger(__name__)
ma_screener_bp = Blueprint("ma_screener", __name__, template_folder="templates")

CACHE_TTL = 900.0
_cache: dict | None = None
_cache_at: float = 0.0


def _clean(rows):
    out = []
    for r in rows:
        o = {}
        for k, v in r.items():
            if isinstance(v, decimal.Decimal):
                v = float(v)
            elif isinstance(v, (dt.date, dt.datetime)):
                v = v.isoformat()
            o[k.lower()] = v
        out.append(o)
    return out


@ma_screener_bp.route("/ma-screener")
def page():
    return render_template("ma_screener.html")


@ma_screener_bp.route("/screener-map")
def lineage_map():
    """Radial lineage web: screener -> pillars -> data sources -> columns."""
    return render_template("ma_screener_map.html")


@ma_screener_bp.route("/api/mascreen/plants")
def api_plants():
    global _cache, _cache_at
    age = time.time() - _cache_at
    if request.args.get("fresh") != "1" and _cache is not None and age < CACHE_TTL:
        return jsonify({**_cache, "cache_age_seconds": round(age, 1)})
    try:
        rows = _clean(query(
            "SELECT * FROM SKYVEST.DBO.MA_SCREEN_PLANTS ORDER BY COMPOSITE DESC"))
        loaded = max((r.get("loaded_at") or "" for r in rows), default=None)
        data = {"plants": rows, "n": len(rows), "loaded_at": loaded}
    except Exception as e:
        logger.exception("mascreen plants failed")
        return jsonify({"plants": [], "n": 0, "error": str(e)}), 502
    _cache, _cache_at = data, time.time()
    return jsonify({**data, "cache_age_seconds": 0})


NOTES_TABLE = "SKYVEST.DBO.MA_SCREEN_NOTES"


@ma_screener_bp.route("/api/mascreen/notes")
def api_notes():
    """Full ledger, newest first. Small table; no cache so new notes show immediately."""
    try:
        rows = _clean(query(
            f"SELECT ID, PLANT, AUTHOR, NOTE, CREATED_AT FROM {NOTES_TABLE} "
            "ORDER BY CREATED_AT DESC"))
        return jsonify({"notes": rows, "n": len(rows)})
    except Exception as e:
        logger.exception("mascreen notes fetch failed")
        return jsonify({"notes": [], "n": 0, "error": str(e)}), 502


@ma_screener_bp.route("/api/mascreen/notes", methods=["POST"])
def api_add_note():
    """Append-only: notes are a ledger, there is no edit/delete surface by design."""
    body = request.get_json(silent=True) or {}
    plant = (body.get("plant") or "").strip()[:300]
    note = (body.get("note") or "").strip()[:4000]
    author = (body.get("author") or "").strip()[:60]
    if not plant or not note:
        return jsonify({"error": "plant and note are required"}), 400
    try:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute(
                f"INSERT INTO {NOTES_TABLE} (PLANT, AUTHOR, NOTE) VALUES (%s, %s, %s)",
                (plant, author or None, note))
            row = cur.execute(
                f"SELECT ID, PLANT, AUTHOR, NOTE, CREATED_AT FROM {NOTES_TABLE} "
                "WHERE PLANT = %s ORDER BY CREATED_AT DESC LIMIT 1", (plant,)).fetchone()
            cols = [d[0] for d in cur.description]
        return jsonify({"saved": _clean([dict(zip(cols, row))])[0]})
    except Exception as e:
        logger.exception("mascreen note insert failed")
        return jsonify({"error": str(e)}), 502
