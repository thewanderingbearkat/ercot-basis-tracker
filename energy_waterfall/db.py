"""Snowflake connections for the energy waterfall (key-pair only, so no MFA push on Render).

  skyvest()     SkyVest account (GOLD.PERFORMANCE): SKYVEST_SNOWFLAKE_ACCOUNT / _USER / _ROLE / _WAREHOUSE and
                SKYVEST_SNOWFLAKE_PRIVATE_KEY (PEM content, Render secret) or SKYVEST_SNOWFLAKE_PRIVATE_KEY_PATH (local).
                Service user SVC_PERFORMANCE_APP / role PERFORMANCE_APP (powerup-bronze sql/60).
  yes_energy()  Yes Energy share (MISO 5-minute LMPs): the existing SNOWFLAKE_* key pair used by the constraint map.
"""
import os
import threading

import snowflake.connector

SCHEMA = "GOLD.PERFORMANCE"
_lock = threading.Lock()
_conns: dict = {}


def _der(pem: str | None, path: str | None) -> bytes:
    from cryptography.hazmat.primitives import serialization
    if not pem and not path:
        raise RuntimeError("no Snowflake private key configured")
    data = pem.replace("\\n", "\n").encode() if pem else open(os.path.expanduser(path), "rb").read()
    key = serialization.load_pem_private_key(data, password=None)
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def _connect(name: str, **kw):
    with _lock:
        c = _conns.get(name)
        if c is not None and not c.is_closed():
            return c
        # bounded well under gunicorn's 120 s request timeout, so a hang surfaces as an error, not a Render 502 page
        c = snowflake.connector.connect(client_session_keep_alive=True, login_timeout=40, network_timeout=60,
                                        socket_timeout=60,
                                        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "energy-waterfall"}, **kw)
        _conns[name] = c
        return c


def skyvest():
    e = lambda k, d=None: (os.getenv(f"SKYVEST_SNOWFLAKE_{k}") or d)
    return _connect("skyvest", account=e("ACCOUNT", "A8022540555871-MAIN"), user=e("USER", "SVC_PERFORMANCE_APP"),
                    role=e("ROLE", "PERFORMANCE_APP"), warehouse=e("WAREHOUSE", "GENERAL_WH"),
                    database="GOLD", schema="PERFORMANCE",          # session temp tables need a current schema
                    private_key=_der(e("PRIVATE_KEY"), e("PRIVATE_KEY_PATH")), paramstyle="qmark")


def yes_energy():
    return _connect("yes", account=os.getenv("SNOWFLAKE_ACCOUNT", "hh33518.east-us-2.azure"),
                    user=os.getenv("SNOWFLAKE_USER", "tyler_martin"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "SKYVEST_WH"),
                    private_key=_der(os.getenv("SNOWFLAKE_PRIVATE_KEY"), os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")))


def query(sql: str, params=None) -> list[dict]:
    cur = skyvest().cursor()
    cur.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]
