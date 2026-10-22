"""Snowflake connections for the energy waterfall (key-pair only, so no MFA push on Render).

  skyvest()     SkyVest account (GOLD.PERFORMANCE): SKYVEST_SNOWFLAKE_ACCOUNT / _USER / _ROLE / _WAREHOUSE and
                SKYVEST_SNOWFLAKE_PRIVATE_KEY (PEM content, Render secret) or SKYVEST_SNOWFLAKE_PRIVATE_KEY_PATH (local).
                Service user SVC_PERFORMANCE_APP / role PERFORMANCE_APP (powerup-bronze sql/60).
  yes_energy()  Yes Energy share (MISO 5-minute LMPs): the existing SNOWFLAKE_* key pair used by the constraint map.
"""
import os
import threading
import time

import snowflake.connector
from snowflake.connector.errors import DatabaseError

SCHEMA = "GOLD.PERFORMANCE"
_lock = threading.Lock()
_query_lock = threading.Lock()      # the web app serves 4 threads; never use one connection concurrently
_conns: dict = {}                   # name -> (connection, created monotonic seconds)
# keep-alive is off (heartbeat thread + connector 5 segfaulted on Render), so an idle session's token expires after
# a few hours; rebuild well before that, and reconnect once on the expiry error if it slips through anyway
_MAX_AGE_S = 3600
_EXPIRED_ERRNOS = {390111, 390112, 390114}     # session gone / no longer exists / authentication token expired


def _expired(e: Exception) -> bool:
    msg = str(e).lower()
    return getattr(e, "errno", None) in _EXPIRED_ERRNOS or "token has expired" in msg or "session no longer" in msg


def _drop(name: str):
    with _lock:
        c, _ = _conns.pop(name, (None, 0))
    if c is not None:
        try:
            c.close()
        except Exception:
            pass


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
        c, born = _conns.get(name, (None, 0))
        if c is not None and not c.is_closed() and time.monotonic() - born < _MAX_AGE_S:
            return c
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
        # bounded well under gunicorn's 120 s request timeout, so a hang surfaces as an error, not a Render 502 page
        c = snowflake.connector.connect(client_session_keep_alive=False, login_timeout=40, network_timeout=60,
                                        socket_timeout=60,
                                        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "energy-waterfall"}, **kw)
        _conns[name] = (c, time.monotonic())
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


def _run(sql: str, params) -> list[dict]:
    cur = skyvest().cursor()
    try:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        try:
            cur.close()
        except Exception:
            pass


def query(sql: str, params=None) -> list[dict]:
    with _query_lock:
        try:
            return _run(sql, params)
        except DatabaseError as e:          # ProgrammingError is a subclass
            if not _expired(e):
                raise
            _drop("skyvest")
            return _run(sql, params)
