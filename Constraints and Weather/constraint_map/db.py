"""Snowflake access for the constraints map.

Thin wrapper over snowflake-connector-python. Credentials come from the
environment (SNOWFLAKE_USER / SNOWFLAKE_ACCOUNT / SNOWFLAKE_PASSWORD), matching
the rest of the repo's secret handling. Queries run read-only against the
Yes Energy share and return lists of dicts.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

import snowflake.connector

logger = logging.getLogger(__name__)

# Yes Energy ERCOT dataset lives here.
YES = "YES_ENERGY__FULL_DATASET.YESDATA"


def _private_key_der() -> bytes | None:
    """Load an RSA private key (PEM) from SNOWFLAKE_PRIVATE_KEY (inline content,
    e.g. a Render env var) or SNOWFLAKE_PRIVATE_KEY_PATH (a local file), returned
    as PKCS8 DER for the connector. Returns None if no key is configured.

    Key-pair auth is a distinct auth method from password, so it does NOT trigger
    Snowflake MFA (Duo) -- this is what lets the app refresh headlessly on Render
    without a phone push. Falls back to password auth when no key is present.
    """
    pem = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not pem and not path:
        return None
    from cryptography.hazmat.primitives import serialization

    data = pem.replace("\\n", "\n").encode() if pem else open(path, "rb").read()
    pwd = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    key = serialization.load_pem_private_key(data, password=pwd.encode() if pwd else None)
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _config() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "user": os.getenv("SNOWFLAKE_USER", "tyler_martin"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "hh33518.east-us-2.azure"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "SKYVEST_WH"),
    }
    der = _private_key_der()
    if der is not None:
        cfg["private_key"] = der          # key-pair auth -> no MFA / no Duo push
    else:
        cfg["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return cfg


@contextmanager
def connect() -> Iterator["snowflake.connector.SnowflakeConnection"]:
    conn = snowflake.connector.connect(**_config())
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a read-only query and return rows as dicts keyed by column name."""
    with connect() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or {})
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()
