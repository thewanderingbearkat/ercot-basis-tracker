"""Tenaska / PTP Energy API authentication."""
import logging
import os

import requests

from shadow_trader.config import TENASKA_TOKEN_URL

logger = logging.getLogger(__name__)


def get_tenaska_token() -> str | None:
    user = os.getenv("TENASKA_API_USER")
    password = os.getenv("TENASKA_API_PASSWORD")
    if not user or not password:
        raise RuntimeError("TENASKA_API_USER and TENASKA_API_PASSWORD must be set in the environment (.env)")
    try:
        response = requests.get(TENASKA_TOKEN_URL, auth=(user, password), timeout=30)
    except Exception as e:
        logger.error("Error reaching Tenaska token endpoint: %s", e)
        return None
    if response.status_code != 200:
        logger.error("Tenaska token request failed: HTTP %s — %s", response.status_code, response.text[:200])
        return None
    return response.json().get("data")
