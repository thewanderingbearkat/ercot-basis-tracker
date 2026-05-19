"""JSON cache for fetched API data and computed shadow results."""
import json
import logging
import os

logger = logging.getLogger(__name__)


def save_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, default=str, indent=2)
    logger.info("Saved %s", path)


def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)
