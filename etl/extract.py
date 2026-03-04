"""
Extract module: reads raw JSON logs from file.
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def extract_from_json(file_path: str) -> list[dict[str, Any]]:
    """
    Read raw log entries from a JSON file.

    Args:
        file_path: Path to the raw_logs.json file.

    Returns:
        List of raw log dictionaries.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file is not valid JSON.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Raw data file not found: {file_path}")

    logger.info("Extracting data from %s", file_path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info("Extracted %d raw log entries", len(data))
    return data
