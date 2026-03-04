"""
Transform module: cleans and reshapes raw log data into star-schema-ready tables.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def clean_and_transform(raw_data: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Clean raw log entries:
      - Remove entries with missing user_id or action_type.
      - Convert timestamps to uniform ISO 8601 / UTC format.
      - Extract device and location from nested metadata.

    Args:
        raw_data: List of raw log dictionaries.

    Returns:
        Cleaned pandas DataFrame with columns:
            user_id, action_type, event_timestamp, device, location
    """
    df = pd.DataFrame(raw_data)
    initial_count = len(df)
    logger.info("Starting transformation on %d records", initial_count)

    # ------------------------------------------------------------------
    # 1. Drop rows where user_id or action_type is missing / null
    # ------------------------------------------------------------------
    df = df.dropna(subset=["user_id", "action_type"])
    df = df[df["user_id"].str.strip().astype(bool)]
    df = df[df["action_type"].str.strip().astype(bool)]
    removed = initial_count - len(df)
    if removed:
        logger.warning("Removed %d rows with missing user_id or action_type", removed)

    # ------------------------------------------------------------------
    # 2. Convert timestamp to uniform ISO 8601 datetime
    # ------------------------------------------------------------------
    df["event_timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df = df.drop(columns=["timestamp"])

    # ------------------------------------------------------------------
    # 3. Flatten metadata → device, location
    # ------------------------------------------------------------------
    metadata_df = pd.json_normalize(df["metadata"].apply(lambda x: x if isinstance(x, dict) else {}))
    metadata_df.index = df.index  # align indexes
    df["device"] = metadata_df.get("device", pd.Series(dtype="str"))
    df["location"] = metadata_df.get("location", pd.Series(dtype="str"))
    df = df.drop(columns=["metadata"])

    # ------------------------------------------------------------------
    # 4. Deduplicate exact duplicates
    # ------------------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates()
    dupes_removed = before_dedup - len(df)
    if dupes_removed:
        logger.warning("Removed %d duplicate rows", dupes_removed)

    logger.info("Transformation complete: %d clean records", len(df))
    return df.reset_index(drop=True)


def build_dim_users(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique users for dim_users."""
    dim = df[["user_id"]].drop_duplicates().reset_index(drop=True)
    dim.index.name = "user_key"
    return dim


def build_dim_actions(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique action types for dim_actions."""
    dim = df[["action_type"]].drop_duplicates().reset_index(drop=True)
    dim.index.name = "action_key"
    return dim


def build_fact_table(
    df: pd.DataFrame,
    dim_users: pd.DataFrame,
    dim_actions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the fact table by replacing natural keys with surrogate keys
    from dimension tables.
    """
    # Create lookup dicts  (natural key → surrogate key)
    user_lookup = {row.user_id: idx + 1 for idx, row in dim_users.iterrows()}
    action_lookup = {row.action_type: idx + 1 for idx, row in dim_actions.iterrows()}

    fact = df.copy()
    fact["user_key"] = fact["user_id"].map(user_lookup)
    fact["action_key"] = fact["action_type"].map(action_lookup)
    fact = fact[["user_key", "action_key", "event_timestamp", "device", "location"]]
    return fact.reset_index(drop=True)
