"""
Data quality checks executed after loading.
"""

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a data quality check fails."""


def check_no_null_keys(engine) -> None:
    """Ensure there are no NULL surrogate keys in the fact table."""
    logger.info("Quality check: no NULL keys in fact_user_actions")
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM fact_user_actions "
                "WHERE user_key IS NULL OR action_key IS NULL"
            )
        )
        null_count = result.scalar()
    if null_count > 0:
        raise DataQualityError(
            f"Found {null_count} rows with NULL surrogate keys in fact_user_actions"
        )
    logger.info("  ✓ No NULL keys found")


def check_referential_integrity(engine) -> None:
    """Ensure every foreign key in fact_user_actions points to a valid dimension row."""
    logger.info("Quality check: referential integrity")

    queries = {
        "user_key": (
            "SELECT COUNT(*) FROM fact_user_actions f "
            "LEFT JOIN dim_users d ON f.user_key = d.user_key "
            "WHERE d.user_key IS NULL"
        ),
        "action_key": (
            "SELECT COUNT(*) FROM fact_user_actions f "
            "LEFT JOIN dim_actions d ON f.action_key = d.action_key "
            "WHERE d.action_key IS NULL"
        ),
    }

    for key, sql in queries.items():
        with engine.connect() as conn:
            orphan_count = conn.execute(text(sql)).scalar()
        if orphan_count > 0:
            raise DataQualityError(
                f"Found {orphan_count} orphan rows for {key} in fact_user_actions"
            )
        logger.info("  ✓ Referential integrity OK for %s", key)


def check_no_duplicate_facts(engine) -> None:
    """Ensure there are no exact duplicate rows in the fact table."""
    logger.info("Quality check: no duplicate fact rows")
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM ("
                "  SELECT user_key, action_key, event_timestamp, device, location, "
                "         COUNT(*) AS cnt "
                "  FROM fact_user_actions "
                "  GROUP BY user_key, action_key, event_timestamp, device, location "
                "  HAVING COUNT(*) > 1"
                ") dupes"
            )
        )
        dupe_count = result.scalar()
    if dupe_count > 0:
        raise DataQualityError(
            f"Found {dupe_count} duplicate groups in fact_user_actions"
        )
    logger.info("  ✓ No duplicate fact rows found")


def check_row_counts(engine) -> None:
    """Log row counts for all star-schema tables and ensure they are non-zero."""
    logger.info("Quality check: row counts")
    tables = ["dim_users", "dim_actions", "fact_user_actions"]
    for table in tables:
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        if count == 0:
            raise DataQualityError(f"Table {table} is empty after load")
        logger.info("  ✓ %s: %d rows", table, count)


def run_all_checks(engine) -> None:
    """Execute every data quality check."""
    logger.info("=" * 50)
    logger.info("Running data quality checks …")
    logger.info("=" * 50)
    check_no_null_keys(engine)
    check_referential_integrity(engine)
    check_no_duplicate_facts(engine)
    check_row_counts(engine)
    logger.info("=" * 50)
    logger.info("All data quality checks PASSED ✓")
    logger.info("=" * 50)
