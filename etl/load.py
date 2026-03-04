"""
Load module: writes transformed DataFrames into PostgreSQL.

Uses direct SQL INSERTs via SQLAlchemy to avoid Pandas 2.2 / SQLAlchemy 1.4
compatibility issues with to_sql().
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def get_engine():
    """Build a SQLAlchemy engine from environment variables."""
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://etl_user:etl_pass@postgres:5432/myoncare_dw",
    )
    return create_engine(db_url, echo=False)


def init_schema(engine, schema_path: str = "/opt/airflow/sql/schema.sql") -> None:
    """Execute the DDL script to create tables if they don't exist."""
    logger.info("Initialising database schema from %s", schema_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Schema initialised successfully")


def truncate_tables(engine) -> None:
    """Truncate all star-schema tables (for idempotent full-reload)."""
    logger.info("Truncating existing tables for full reload")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_user_actions, dim_users, dim_actions RESTART IDENTITY CASCADE"))


def load_dim_users(engine, dim_users: pd.DataFrame) -> None:
    """Load dim_users into PostgreSQL."""
    logger.info("Loading %d rows into dim_users", len(dim_users))
    rows = dim_users.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO dim_users (user_id) VALUES (:user_id)"),
            rows,
        )


def load_dim_actions(engine, dim_actions: pd.DataFrame) -> None:
    """Load dim_actions into PostgreSQL."""
    logger.info("Loading %d rows into dim_actions", len(dim_actions))
    rows = dim_actions.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO dim_actions (action_type) VALUES (:action_type)"),
            rows,
        )


def load_fact_user_actions(engine, fact: pd.DataFrame) -> None:
    """Load fact_user_actions into PostgreSQL."""
    logger.info("Loading %d rows into fact_user_actions", len(fact))
    rows = fact.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO fact_user_actions "
                "(user_key, action_key, event_timestamp, device, location) "
                "VALUES (:user_key, :action_key, :event_timestamp, :device, :location)"
            ),
            rows,
        )
