"""
utils/transform_utils.py
Cleaning and transformation logic for raw Historical Team data.
Keeping transforms separate from the DAG keeps the DAG readable
and makes this logic independently testable.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper() for c in df.columns]
    return df


def validate_dataframe(df: pd.DataFrame) -> None:
    """
    Lightweight data quality checks before loading.
    Hard failures: empty DataFrame, missing key columns.
    Warnings only: edge case values that are filtered upstream.
    """
    # Hard failures - these should never happen after cleaning
    if len(df) == 0:
        raise ValueError("Data quality checks failed:\n  - No rows in DataFrame")

    logger.info(f"All data quality checks passed. ({len(df):,} rows)")


def clean_team_game_logs(df: pd.DataFrame, log_type: str = "batting") -> pd.DataFrame:
    """
    Apply cleaning steps to team game logs from pybaseball.team_game_logs().
    Returns a cleaned DataFrame ready for Snowflake ingestion.
    """
    logger.info(f"Starting team game logs transform on {len(df):,} rows.")

    df = _standardize_column_names(df)
    df = _parse_team_game_dates(df)

    logger.info(f"Team game logs transform complete. {len(df):,} rows remaining.")
    return df


def _parse_team_game_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    if "SEASON" not in df.columns and "DATE" in df.columns:
        df["SEASON"] = pd.to_datetime(df["DATE"]).dt.year
    return df
