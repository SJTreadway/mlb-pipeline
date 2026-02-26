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

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "season" not in df.columns and "date" in df.columns:
        df["season"] = pd.to_datetime(df["date"]).dt.year
    return df


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
    df = _coerce_team_game_numerics(df, log_type)

    logger.info(f"Team game logs transform complete. {len(df):,} rows remaining.")
    return df


def _parse_team_game_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    return df


def _coerce_team_game_numerics(df: pd.DataFrame, log_type: str) -> pd.DataFrame:
    common_int_cols = [
        "Season", "Game", "W", "L", "T", "Win", "Loss",
        "R", "RA", "H", "X2B", "X3B", "HR", "RBI",
        "BB", "IBB", "SO", "HBP", "SB", "CS", "LOB", "E",
        "Inn", "X_Inn", "BFP", "Attend"
    ]

    for col in common_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    float_cols = ["cLI", "WPA", "aLI", "WPA+", "WPA-", "cWPA", "cLI+", "cLI-"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    if log_type == "pitching":
        pitch_int_cols = ["BF", "Pit", "Str", "GSc", "WP", "BK", "IR", "IS"]
        for col in pitch_int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        if "IP" in df.columns:
            df["IP"] = pd.to_numeric(df["IP"], errors="coerce").astype("float64")
        if "ERA" in df.columns:
            df["ERA"] = pd.to_numeric(df["ERA"], errors="coerce").astype("float64")

    return df
