"""
historical_retrosheet_events_backfill.py
=========================================
One-off DAG for loading historical events data from Retrosheet va pybaseball.
Trigger from the Airflow UI to backfill season event data.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from utils.snowflake_utils import load_dataframe
from utils.retrosheet import get_season_game_logs

logger = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = "BASEBALL"
SNOWFLAKE_SCHEMA = "HISTORICAL"
TARGET_TABLE = "RETROSHEET_EVENTS"

# separate from bref since retrosheet doesn't have 2024+ data
SEASONS = list[int](range(1980, 2024))

@dag(
    dag_id="historical_retrosheet_events_backfill",
    description="Backfill historical events data from Retrosheet",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={"owner": "steven.treadway", "retries": 2, "retry_delay": timedelta(minutes=10)},
    max_active_tasks=4,
    tags=["baseball", "historical", "retrosheet", "events", "backfill"],
    params={
        "seasons": Param(SEASONS, type="array", description="List of seasons to backfill")
    },
)
def historical_retrosheet_events_backfill():

    @task
    def get_seasons(**context) -> list[int]:
        params = context.get("params", {})
        return params.get("seasons", SEASONS)

    @task
    def extract_retrosheet_events_by_season(season: int) -> dict:
        """Extract Retrosheet events for a single season."""
        logger.info(f"Extracting Retrosheet events data for the {season} season")

        df = get_season_game_logs(season)

        if df is None or df.empty:
            logger.warning(f"No Retrosheet events data for season: {season}")
            return {"season": season, "data": None, "row_count": 0}

        return {
            "season": season,
            "data": df.to_json(orient="split", date_format="iso"),
            "row_count": len(df),
        }

    @task
    def load_retrosheet_events(extract_result: dict) -> int:
        """Load one season's retrosheet events data into Snowflake."""
        if not extract_result.get("data"):
            return 0

        df = pd.read_json(extract_result["data"], orient="split")

        rows = load_dataframe(
            df=df,
            table=TARGET_TABLE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
        )
        return rows

    @task
    def summarize(row_counts: list[int]) -> None:
        total = sum(row_counts)
        logger.info(f"Retrosheet event data backfill complete. Total rows loaded: {total:,}")

    seasons = get_seasons()
    extracted = extract_retrosheet_events_by_season.expand(season=seasons)
    row_counts = load_retrosheet_events.expand(extract_result=extracted)
    summarize(row_counts)


historical_retrosheet_events_backfill()
