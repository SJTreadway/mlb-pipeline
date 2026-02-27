"""
historical_team_pitching_logs_backfill.py
==========================================
One-off DAG for loading historical team pitching logs data.
Trigger from the Airflow UI to backfill team pitching data.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from utils.snowflake_utils import load_dataframe
from utils.historical_team_utils import get_game_data_by_team, TEAMS

logger = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = "BASEBALL"
SNOWFLAKE_SCHEMA = "HISTORICAL"
TARGET_TABLE = "TEAM_PITCHING_LOGS"


@dag(
    dag_id="historical_team_pitching_logs_backfill",
    description="Backfill historical team pitching logs data",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={"owner": "steven.treadway", "retries": 2, "retry_delay": timedelta(minutes=10)},
    max_active_tasks=4,
    tags=["baseball", "historical", "pitching", "backfill"],
    #params={
    #    "teams": Param(TEAMS, type="array", description="List of team abbreviations to backfill"),
    #},
)
def historical_team_pitching_logs_backfill():

    '''
    @task
    def get_teams(**context) -> list[str]:
        params = context.get("params", {})
        return params.get("teams", TEAMS)
    '''

    @task
    def extract_team_pitching(team: str) -> dict:
        """Extract pitching game logs for a single team."""
        logger.info(f"Extracting pitching data for team: {team}")

        df = get_game_data_by_team(team, "pitching")

        if df is None or df.empty:
            logger.warning(f"No pitching data for team: {team}")
            return {"team": team, "data": None, "row_count": 0}

        return {
            "team": team,
            "data": df.to_json(orient="split", date_format="iso"),
            "row_count": len(df),
        }

    @task
    def load_team_pitching(extract_result: dict) -> int:
        """Load one team's pitching data into Snowflake."""
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
        logger.info(f"Pitching logs backfill complete. Total rows loaded: {total:,}")

    #teams = get_teams()
    extracted = extract_team_pitching.expand(team=TEAMS)
    row_counts = load_team_pitching.expand(extract_result=extracted)
    summarize(row_counts)


historical_team_pitching_logs_backfill()
