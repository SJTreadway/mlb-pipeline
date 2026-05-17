import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.decorators import dag, task
from datetime import datetime, timedelta

from jobs.statcast_pipeline import (
    get_yesterdays_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    compute_rolling_features,
)

default_args = {
    "owner": "moneyballvo",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}


@dag(
    dag_id="daily_statcast_features",
    default_args=default_args,
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlb", "statcast", "daily"],
)
def daily_statcast_features():

    @task()
    def run_get_players(**context):
        return get_yesterdays_players()

    @task()
    def run_batter_stats(player_info: dict) -> int:
        return fetch_and_load_batter_stats(player_info)

    @task()
    def run_pitcher_stats(player_info: dict) -> int:
        return fetch_and_load_pitcher_stats(player_info)

    @task()
    def run_game_results(**context) -> int:
        return update_game_results()

    @task()
    def run_rolling_features(batter_rows: int, pitcher_rows: int) -> str:
        return compute_rolling_features(batter_rows, pitcher_rows)

    player_info = run_get_players()
    batter_rows = run_batter_stats(player_info)
    pitcher_rows = run_pitcher_stats(player_info)
    run_game_results()
    run_rolling_features(batter_rows, pitcher_rows)


daily_statcast_features()
