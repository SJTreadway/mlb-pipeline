# jobs/daily_statcast_ingest.py
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta, timezone
from dags.daily_statcast_features import (
    get_yesterdays_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    compute_rolling_features,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("Starting daily MLB pipeline")

    player_info = get_yesterdays_players()
    batter_rows = fetch_and_load_batter_stats(player_info)
    pitcher_rows = fetch_and_load_pitcher_stats(player_info)
    game_results = update_game_results()
    compute_rolling_features(batter_rows, pitcher_rows)

    log.info("Daily pipeline complete")
