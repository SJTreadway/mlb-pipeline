import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import (
    get_yesterdays_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    compute_rolling_features,
    check_todays_lineups,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("Starting daily MLB statcast pipeline")

    player_info = get_yesterdays_players()

    batter_rows = fetch_and_load_batter_stats(player_info)
    pitcher_rows = fetch_and_load_pitcher_stats(player_info)
    update_game_results()
    compute_rolling_features(batter_rows, pitcher_rows, player_info["date"])

    log.info("Daily pipeline complete")
