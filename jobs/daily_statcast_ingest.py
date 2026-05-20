import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import (
    get_yesterdays_players,
    get_todays_lineup_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    compute_rolling_features,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("Starting daily MLB statcast pipeline")
    player_info = get_yesterdays_players()
    batter_rows = fetch_and_load_batter_stats(player_info)
    pitcher_rows = fetch_and_load_pitcher_stats(player_info)
    update_game_results()

    # only compute rolling features for today's confirmed lineup players
    todays_players = get_todays_lineup_players()
    compute_rolling_features(
        todays_players["batter_ids"], todays_players["pitcher_ids"], player_info["date"]
    )
    log.info("Daily pipeline complete")
