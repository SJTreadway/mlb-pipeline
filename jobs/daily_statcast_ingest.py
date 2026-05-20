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
    if todays_players["batter_ids"] or todays_players["pitcher_ids"]:
        compute_rolling_features(
            batter_ids=todays_players["batter_ids"],
            pitcher_ids=todays_players["pitcher_ids"],
            game_date=player_info["date"],
        )
    else:
        log.info("No confirmed lineups yet — skipping rolling features")
    log.info("Daily pipeline complete")
