import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.statcast_pipeline import (
    get_yesterdays_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    compute_rolling_features,
    check_todays_lineups,
)

if __name__ == "__main__":
    print("Starting daily MLB statcast pipeline")

    player_info = get_yesterdays_players()

    batter_rows = fetch_and_load_batter_stats(player_info)
    pitcher_rows = fetch_and_load_pitcher_stats(player_info)
    update_game_results()
    compute_rolling_features(batter_rows, pitcher_rows, player_info["date"])

    print("Daily pipeline complete")
