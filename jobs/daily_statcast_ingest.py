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
    fetch_and_load_pitch_stats,
    update_bvp_history,
    compute_rolling_features,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

REFRESH_DATA = os.environ.get("REFRESH_DATA", "false") == "true"


if __name__ == "__main__":
    log.info("Starting daily MLB statcast pipeline")
    player_info = get_yesterdays_players()
    if REFRESH_DATA:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(fetch_and_load_batter_stats, player_info): "batters",
                executor.submit(fetch_and_load_pitcher_stats, player_info): "pitchers",
                executor.submit(fetch_and_load_pitch_stats, player_info): "pitches",
                executor.submit(update_game_results): "game_results",
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                    log.info(f"{name} complete")
                except Exception as e:
                    log.warning(f"{name} failed: {e}")

        # must run after pitch stats are loaded
        update_bvp_history()  # RAW_PITCHES → BVP_HISTORY

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
