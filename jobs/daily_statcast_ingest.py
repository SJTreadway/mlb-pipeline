import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobs.statcast_pipeline import (
    get_yesterdays_players,
    fetch_and_load_batter_stats,
    fetch_and_load_pitcher_stats,
    update_game_results,
    fetch_and_load_pitch_stats,
    update_bvp_history,
    run_daily_ingest,
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
        update_bvp_history()

    # run_daily_ingest handles lineup fetch, checkpoint comparison,
    # rolling feature compute, and checkpoint write all in one shot
    run_daily_ingest()

    log.info("Daily pipeline complete")
