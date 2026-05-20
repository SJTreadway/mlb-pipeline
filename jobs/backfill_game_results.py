import os
import sys
import time
import logging
import pandas as pd
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import statsapi

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobs.statcast_pipeline import _get_snowflake_conn, _bulk_insert_snowflake

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "BASEBALL")
SCHEMA = "STATCAST"
START_YEAR = int(os.environ.get("START_YEAR", 2015))
END_YEAR = int(os.environ.get("END_YEAR", 2026))


def fetch_game_results_for_date(game_date: str) -> list:
    """Fetch game results for a specific date from MLB Stats API."""
    try:
        schedule = statsapi.schedule(date=game_date)
        rows = []
        for game in schedule:
            if game.get("status") != "Final":
                continue
            rows.append(
                {
                    "game_pk": game["game_id"],
                    "game_date": game_date,
                    "team_h": game["home_id"],
                    "team_v": game["away_id"],
                    "runs_h": game["home_score"],
                    "runs_v": game["away_score"],
                    "home_victory": 1 if game["home_score"] > game["away_score"] else 0,
                    "run_diff": game["home_score"] - game["away_score"],
                }
            )
        return rows
    except Exception as e:
        log.warning(f"Error fetching {game_date}: {e}")
        return []


def get_season_dates(year: int) -> list:
    """Get all dates in a season."""
    start = date(year, 3, 20)  # spring training starts ~March 20
    end = date(year, 11, 5)  # latest possible World Series end
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def backfill_year(year: int, conn) -> int:
    """Backfill game results for a full season."""
    dates = get_season_dates(year)
    all_rows = []

    log.info(f"{year}: fetching {len(dates)} dates")

    def fetch(d):
        rows = fetch_game_results_for_date(d)
        time.sleep(0.1)
        return rows

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch, d): d for d in dates}
        for future in as_completed(futures):
            rows = future.result()
            all_rows.extend(rows)

    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates(subset=["game_pk"])
        _bulk_insert_snowflake(conn, df, "GAME_RESULTS")
        log.info(f"{year}: inserted {len(df)} games")
        return len(df)
    return 0


if __name__ == "__main__":
    conn = _get_snowflake_conn()
    total = 0
    for year in range(START_YEAR, END_YEAR + 1):
        rows = backfill_year(year, conn)
        total += rows
        log.info(f"Year {year} complete — {rows} games")
    conn.close()
    log.info(f"Total: {total} games inserted")
