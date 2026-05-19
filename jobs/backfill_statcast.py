"""
Backfill historical Statcast data (2015-2025) into Snowflake.

Run on EC2 inside a tmux session:
    tmux new -s backfill
    python3.11 jobs/backfill_statcast.py
    # detach with Ctrl+B D

Resume after interruption — automatically skips already-loaded player/year combos.
"""

import os
import sys
import time
import json
import logging
import pickle
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

import requests
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobs.statcast_pipeline import (
    _get_snowflake_conn,
    _bulk_insert_snowflake,
    _transform_batter_game,
    _transform_pitcher_game,
    DATABASE,
    SCHEMA,
)

from pybaseball import statcast_batter, statcast_pitcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("backfill.log"),
    ],
)
log = logging.getLogger(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"
START_YEAR = int(os.environ.get('START_YEAR', 2015))
END_YEAR   = int(os.environ.get('END_YEAR', 2025))
MIN_PA = 50  # minimum plate appearances to qualify
MIN_GS = 5  # minimum games started for pitchers
API_SLEEP = 0.2  # seconds between API calls
MAX_WORKERS = 8

CHECKPOINT_FILE = "backfill_checkpoint.json"M


# ── Checkpoint ────────────────────────────────────────────────────────────────


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"completed_batters": [], "completed_pitchers": []}


def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)


# ── Player lists ──────────────────────────────────────────────────────────────

def _fetch_and_load_batter(args):
    mlbam_id, year, completed = args
    key = f'{mlbam_id}_{year}'
    if key in completed:
        return key, 0
    try:
        start = f'{year}-03-01'
        end   = f'{year}-11-30'
        df    = statcast_batter(start, end, int(mlbam_id))
        if df is None or df.empty:
            return key, 0
        game_df = _transform_batter_game(df)
        conn = _get_snowflake_conn()  # create per thread
        if not game_df.empty:
            _bulk_insert_snowflake(conn, game_df, 'RAW_BATTER_GAMES')
        time.sleep(API_SLEEP)
        return key, len(game_df) if not game_df.empty else 0
    except Exception as e:
        log.warning(f'Error fetching batter {mlbam_id} ({year}): {e}')
        return key, 0


def get_qualified_batters(year, min_pa=MIN_PA):
    """Get MLBAM IDs for batters with enough PA in a given year."""
    try:
        resp = requests.get(
            f"{MLB_API}/stats",
            params={
                "stats": "season",
                "group": "hitting",
                "season": year,
                "playerPool": "All",
                "limit": 2000,
            },
            timeout=15,
        )
        resp.raise_for_status()
        splits = resp.json()["stats"][0]["splits"]
        ids = [
            s["player"]["id"]
            for s in splits
            if s.get("stat", {}).get("plateAppearances", 0) >= min_pa
        ]
        log.info(f"{year}: {len(ids)} qualified batters (min {min_pa} PA)")
        return ids
    except Exception as e:
        log.error(f"Error fetching batters for {year}: {e}")
        return []
      
def _fetch_and_load_pitcher(args):
    mlbam_id, year, completed = args
    key = f'{mlbam_id}_{year}'
    if key in completed:
        return key, 0
    try:
        start = f'{year}-03-01'
        end   = f'{year}-11-30'
        df    = statcast_pitcher(start, end, int(mlbam_id))
        if df is None or df.empty:
            return key, 0
        game_df = _transform_pitcher_game(df)
        conn = _get_snowflake_conn()
        try:
            if not game_df.empty:
                _bulk_insert_snowflake(conn, game_df, 'RAW_PITCHER_GAMES')
        finally:
            conn.close()
        time.sleep(API_SLEEP)
        return key, len(game_df) if not game_df.empty else 0
    except Exception as e:
        log.warning(f'Error fetching pitcher {mlbam_id} ({year}): {e}')
        return key, 0


def get_qualified_pitchers(year, min_gs=MIN_GS):
    """Get MLBAM IDs for pitchers with enough starts in a given year."""
    try:
        resp = requests.get(
            f"{MLB_API}/stats",
            params={
                "stats": "season",
                "group": "pitching",
                "season": year,
                "playerPool": "All",
                "limit": 2000,
            },
            timeout=15,
        )
        resp.raise_for_status()
        splits = resp.json()["stats"][0]["splits"]
        ids = [
            s["player"]["id"]
            for s in splits
            if s.get("stat", {}).get("gamesStarted", 0) >= min_gs
        ]
        log.info(f"{year}: {len(ids)} qualified pitchers (min {min_gs} GS)")
        return ids
    except Exception as e:
        log.error(f"Error fetching pitchers for {year}: {e}")
        return []


# ── Backfill functions ────────────────────────────────────────────────────────


def backfill_batters(checkpoint):
    completed  = set(checkpoint['completed_batters'])
    total_rows = 0

    for year in range(START_YEAR, END_YEAR + 1):
        batter_ids = get_qualified_batters(year)
        remaining  = [bid for bid in batter_ids if f'{bid}_{year}' not in completed]
        log.info(f'{year}: {len(remaining)} batters to backfill')

        args = [(bid, year, completed) for bid in remaining]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_and_load_batter, a): a for a in args}
            for i, future in enumerate(tqdm(
                as_completed(futures), total=len(futures), desc=f'{year} batters'
            ), 1):
                key, rows = future.result()
                completed.add(key)
                total_rows += rows

                if i % 50 == 0:
                    checkpoint['completed_batters'] = list(completed)
                    save_checkpoint(checkpoint)
                    log.info(f'{year}: {i}/{len(remaining)} done — {total_rows} rows total')

        checkpoint['completed_batters'] = list(completed)
        save_checkpoint(checkpoint)
        log.info(f'Year {year} complete — {total_rows} total rows')

    return total_rows

def backfill_pitchers(checkpoint):
    completed  = set(checkpoint['completed_pitchers'])
    total_rows = 0

    for year in range(START_YEAR, END_YEAR + 1):
        pitcher_ids = get_qualified_pitchers(year)
        remaining   = [pid for pid in pitcher_ids if f'{pid}_{year}' not in completed]
        log.info(f'{year}: {len(remaining)} pitchers to backfill')

        args = [(pid, year, completed) for pid in remaining]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_and_load_pitcher, a): a for a in args}
            for i, future in enumerate(tqdm(
                as_completed(futures), total=len(futures), desc=f'{year} pitchers'
            ), 1):
                key, rows = future.result()
                completed.add(key)
                total_rows += rows

                if i % 50 == 0:
                    checkpoint['completed_pitchers'] = list(completed)
                    save_checkpoint(checkpoint)
                    log.info(f'{year}: {i}/{len(remaining)} done — {total_rows} rows total')

        checkpoint['completed_pitchers'] = list(completed)
        save_checkpoint(checkpoint)
        log.info(f'Year {year} complete — {total_rows} total rows')

    return total_rows

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"Starting backfill {START_YEAR}-{END_YEAR}")
    checkpoint = load_checkpoint()
    conn = _get_snowflake_conn()

    log.info("--- Backfilling batters ---")
    batter_rows = backfill_batters(checkpoint)
    log.info(f"Batter backfill complete — {batter_rows} total rows")

    log.info("--- Backfilling pitchers ---")
    pitcher_rows = backfill_pitchers(checkpoint)
    log.info(f"Pitcher backfill complete — {pitcher_rows} total rows")

    conn.close()
    log.info("Backfill complete!")
