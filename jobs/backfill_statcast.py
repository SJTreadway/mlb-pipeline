"""
Backfill historical Statcast data (2015-2026) into Snowflake.

Run on EC2 inside a tmux session:
    tmux new -s backfill
    python3.11 jobs/backfill_statcast.py
    # detach with Ctrl+B D

Flags
-----
    --batters-only      skip pitcher backfill
    --pitchers-only     skip batter backfill
    --reset-pitchers    clear pitcher checkpoint and re-run all pitchers
                        ← use this to backfill H, BB, SO, ER, X2B, X3B
    --reset-batters     clear batter checkpoint and re-run all batters
    --year YYYY         only backfill a single year

Resume after interruption — automatically skips already-loaded player/year combos.
"""

import argparse
import os
import sys
import time
import json
import logging
from datetime import date

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jobs.statcast_pipeline import (
    _get_snowflake_conn,
    _bulk_insert_snowflake,
    _upsert_to_snowflake,
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
START_YEAR = int(os.environ.get("START_YEAR", 2015))
END_YEAR = int(os.environ.get("END_YEAR", 2026))
MIN_PA = 50
MIN_GS = 5
API_SLEEP = 0.2
MAX_WORKERS = 8
CHECKPOINT_FILE = "backfill_checkpoint.json"


# ── Checkpoint ────────────────────────────────────────────────────────────────


def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"completed_batters": [], "completed_pitchers": []}


def save_checkpoint(checkpoint: dict) -> None:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)


# ── Player lists ──────────────────────────────────────────────────────────────


def get_qualified_batters(year: int, min_pa: int = MIN_PA) -> list:
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


def get_qualified_pitchers(year: int, min_gs: int = MIN_GS) -> list:
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


# ── Worker functions ──────────────────────────────────────────────────────────


def _fetch_and_load_batter(args: tuple) -> tuple[str, int]:
    mlbam_id, year, completed = args
    key = f"{mlbam_id}_{year}"
    if key in completed:
        return key, 0
    try:
        start = f"{year}-03-01"
        end = (
            f"{year}-11-30"
            if year < date.today().year
            else date.today().strftime("%Y-%m-%d")
        )
        df = statcast_batter(start, end, int(mlbam_id))
        if df is None or df.empty:
            return key, 0
        game_df = _transform_batter_game(df)
        if game_df.empty:
            return key, 0
        conn = _get_snowflake_conn()
        try:
            _bulk_insert_snowflake(conn, game_df, "RAW_BATTER_GAMES")
        finally:
            conn.close()
        time.sleep(API_SLEEP)
        return key, len(game_df)
    except Exception as e:
        log.warning(f"Error fetching batter {mlbam_id} ({year}): {e}")
        return key, 0


def _fetch_and_load_pitcher(args: tuple) -> tuple[str, int]:
    """Fetch pitcher game logs and bulk insert to RAW_PITCHER_GAMES.

    Fast path — uses bulk insert, not upsert. Table should be truncated
    before the backfill run when re-fetching existing data (--reset-pitchers
    handles this automatically in main).
    """
    mlbam_id, year, completed = args
    key = f"{mlbam_id}_{year}"
    if key in completed:
        return key, 0
    try:
        start = f"{year}-03-01"
        end = (
            f"{year}-11-30"
            if year < date.today().year
            else date.today().strftime("%Y-%m-%d")
        )
        df = statcast_pitcher(start, end, int(mlbam_id))
        if df is None or df.empty:
            return key, 0
        game_df = _transform_pitcher_game(df)
        if game_df.empty:
            return key, 0
        conn = _get_snowflake_conn()
        try:
            _bulk_insert_snowflake(conn, game_df, "RAW_PITCHER_GAMES")
        finally:
            conn.close()
        time.sleep(API_SLEEP)
        return key, len(game_df)
    except Exception as e:
        log.warning(f"Error fetching pitcher {mlbam_id} ({year}): {e}")
        return key, 0


# ── Backfill runners ──────────────────────────────────────────────────────────


def backfill_batters(checkpoint: dict, year_filter: int | None = None) -> int:
    completed = set(checkpoint["completed_batters"])
    total_rows = 0
    years = [year_filter] if year_filter else range(START_YEAR, END_YEAR + 1)

    for year in years:
        batter_ids = get_qualified_batters(year)
        remaining = [bid for bid in batter_ids if f"{bid}_{year}" not in completed]
        log.info(f"{year}: {len(remaining)} batters to backfill")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_and_load_batter, (bid, year, completed)): bid
                for bid in remaining
            }
            for i, future in enumerate(
                tqdm(as_completed(futures), total=len(futures), desc=f"{year} batters"),
                1,
            ):
                key, rows = future.result()
                completed.add(key)
                total_rows += rows
                if i % 50 == 0:
                    checkpoint["completed_batters"] = list(completed)
                    save_checkpoint(checkpoint)
                    log.info(
                        f"{year}: {i}/{len(remaining)} done — {total_rows} rows total"
                    )

        checkpoint["completed_batters"] = list(completed)
        save_checkpoint(checkpoint)
        log.info(f"Year {year} complete — {total_rows} total rows")

    return total_rows


def backfill_pitchers(checkpoint: dict, year_filter: int | None = None) -> int:
    completed = set(checkpoint["completed_pitchers"])
    total_rows = 0
    years = [year_filter] if year_filter else range(START_YEAR, END_YEAR + 1)

    for year in years:
        pitcher_ids = get_qualified_pitchers(year)
        remaining = [pid for pid in pitcher_ids if f"{pid}_{year}" not in completed]
        log.info(f"{year}: {len(remaining)} pitchers to backfill")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_and_load_pitcher, (pid, year, completed)): pid
                for pid in remaining
            }
            for i, future in enumerate(
                tqdm(
                    as_completed(futures), total=len(futures), desc=f"{year} pitchers"
                ),
                1,
            ):
                key, rows = future.result()
                completed.add(key)
                total_rows += rows
                if i % 50 == 0:
                    checkpoint["completed_pitchers"] = list(completed)
                    save_checkpoint(checkpoint)
                    log.info(
                        f"{year}: {i}/{len(remaining)} done — {total_rows} rows total"
                    )

        checkpoint["completed_pitchers"] = list(completed)
        save_checkpoint(checkpoint)
        log.info(f"Year {year} complete — {total_rows} total rows")

    return total_rows


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description="Backfill Statcast data into Snowflake")
    p.add_argument("--batters-only", action="store_true")
    p.add_argument("--pitchers-only", action="store_true")
    p.add_argument(
        "--reset-pitchers",
        action="store_true",
        help="Clear pitcher checkpoint and re-fetch all pitchers (use after schema changes)",
    )
    p.add_argument(
        "--reset-batters",
        action="store_true",
        help="Clear batter checkpoint and re-fetch all batters",
    )
    p.add_argument("--year", type=int, default=None, help="Backfill a single year only")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    checkpoint = load_checkpoint()

    if args.reset_pitchers:
        log.info("Resetting pitcher checkpoint — all pitchers will be re-fetched")
        checkpoint["completed_pitchers"] = []
        save_checkpoint(checkpoint)
        log.info("Truncating RAW_PITCHER_GAMES for clean bulk insert …")
        conn = _get_snowflake_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.RAW_PITCHER_GAMES")
            conn.commit()
            cursor.close()
            log.info("RAW_PITCHER_GAMES truncated")
        finally:
            conn.close()

    if args.reset_batters:
        log.info("Resetting batter checkpoint — all batters will be re-fetched")
        checkpoint["completed_batters"] = []
        save_checkpoint(checkpoint)

    log.info(
        f"Starting backfill {START_YEAR}–{END_YEAR}"
        + (f" (year={args.year})" if args.year else "")
    )

    if not args.pitchers_only:
        log.info("--- Backfilling batters ---")
        batter_rows = backfill_batters(checkpoint, year_filter=args.year)
        log.info(f"Batter backfill complete — {batter_rows} total rows")

    if not args.batters_only:
        log.info("--- Backfilling pitchers ---")
        pitcher_rows = backfill_pitchers(checkpoint, year_filter=args.year)
        log.info(f"Pitcher backfill complete — {pitcher_rows} total rows")

    log.info("Backfill complete!")
