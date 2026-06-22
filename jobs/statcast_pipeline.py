import hashlib
import os
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

import requests
from pybaseball import statcast_batter, statcast_pitcher, statcast
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "BASEBALL")
SCHEMA = "STATCAST"

NON_AB_EVENTS = [
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "sac_bunt",
    "sac_fly",
    "sac_fly_error",
    "catcher_interf",
]

WINDOWS_BAT = [7, 14, 30, 75, 162, 350]
WINDOWS_PITCH = [10, 35, 75]

# How far back (in days) to pull raw game rows when computing rolling
# features for a batch of players. Must be >= the longest rolling window
# (350 games for batters) translated into a safe calendar-day equivalent —
# NOT simply equal to the longest window in GAMES, since games-per-day
# varies (off-days, doubleheaders). 365 days (one full season-equivalent,
# plus slack for a 162-game season spread across ~186 calendar days) safely
# covers WINDOWS_BAT's max of 350 GAMES for any real player, since no
# player accumulates 350 games in fewer than ~365 calendar days even in a
# theoretical every-day-a-doubleheader scenario for actual MLB schedules.
# Reduced from 400 -> 365 to cut redundant raw-row fetch/compute volume on
# every incremental run while keeping a safety margin above the actual need.
ROLLING_LOOKBACK_DAYS = 365

# Pitcher smoothing defaults (mirror pitchers.py)
IP_PER_GAME_DEF = 3
BF_PER_GAME_DEF = 12
H_BB_PER_IP_DEF = 1.5
H_BB_PER_BF_DEF = 0.37
SO_PER_BF_DEF = 0.2
TB_BB_PERC_DEF = 0.45
ER_PER_IP_DEF = 5 / 9
FIP_PER_IP_DEF = 0.124 * 13 + 1.5 * 3 - 2 * 0.8
FIP_PER_BF_DEF = 0.03 * 13 + 0.37 * 3 - 2 * 0.2


# ── Snowflake ─────────────────────────────────────────────────────────────────


def _get_snowflake_conn():
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        return SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
    except Exception:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        private_key_str = os.environ["SNOWFLAKE_PRIVATE_KEY"]
        private_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=None,
            backend=default_backend(),
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key=private_key_bytes,
            database=DATABASE,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            schema=SCHEMA,
        )


def _get_table_columns(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {DATABASE}.{SCHEMA}.{table} LIMIT 0")
    cols = [desc[0].lower() for desc in cursor.description]
    cursor.close()
    return cols


def _add_missing_columns(conn, df, table):
    type_map = {
        "int64": "INTEGER",
        "float64": "FLOAT",
        "object": "VARCHAR(50)",
        "bool": "BOOLEAN",
        "datetime64[ns]": "DATE",
    }
    table_cols = _get_table_columns(conn, table)
    cursor = conn.cursor()
    for col in df.columns:
        if col.lower() not in table_cols:
            dtype = type_map.get(str(df[col].dtype), "VARCHAR(50)")
            try:
                cursor.execute(
                    f"ALTER TABLE {DATABASE}.{SCHEMA}.{table} ADD COLUMN {col} {dtype}"
                )
                log.info(f"Added column {col} ({dtype}) to {table}")
            except Exception as e:
                log.warning(f"Could not add column {col}: {e}")
    conn.commit()
    cursor.close()


def _upsert_to_snowflake(conn, df, table, unique_cols):
    if df.empty:
        log.info(f"No rows to upsert to {table}")
        return
    df = df.copy()

    # Convert timestamp/datetime columns to strings
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: (
                    v.strftime("%Y-%m-%d %H:%M:%S") if hasattr(v, "strftime") else v
                )
            )

    # Convert pandas nullable dtypes to numpy — Snowflake connector doesn't support pd.NA
    for col in df.columns:
        if hasattr(df[col], "dtype") and hasattr(df[col].dtype, "numpy_dtype"):
            try:
                df[col] = df[col].astype(df[col].dtype.numpy_dtype)
            except Exception:
                df[col] = df[col].astype(object)

    df = df.replace(
        {np.nan: None, float("nan"): None, "nan": None, "NaN": None, "NAN": None}
    )
    df = df.where(pd.notnull(df), None)
    _add_missing_columns(conn, df, table)
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]]
    if df.empty:
        log.warning(f"No matching columns for {table}")
        return

    cursor = conn.cursor()
    cols = df.columns.tolist()
    unique_vals = df[unique_cols].drop_duplicates()

    # ── fast path: mlbam_id + game_date composite key ─────────────────────────
    # One DELETE per date instead of one DELETE per row
    if set(unique_cols) == {"mlbam_id", "game_date", "game_pk"}:
        id_list = ",".join(str(i) for i in df["mlbam_id"].unique())
        date_list = "', '".join(df["game_date"].unique())
        cursor.execute(
            f"DELETE FROM {DATABASE}.{SCHEMA}.{table} "
            f"WHERE mlbam_id IN ({id_list}) AND game_date IN ('{date_list}')"
        )
    elif len(unique_cols) == 1:
        ids = tuple(unique_vals[unique_cols[0]].tolist())
        cursor.execute(
            f"DELETE FROM {DATABASE}.{SCHEMA}.{table} "
            f"WHERE {unique_cols[0]} IN ({','.join(['%s']*len(ids))})",
            ids,
        )
    else:
        where = " AND ".join([f"{c} = %s" for c in unique_cols])
        delete_sql = f"DELETE FROM {DATABASE}.{SCHEMA}.{table} WHERE {where}"
        delete_data = [tuple(row) for row in unique_vals.itertuples(index=False)]
        cursor.executemany(delete_sql, delete_data)

    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{table} ({col_str}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.itertuples(index=False)]
    chunk_size = 1000
    try:
        for i in range(0, len(data), chunk_size):
            cursor.executemany(sql, data[i : i + chunk_size])
            log.info(
                f"Inserted chunk {i//chunk_size + 1}/{(len(data)-1)//chunk_size + 1}"
            )
        conn.commit()
    except Exception as e:
        log.error(f"Insert failed for {table}: {e}")
        conn.rollback()
        raise
    cursor.close()


def _bulk_insert_snowflake(conn, df, table):
    if df.empty:
        return
    _add_missing_columns(conn, df, table)
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]].copy()
    if df.empty:
        return

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: (
                    v.strftime("%Y-%m-%d %H:%M:%S") if hasattr(v, "strftime") else v
                )
            )

    df = df.convert_dtypes(convert_string=False).fillna(value=pd.NA)
    for col in df.columns:
        if hasattr(df[col], "dtype") and hasattr(df[col].dtype, "numpy_dtype"):
            try:
                df[col] = df[col].astype(df[col].dtype.numpy_dtype)
            except Exception:
                df[col] = df[col].astype(object)
    df = df.where(pd.notnull(df), None)

    cursor = conn.cursor()
    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{table} ({col_str}) VALUES ({placeholders})"
    data = [
        tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in row)
        for row in df.itertuples(index=False)
    ]
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        cursor.executemany(sql, data[i : i + chunk_size])
        conn.commit()
    cursor.close()
    log.info(f"Inserted {len(df)} rows to {table}")


def _truncate_and_bulk_insert(conn, df, table):
    if df.empty:
        return
    _add_missing_columns(conn, df, table)
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]]
    if df.empty:
        return

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.{table}")
    log.info(f"Truncated {table}")
    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{table} ({col_str}) VALUES ({placeholders})"
    data = [
        tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in row)
        for row in df.itertuples(index=False)
    ]
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    log.info(f"Inserted {len(df)} rows to {table}")


# ── Transform helpers ─────────────────────────────────────────────────────────


def _rolling_sum(df, col, winsize):
    return df[col].rolling(window=winsize, min_periods=1).sum().shift(1)


def _transform_batter_game(df):
    if df.empty:
        return pd.DataFrame()

    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values("game_date")
    rows = []

    for (game_date, game_pk), group in df.groupby(["game_date", "game_pk"]):
        pa = group[group["events"].notna() & (group["events"] != "")].copy()
        if pa.empty:
            continue

        pa["runs_scored"] = (pa["post_bat_score"] - pa["bat_score"]).clip(lower=0)

        is_home = group["inning_topbot"].iloc[0] == "Bot"
        opponent = group["away_team"].iloc[0] if is_home else group["home_team"].iloc[0]
        p_throws = group["p_throws"].iloc[0] if "p_throws" in group.columns else ""
        stand = group["stand"].iloc[0] if "stand" in group.columns else ""

        ab = len(pa[~pa["events"].isin(NON_AB_EVENTS)])
        h = len(pa[pa["events"].isin(["single", "double", "triple", "home_run"])])
        x2b = len(pa[pa["events"] == "double"])
        x3b = len(pa[pa["events"] == "triple"])
        hr = len(pa[pa["events"] == "home_run"])
        bb = len(pa[pa["events"].isin(["walk", "intent_walk"])])
        hbp = len(pa[pa["events"] == "hit_by_pitch"])
        sf = len(pa[pa["events"] == "sac_fly"])

        batted = pa[pa["launch_speed"].notna()].copy()
        n_batted = len(batted)
        ev_sum = float(batted["launch_speed"].sum())
        hard_hits = int((batted["launch_speed"] >= 95).sum())
        sweet_spots = int(
            ((batted["launch_angle"] >= 8) & (batted["launch_angle"] <= 32)).sum()
        )
        barrels = (
            int((batted["launch_speed_angle"] == 6).sum())
            if "launch_speed_angle" in batted.columns
            else 0
        )
        est_woba = (
            float(batted["estimated_woba_using_speedangle"].dropna().sum())
            if "estimated_woba_using_speedangle" in batted.columns
            else 0.0
        )
        est_slg = (
            float(batted["estimated_slg_using_speedangle"].dropna().sum())
            if "estimated_slg_using_speedangle" in batted.columns
            else 0.0
        )
        age = (
            float(group["age_bat"].dropna().iloc[0])
            if "age_bat" in group.columns and group["age_bat"].notna().any()
            else None
        )
        opp_pitcher = (
            int(group["pitcher"].iloc[0]) if "pitcher" in group.columns else None
        )
        opp_is_starter = int(group["inning"].min() == 1)

        rows.append(
            {
                "mlbam_id": int(group["batter"].iloc[0]),
                "game_date": game_date.date(),
                "game_pk": int(game_pk),
                "opponent": opponent,
                "is_home": int(is_home),
                "stand": stand,
                "p_throws": p_throws,
                "opp_pitcher_id": opp_pitcher,
                "opp_is_starter": opp_is_starter,
                "age": age,
                "ab": ab,
                "h": h,
                "x2b": x2b,
                "x3b": x3b,
                "hr": hr,
                "bb": bb,
                "hbp": hbp,
                "sf": sf,
                "hr_vs_r": hr if p_throws == "R" else 0,
                "ab_vs_r": ab if p_throws == "R" else 0,
                "hr_vs_l": hr if p_throws == "L" else 0,
                "ab_vs_l": ab if p_throws == "L" else 0,
                "batted_balls": n_batted,
                "ev_sum": ev_sum,
                "hard_hits": hard_hits,
                "sweet_spots": sweet_spots,
                "barrels": barrels,
                "est_woba": est_woba,
                "est_slg": est_slg,
            }
        )

    return pd.DataFrame(rows)


def _transform_pitcher_game(df):
    """Pitch-level Statcast → one row per pitcher-game."""
    if df.empty:
        return pd.DataFrame()

    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values("game_date")
    rows = []

    out_events = {
        "strikeout": 1,
        "field_out": 1,
        "force_out": 1,
        "sac_bunt": 1,
        "sac_fly": 1,
        "fielders_choice_out": 1,
        "grounded_into_double_play": 2,
        "double_play": 2,
        "triple_play": 3,
    }

    for (game_date, game_pk), group in df.groupby(["game_date", "game_pk"]):
        pa = group[group["events"].notna() & (group["events"] != "")].copy()
        if pa.empty:
            continue

        pa["runs_scored"] = (pa["post_bat_score"] - pa["bat_score"]).clip(lower=0)

        is_home_pitcher = group["inning_topbot"].iloc[0] == "Top"
        opponent = (
            group["away_team"].iloc[0]
            if is_home_pitcher
            else group["home_team"].iloc[0]
        )

        outs = pa["events"].apply(lambda e: out_events.get(e, 0)).sum()
        ip = outs / 3.0
        bfp = len(pa)
        h = len(pa[pa["events"].isin(["single", "double", "triple", "home_run"])])
        x2b = len(pa[pa["events"] == "double"])
        x3b = len(pa[pa["events"] == "triple"])
        hr = len(pa[pa["events"] == "home_run"])
        bb = len(pa[pa["events"].isin(["walk", "intent_walk"])])
        so = len(pa[pa["events"] == "strikeout"])
        r = int(pa["runs_scored"].sum())
        er = r

        batted = pa[pa["launch_speed"].notna()].copy()
        n_batted = len(batted)
        fly_balls = (
            int((batted["bb_type"] == "fly_ball").sum())
            if "bb_type" in batted.columns
            else 0
        )
        gs = int(group["inning"].min() == 1)

        rows.append(
            {
                "mlbam_id": int(group["pitcher"].iloc[0]),
                "game_date": game_date.date(),
                "game_pk": int(game_pk),
                "opponent": opponent,
                "is_home_pitcher": int(is_home_pitcher),
                "gs": gs,
                "ip": ip,
                "bfp": bfp,
                "h": h,
                "bb": bb,
                "so": so,
                "hr": hr,
                "r": r,
                "er": er,
                "x2b": x2b,
                "x3b": x3b,
                "fly_balls": fly_balls,
                "batted_balls_allowed": n_batted,
            }
        )

    return pd.DataFrame(rows)


# ── Pipeline tasks ────────────────────────────────────────────────────────────


def get_yesterdays_players() -> dict:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Fetching players for {yesterday}")
    resp = requests.get(
        f"{MLB_API}/schedule", params={"sportId": 1, "date": yesterday}, timeout=15
    )
    resp.raise_for_status()

    batter_ids = set()
    pitcher_ids = set()

    for date_obj in resp.json().get("dates", []):
        for game in date_obj.get("games", []):
            game_pk = game["gamePk"]
            try:
                box_resp = requests.get(
                    f"{MLB_API}/game/{game_pk}/boxscore", timeout=15
                )
                box_resp.raise_for_status()
                boxscore = box_resp.json()
                for side in ["home", "away"]:
                    players = boxscore.get("teams", {}).get(side, {}).get("players", {})
                    for _, player in players.items():
                        pos = player.get("position", {}).get("abbreviation", "")
                        pid = player.get("person", {}).get("id")
                        if not pid:
                            continue
                        if pos == "P":
                            pitcher_ids.add(pid)
                        else:
                            batter_ids.add(pid)
            except Exception as e:
                log.warning(f"Error fetching boxscore for game {game_pk}: {e}")

    log.info(f"Found {len(batter_ids)} batters, {len(pitcher_ids)} pitchers")
    return {
        "date": yesterday,
        "batter_ids": list(batter_ids),
        "pitcher_ids": list(pitcher_ids),
    }


def fetch_and_load_batter_stats(player_info: dict) -> int:
    log.info(
        f'fetch_and_load_batter_stats: {len(player_info["batter_ids"])} batters for {player_info["date"]}'
    )
    game_date = player_info["date"]
    batter_ids = player_info["batter_ids"]
    conn = _get_snowflake_conn()
    all_rows = []

    for mlbam_id in batter_ids:
        try:
            df = statcast_batter(game_date, game_date, mlbam_id)
            if df is None or df.empty:
                continue
            game_df = _transform_batter_game(df)
            if game_df.empty:
                continue
            all_rows.append(game_df)
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Error fetching batter {mlbam_id}: {e}")

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        log.info(f"Upserting {len(combined)} batter rows")
        _upsert_to_snowflake(
            conn, combined, "RAW_BATTER_GAMES", ["mlbam_id", "game_date", "game_pk"]
        )

    conn.close()
    total = len(combined) if all_rows else 0
    log.info(f"Loaded {total} batter game rows")
    return total


def fetch_and_load_pitcher_stats(player_info: dict) -> int:
    game_date = player_info["date"]
    pitcher_ids = player_info["pitcher_ids"]
    conn = _get_snowflake_conn()
    all_rows = []

    for mlbam_id in pitcher_ids:
        try:
            df = statcast_pitcher(game_date, game_date, mlbam_id)
            if df is None or df.empty:
                continue
            game_df = _transform_pitcher_game(df)
            if game_df.empty:
                continue
            all_rows.append(game_df)
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Error fetching pitcher {mlbam_id}: {e}")

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        log.info(f"Upserting {len(combined)} pitcher rows")
        _upsert_to_snowflake(
            conn, combined, "RAW_PITCHER_GAMES", ["mlbam_id", "game_date", "game_pk"]
        )

    conn.close()
    total = len(combined) if all_rows else 0
    log.info(f"Loaded {total} pitcher game rows")
    return total


def get_recent_reliever_ids(days: int = 7) -> list:
    """Get pitcher IDs for relievers who appeared in the last N days."""
    conn = _get_snowflake_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT DISTINCT mlbam_id
            FROM {DATABASE}.{SCHEMA}.RAW_PITCHER_GAMES
            WHERE game_date >= DATEADD(day, -{days}, CURRENT_DATE)
            AND gs = 0
        """
        )
        ids = [row[0] for row in cursor.fetchall()]
        log.info(f"Found {len(ids)} relievers from last {days} days")
        return ids
    except Exception as e:
        log.warning(f"Error fetching recent reliever IDs: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def update_game_results() -> int:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    conn = _get_snowflake_conn()
    resp = requests.get(
        f"{MLB_API}/schedule",
        params={"sportId": 1, "date": yesterday, "hydrate": "team,linescore"},
        timeout=15,
    )
    resp.raise_for_status()

    rows = []
    for date_obj in resp.json().get("dates", []):
        for game in date_obj.get("games", []):
            if game.get("status", {}).get("abstractGameState") != "Final":
                continue
            home = game["teams"]["home"]
            away = game["teams"]["away"]
            rows.append(
                {
                    "game_pk": game["gamePk"],
                    "game_date": yesterday,
                    "team_h": home["team"]["abbreviation"],
                    "team_v": away["team"]["abbreviation"],
                    "runs_h": home.get("score", 0),
                    "runs_v": away.get("score", 0),
                    "home_victory": int(home.get("score", 0) > away.get("score", 0)),
                    "run_diff": home.get("score", 0) - away.get("score", 0),
                }
            )

    if rows:
        df = pd.DataFrame(rows)
        _upsert_to_snowflake(conn, df, "GAME_RESULTS", ["game_pk"])
        log.info(f"Loaded {len(rows)} game results")

    conn.close()
    return len(rows)


def fetch_and_load_pitch_stats(player_info: dict) -> int:
    """Pull raw pitch-level Statcast data for date → Snowflake RAW_PITCHES."""
    game_date = player_info["date"]
    log.info(f"Fetching raw pitches for {game_date} …")

    try:
        df = statcast(start_dt=game_date, end_dt=game_date)
    except Exception as e:
        log.warning(f"Error fetching pitches for {game_date}: {e}")
        return 0

    if df is None or df.empty:
        log.info(f"No pitch data for {game_date}")
        return 0

    df["_source"] = "pybaseball"

    conn = _get_snowflake_conn()
    try:
        # One DELETE by date instead of ~4,000 individual row deletes
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM {DATABASE}.{SCHEMA}.RAW_PITCHES WHERE game_date = '{game_date}'"
        )
        conn.commit()
        cursor.close()
        log.info(f"Cleared existing pitches for {game_date}")
        _bulk_insert_snowflake(conn, df, "RAW_PITCHES")
        log.info(f"Loaded {len(df)} pitch rows for {game_date}")
    finally:
        conn.close()
    return len(df)


def update_bvp_history() -> int:
    """Refresh BVP_HISTORY from PITCHES view for yesterday's games."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    conn = _get_snowflake_conn()
    cursor = conn.cursor()

    merge_sql = f"""
        MERGE INTO {DATABASE}.{SCHEMA}.BVP_HISTORY tgt
        USING (
            WITH pa_level AS (
                SELECT
                    batter, pitcher, game_date, game_pk,
                    CASE WHEN events = 'home_run' THEN 1 ELSE 0 END AS is_hr,
                    1 AS is_pa
                FROM {DATABASE}.{SCHEMA}.PITCHES
                WHERE game_date = '{yesterday}'
                  AND events IS NOT NULL AND events != ''
            ),
            pa_by_game AS (
                SELECT batter, pitcher, game_date, game_pk,
                       SUM(is_pa) AS pa, SUM(is_hr) AS hr
                FROM pa_level
                GROUP BY batter, pitcher, game_date, game_pk
            ),
            -- Most recent cumulative row per batter-pitcher instead of full scan
            prior AS (
                SELECT batter, pitcher, bvp_pa_prior, bvp_hr_prior
                FROM (
                    SELECT batter, pitcher, bvp_pa_prior, bvp_hr_prior,
                           ROW_NUMBER() OVER (
                               PARTITION BY batter, pitcher
                               ORDER BY game_date DESC, game_pk DESC
                           ) AS rn
                    FROM {DATABASE}.{SCHEMA}.BVP_HISTORY
                )
                WHERE rn = 1
            )
            SELECT
                p.batter, p.pitcher, p.game_date, p.game_pk,
                p.pa, p.hr,
                COALESCE(pr.bvp_pa_prior, 0) + p.pa AS bvp_pa_prior,
                COALESCE(pr.bvp_hr_prior, 0) + p.hr AS bvp_hr_prior
            FROM pa_by_game p
            LEFT JOIN prior pr
                ON p.batter  = pr.batter
               AND p.pitcher = pr.pitcher
        ) src
        ON  tgt.batter  = src.batter
        AND tgt.pitcher = src.pitcher
        AND tgt.game_pk = src.game_pk
        WHEN NOT MATCHED THEN INSERT
            (batter, pitcher, game_date, game_pk, pa, hr, bvp_pa_prior, bvp_hr_prior)
        VALUES
            (src.batter, src.pitcher, src.game_date, src.game_pk,
             src.pa, src.hr, src.bvp_pa_prior, src.bvp_hr_prior)
    """

    try:
        cursor.execute(merge_sql)
        rows = cursor.rowcount
        conn.commit()
        log.info(f"BVP_HISTORY updated — {rows} rows merged for {yesterday}")
    finally:
        cursor.close()
        conn.close()
    return rows


# ── Checkpoint helpers ────────────────────────────────────────────────────────


def _lineup_hash(batter_ids: list, pitcher_ids: list) -> str:
    """Stable hash of a game's confirmed roster. Changes if a player is scratched."""
    combined = sorted(map(str, batter_ids + pitcher_ids))
    return hashlib.md5("|".join(combined).encode()).hexdigest()


def _get_uncomputed_games(conn, confirmed_games: list[dict]) -> list[dict]:
    """
    confirmed_games: [{"game_pk": int, "batter_ids": [...], "pitcher_ids": [...]}]
    Returns only games whose lineup hash differs from the last stored run,
    meaning they need rolling features recomputed.
    """
    if not confirmed_games:
        return []

    game_pks = tuple(g["game_pk"] for g in confirmed_games)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT game_pk, lineup_hash
        FROM {DATABASE}.{SCHEMA}.FEATURE_RUN_CHECKPOINTS
        WHERE game_pk IN ({','.join(['%s'] * len(game_pks))})
        """,
        game_pks,
    )
    stored = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()

    to_recompute = []
    for game in confirmed_games:
        current_hash = _lineup_hash(game["batter_ids"], game["pitcher_ids"])
        if stored.get(game["game_pk"]) != current_hash:
            to_recompute.append({**game, "lineup_hash": current_hash})

    skipped = len(confirmed_games) - len(to_recompute)
    log.info(
        f"Checkpoint: {len(to_recompute)} games need recompute, {skipped} already current"
    )
    return to_recompute


def _checkpoint_games(conn, computed_games: list[dict], game_date: str):
    """Upsert a checkpoint row for each successfully computed game."""
    if not computed_games:
        return
    cursor = conn.cursor()
    for game in computed_games:
        cursor.execute(
            f"""
            MERGE INTO {DATABASE}.{SCHEMA}.FEATURE_RUN_CHECKPOINTS t
            USING (SELECT %s AS game_pk) s ON t.game_pk = s.game_pk
            WHEN MATCHED THEN UPDATE SET
                lineup_hash = %s,
                game_date   = %s,
                computed_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (game_pk, lineup_hash, game_date, computed_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP())
            """,
            (
                game["game_pk"],
                game["lineup_hash"],
                game_date,
                game["game_pk"],
                game["lineup_hash"],
                game_date,
            ),
        )
    conn.commit()
    cursor.close()
    log.info(f"Checkpointed {len(computed_games)} games for {game_date}")


# ── Lineup fetch ──────────────────────────────────────────────────────────────


def get_todays_lineup_players() -> dict:
    """
    Hit the MLB API for today's confirmed lineups.

    Returns per-game roster lists (for checkpoint comparison) as well as
    flat batter_ids / pitcher_ids sets for callers that just need the union.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resp = requests.get(
        f"{MLB_API}/schedule",
        params={"sportId": 1, "date": today, "hydrate": "lineups,probablePitcher"},
        timeout=15,
    )
    resp.raise_for_status()
    games = resp.json().get("dates", [{}])[0].get("games", [])

    confirmed_games = []
    all_batter_ids: set = set()
    all_pitcher_ids: set = set()

    for game in games:
        lineups = game.get("lineups", {})
        # Skip games where the lineup hasn't been posted yet
        if not lineups.get("homePlayers") and not lineups.get("awayPlayers"):
            continue

        game_pk = game["gamePk"]
        batter_ids: set = set()
        pitcher_ids: set = set()

        for side in ["homePlayers", "awayPlayers"]:
            for player in lineups.get(side, []):
                pid = player.get("id")
                pos = player.get("primaryPosition", {}).get("abbreviation", "")
                if not pid:
                    continue
                if pos == "P":
                    pitcher_ids.add(pid)
                else:
                    batter_ids.add(pid)

        # Always include probable pitchers even if full lineup not yet posted
        for side in ["home", "away"]:
            pitcher = game.get("teams", {}).get(side, {}).get("probablePitcher", {})
            pid = pitcher.get("id")
            if pid:
                pitcher_ids.add(pid)

        confirmed_games.append(
            {
                "game_pk": game_pk,
                "batter_ids": list(batter_ids),
                "pitcher_ids": list(pitcher_ids),
            }
        )
        all_batter_ids.update(batter_ids)
        all_pitcher_ids.update(pitcher_ids)

    log.info(
        f"Today ({today}): {len(confirmed_games)} confirmed games, "
        f"{len(all_batter_ids)} batters, {len(all_pitcher_ids)} pitchers"
    )
    return {
        "date": today,
        "confirmed_games": confirmed_games,
        "batter_ids": list(all_batter_ids),
        "pitcher_ids": list(all_pitcher_ids),
    }


# ── Rolling features ──────────────────────────────────────────────────────────


def compute_rolling_features(
    batter_ids: list,
    pitcher_ids: list,
    game_date: str = None,
    year: int = None,
    pitcher_only: bool = False,
    allow_full_recompute: bool = False,
) -> str:
    """Recompute rolling features from raw tables → feature tables.

    allow_full_recompute must be explicitly True to run a full table recompute.
    This prevents an accidental expensive fallthrough during the daily pipeline
    when batter_ids/pitcher_ids are empty (e.g. lineups not confirmed yet).
    """
    # ── safety guard ──────────────────────────────────────────────────────────
    if not batter_ids and not pitcher_ids and not year and not allow_full_recompute:
        log.warning(
            "compute_rolling_features: no player IDs, no year, and "
            "allow_full_recompute=False — skipping to avoid full table recompute"
        )
        return "skipped"

    conn = _get_snowflake_conn()
    cursor = conn.cursor()

    # ── build WHERE clauses ───────────────────────────────────────────────────
    if batter_ids or pitcher_ids:
        # Use OR so a partial list (e.g. only batters) still takes the fast path
        b_ids = ",".join(str(i) for i in batter_ids) if batter_ids else "NULL"
        p_ids = ",".join(str(i) for i in pitcher_ids) if pitcher_ids else "NULL"
        batter_where = f"""WHERE mlbam_id IN ({b_ids})
            AND game_date >= DATEADD(day, -{ROLLING_LOOKBACK_DAYS}, '{game_date}')
            ORDER BY mlbam_id, game_date"""
        pitcher_where = f"""WHERE mlbam_id IN ({p_ids})
            AND game_date >= DATEADD(day, -{ROLLING_LOOKBACK_DAYS}, '{game_date}')
            ORDER BY mlbam_id, game_date"""
        insert_fn = lambda conn, df, table: _upsert_to_snowflake(
            conn, df, table, ["mlbam_id", "game_date", "game_pk"]
        )
    elif game_date:
        batter_where = f"""WHERE mlbam_id IN (
              SELECT DISTINCT mlbam_id FROM {DATABASE}.{SCHEMA}.RAW_BATTER_GAMES
              WHERE game_date = '{game_date}'
          ) AND game_date >= DATEADD(day, -{ROLLING_LOOKBACK_DAYS}, '{game_date}')
          ORDER BY mlbam_id, game_date"""
        pitcher_where = f"""WHERE mlbam_id IN (
              SELECT DISTINCT mlbam_id FROM {DATABASE}.{SCHEMA}.RAW_PITCHER_GAMES
              WHERE game_date = '{game_date}'
          ) AND game_date >= DATEADD(day, -{ROLLING_LOOKBACK_DAYS}, '{game_date}')
          ORDER BY mlbam_id, game_date"""
        insert_fn = lambda conn, df, table: _upsert_to_snowflake(
            conn, df, table, ["mlbam_id", "game_date", "game_pk"]
        )
    elif year:
        batter_where = f"WHERE YEAR(game_date) = {year} ORDER BY mlbam_id, game_date"
        pitcher_where = f"WHERE YEAR(game_date) = {year} ORDER BY mlbam_id, game_date"
        insert_fn = lambda conn, df, table: _bulk_insert_snowflake(conn, df, table)
    else:
        batter_where = "ORDER BY mlbam_id, game_date"
        pitcher_where = "ORDER BY mlbam_id, game_date"
        log.info("Full recompute — truncating feature tables …")
        for table in ["BATTER_ROLLING_FEATURES", "PITCHER_ROLLING_FEATURES"]:
            trunc_cur = conn.cursor()
            trunc_cur.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.{table}")
            conn.commit()
            trunc_cur.close()
            log.info(f"Truncated {table}")
        insert_fn = lambda conn, df, table: _bulk_insert_snowflake(conn, df, table)

    # ── pull raw data ─────────────────────────────────────────────────────────
    if not pitcher_only:
        cursor.execute(
            f"SELECT * FROM {DATABASE}.{SCHEMA}.RAW_BATTER_GAMES {batter_where}"
        )
        batter_df = pd.DataFrame(
            cursor.fetchall(), columns=[desc[0].lower() for desc in cursor.description]
        )
        log.info(f"Pulled {len(batter_df)} batter rows")

    cursor.execute(
        f"SELECT * FROM {DATABASE}.{SCHEMA}.RAW_PITCHER_GAMES {pitcher_where}"
    )
    pitcher_df = pd.DataFrame(
        cursor.fetchall(), columns=[desc[0].lower() for desc in cursor.description]
    )
    log.info(f"Pulled {len(pitcher_df)} pitcher rows")
    cursor.close()

    # ── batter rolling features ───────────────────────────────────────────────
    if not pitcher_only:
        batter_feat_rows = []
        bat_stat_cols = [
            "hr",
            "ab",
            "bb",
            "h",
            "hbp",
            "sf",
            "x2b",
            "x3b",
            "hr_vs_r",
            "ab_vs_r",
            "hr_vs_l",
            "ab_vs_l",
            "barrels",
            "ev_sum",
            "hard_hits",
            "sweet_spots",
            "batted_balls",
            "est_woba",
            "est_slg",
        ]

        for mlbam_id, df in batter_df.groupby("mlbam_id"):
            df = df.sort_values("game_date").reset_index(drop=True)
            new_cols = {}
            for w in WINDOWS_BAT:
                for col in bat_stat_cols:
                    if col in df.columns:
                        new_cols[f"rollsum_{col}_{w}"] = _rolling_sum(df, col, w).values

            new_df = pd.DataFrame(new_cols, index=df.index)
            df = pd.concat([df, new_df], axis=1)

            for w in WINDOWS_BAT:

                def g(col, _w=w):
                    return pd.Series(
                        new_cols.get(f"rollsum_{col}_{_w}", np.zeros(len(df))),
                        index=df.index,
                    )

                ab = g("ab")
                hr = g("hr")
                h = g("h")
                bb = g("bb")
                hbp = g("hbp")
                sf = g("sf")
                x2b = g("x2b")
                x3b = g("x3b")
                bbd = g("batted_balls")
                evs = g("ev_sum")
                hh = g("hard_hits")
                ss = g("sweet_spots")
                bar = g("barrels")
                hr_r = g("hr_vs_r")
                ab_r = g("ab_vs_r")
                hr_l = g("hr_vs_l")
                ab_l = g("ab_vs_l")

                ab_denom = ab.replace(0, np.nan)
                pa_denom = (ab + bb + hbp + sf).replace(0, np.nan)
                batted_denom = bbd.replace(0, np.nan)

                df[f"hr_per_pa_{w}"] = hr / pa_denom
                df[f"slg_{w}"] = (h + x2b + 2 * x3b + 3 * hr) / ab_denom
                df[f"obp_{w}"] = (h + bb + hbp) / pa_denom
                df[f"obs_{w}"] = df[f"slg_{w}"] + df[f"obp_{w}"]
                df[f"ev_{w}"] = evs / batted_denom
                df[f"hardhit_{w}"] = hh / batted_denom
                df[f"swspot_{w}"] = ss / batted_denom
                df[f"barrel_{w}"] = bar / batted_denom
                df[f"hr_per_pa_vs_r_{w}"] = hr_r / ab_r.replace(0, np.nan)
                df[f"hr_per_pa_vs_l_{w}"] = hr_l / ab_l.replace(0, np.nan)
                df[f"est_woba_{w}"] = g("est_woba") / batted_denom
                df[f"est_slg_{w}"] = g("est_slg") / batted_denom

            batter_feat_rows.append(df)

        if batter_feat_rows:
            batter_features = pd.concat(batter_feat_rows, ignore_index=True)
            batter_features = batter_features.sort_values(
                ["mlbam_id", "game_date"]
            ).reset_index(drop=True)
            batter_features = (
                batter_features.groupby(["mlbam_id", "game_date"]).last().reset_index()
            )
            log.info(f"Computed batter features: {len(batter_features)} rows")
            t = time.time()
            insert_fn(conn, batter_features, "BATTER_ROLLING_FEATURES")
            log.info(f"Batter insert took {time.time() - t:.1f}s")
            del batter_features, batter_feat_rows

    # ── pitcher rolling features ──────────────────────────────────────────────
    pitcher_feat_rows = []
    pitch_stat_cols = [
        "hr",
        "bfp",
        "fly_balls",
        "batted_balls_allowed",
        "ip",
        "h",
        "bb",
        "so",
        "er",
        "x2b",
        "x3b",
    ]

    for mlbam_id, df in pitcher_df.groupby("mlbam_id"):
        df = df.sort_values("game_date").reset_index(drop=True)
        new_cols = {}
        for w in WINDOWS_PITCH:
            for col in pitch_stat_cols:
                new_cols[f"rollsum_{col}_{w}"] = (
                    _rolling_sum(df, col, w).values
                    if col in df.columns
                    else np.zeros(len(df))
                )

        new_df = pd.DataFrame(new_cols, index=df.index)
        df = pd.concat([df, new_df], axis=1)

        for w in WINDOWS_PITCH:

            def s(col, _w=w):
                return pd.Series(
                    new_cols.get(f"rollsum_{col}_{_w}", np.zeros(len(df))),
                    index=df.index,
                )

            hr = s("hr")
            bf = s("bfp")
            fb = s("fly_balls")
            bat = s("batted_balls_allowed")
            ip = s("ip")
            h = s("h")
            bb = s("bb")
            so = s("so")
            er = s("er")
            x2b = s("x2b")
            x3b = s("x3b")

            df[f"hr_per_bf_{w}"] = hr / bf.replace(0, np.nan)
            df[f"fb_perc_{w}"] = fb / bat.replace(0, np.nan)

            ip_mod = np.maximum(ip, w * IP_PER_GAME_DEF)
            bf_mod = np.maximum(bf, w * BF_PER_GAME_DEF)
            xb = x2b + 2 * x3b + 3 * hr
            tb = h + xb
            h_bb = h + bb
            fip = 13 * hr + 3 * h_bb - 2 * so

            h_bb_mod = h_bb + H_BB_PER_IP_DEF * (ip_mod - ip)
            h_bb_mod2 = h_bb + H_BB_PER_BF_DEF * (bf_mod - bf)
            so_mod = so + SO_PER_BF_DEF * (bf_mod - bf)
            tb_bb_mod = (tb + bb) + TB_BB_PERC_DEF * (bf_mod - bf)
            er_mod = er + ER_PER_IP_DEF * (ip_mod - ip)
            fip_mod = fip + FIP_PER_IP_DEF * (ip_mod - ip)
            fip_mod2 = fip + FIP_PER_BF_DEF * (bf_mod - bf)

            df[f"whip_{w}"] = h_bb_mod / ip_mod
            df[f"so_perc_{w}"] = so_mod / bf_mod
            df[f"h_bb_perc_{w}"] = h_bb_mod2 / bf_mod
            df[f"tb_bb_perc_{w}"] = tb_bb_mod / bf_mod
            df[f"era_{w}"] = (er_mod / ip_mod) * 9
            df[f"fip_{w}"] = fip_mod / ip_mod
            df[f"fip_perc_{w}"] = fip_mod2 / bf_mod

        pitcher_feat_rows.append(df)

    if pitcher_feat_rows:
        pitcher_features = pd.concat(pitcher_feat_rows, ignore_index=True)
        pitcher_features = pitcher_features.sort_values(
            ["mlbam_id", "game_date"]
        ).reset_index(drop=True)
        pitcher_features = (
            pitcher_features.groupby(["mlbam_id", "game_date"]).last().reset_index()
        )
        log.info(f"Computed pitcher features: {len(pitcher_features)} rows")
        t = time.time()
        insert_fn(conn, pitcher_features, "PITCHER_ROLLING_FEATURES")
        log.info(f"Pitcher insert took {time.time() - t:.1f}s")
        del pitcher_features, pitcher_feat_rows

    conn.close()
    log.info("Rolling features complete")
    return "success"


# ── Daily ingest entry point ──────────────────────────────────────────────────


def run_daily_ingest():
    """
    Main entry point for the 4x/day scheduled runs.

    Flow:
      1. Hit MLB API for today's confirmed lineups (per game).
      2. Compare each game's roster hash against FEATURE_RUN_CHECKPOINTS.
      3. Only compute rolling features for games that are new or whose
         lineup changed (late scratch, etc.).
      4. Merge in recent relievers (last 7 days) so their features stay
         current even when they aren't in today's confirmed lineup.
      5. Checkpoint successfully computed games so the next run skips them.
    """
    today_data = get_todays_lineup_players()
    confirmed_games = today_data["confirmed_games"]
    game_date = today_data["date"]

    if not confirmed_games:
        log.info("No confirmed lineups yet — skipping feature compute")
        return

    conn = _get_snowflake_conn()
    try:
        games_to_run = _get_uncomputed_games(conn, confirmed_games)
        if not games_to_run:
            log.info("All confirmed games already current — nothing to recompute")
            return

        # Flatten to unique player IDs across only the games that need recompute
        batter_ids = list({pid for g in games_to_run for pid in g["batter_ids"]})
        pitcher_ids = list({pid for g in games_to_run for pid in g["pitcher_ids"]})

        # Always refresh recent relievers regardless of lineup confirmation —
        # they appear late and are often missing from the confirmed lineup set
        recent_relievers = get_recent_reliever_ids(days=7)
        pitcher_ids = list(set(pitcher_ids) | set(recent_relievers))

        log.info(
            f"Recomputing features for {len(games_to_run)} games — "
            f"{len(batter_ids)} batters, {len(pitcher_ids)} pitchers "
            f"({len(recent_relievers)} relievers merged)"
        )

        result = compute_rolling_features(
            batter_ids=batter_ids,
            pitcher_ids=pitcher_ids,
            game_date=game_date,
        )

        if result == "success":
            _checkpoint_games(conn, games_to_run, game_date)
        else:
            log.warning(
                f"compute_rolling_features returned '{result}' — not checkpointing"
            )
    finally:
        conn.close()
