import os
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

import requests
from pybaseball import statcast_batter, statcast_pitcher

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
    """Add any columns in df that don't exist in the table yet."""
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
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if (isinstance(x, float) and np.isnan(x)) else x
        )
    _add_missing_columns(conn, df, table)
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]]
    if df.empty:
        log.warning(f"No matching columns for {table}")
        return

    cursor = conn.cursor()
    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    where = " AND ".join([f"{c} = %s" for c in unique_cols])

    # single bulk DELETE using IN clause
    unique_vals = df[unique_cols].drop_duplicates()
    if len(unique_cols) == 1:
        ids = tuple(unique_vals[unique_cols[0]].tolist())
        cursor.execute(
            f"DELETE FROM {DATABASE}.{SCHEMA}.{table} WHERE {unique_cols[0]} IN ({','.join(['%s']*len(ids))})",
            ids,
        )
    else:
        # batch delete with executemany
        delete_sql = f"DELETE FROM {DATABASE}.{SCHEMA}.{table} WHERE {where}"
        delete_data = [tuple(row) for row in unique_vals.itertuples(index=False)]
        cursor.executemany(delete_sql, delete_data)

    # bulk insert with executemany
    sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{table} ({col_str}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.itertuples(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()


def _bulk_insert_snowflake(conn, df, table):
    if df.empty:
        return

    _add_missing_columns(conn, df, table)
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]]
    if df.empty:
        return

    cursor = conn.cursor()
    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{table} ({col_str}) VALUES ({placeholders})"

    data = [
        tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in row)
        for row in df.itertuples(index=False)
    ]

    # batch insert in chunks of 1000
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        cursor.executemany(sql, chunk)
        conn.commit()

    cursor.close()
    log.info(f"Inserted {len(df)} rows to {table}")


def _truncate_and_bulk_insert(conn, df, table):
    """Truncate table and bulk insert all rows — for full recompute only."""
    if df.empty:
        return

    # add missing columns first
    _add_missing_columns(conn, df, table)

    # only keep columns that exist in table
    table_cols = _get_table_columns(conn, table)
    df = df[[c for c in df.columns if c.lower() in table_cols]]
    if df.empty:
        return

    cursor = conn.cursor()

    # truncate first
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
    """Pitch-level Statcast → one row per batter-game."""
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
        hr = len(pa[pa["events"] == "home_run"])
        r = int(pa["runs_scored"].sum())

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
                "hr": hr,
                "r": r,
                "fly_balls": fly_balls,
                "batted_balls_allowed": n_batted,
            }
        )

    return pd.DataFrame(rows)


# ── Pipeline tasks ────────────────────────────────────────────────────────────


def get_yesterdays_players() -> dict:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"Fetching players for {yesterday}")

    # get game IDs from schedule
    resp = requests.get(
        f"{MLB_API}/schedule",
        params={"sportId": 1, "date": yesterday},
        timeout=15,
    )
    resp.raise_for_status()

    batter_ids = set()
    pitcher_ids = set()

    for date_obj in resp.json().get("dates", []):
        for game in date_obj.get("games", []):
            game_pk = game["gamePk"]
            try:
                # fetch boxscore directly per game
                box_resp = requests.get(
                    f"{MLB_API}/game/{game_pk}/boxscore",
                    timeout=15,
                )
                box_resp.raise_for_status()
                boxscore = box_resp.json()

                for side in ["home", "away"]:
                    team = boxscore.get("teams", {}).get(side, {})
                    players = team.get("players", {})
                    for player_key, player in players.items():
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
        f'fetch_and_load_batter_stats called with {len(player_info["batter_ids"])} batters for {player_info["date"]}'
    )
    game_date = player_info["date"]
    batter_ids = player_info["batter_ids"]
    conn = _get_snowflake_conn()
    total_rows = 0

    for mlbam_id in batter_ids:
        try:
            df = statcast_batter(game_date, game_date, mlbam_id)
            if df is None or df.empty:
                continue
            game_df = _transform_batter_game(df)
            if game_df.empty:
                continue
            _upsert_to_snowflake(
                conn,
                game_df,
                "RAW_BATTER_GAMES",
                ["mlbam_id", "game_date", "game_pk"],
            )
            total_rows += len(game_df)
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Error fetching batter {mlbam_id}: {e}")

    conn.close()
    log.info(f"Loaded {total_rows} batter game rows")
    return total_rows


def fetch_and_load_pitcher_stats(player_info: dict) -> int:
    """Pull Statcast pitcher data for date → Snowflake RAW_PITCHER_GAMES."""
    game_date = player_info["date"]
    pitcher_ids = player_info["pitcher_ids"]
    conn = _get_snowflake_conn()
    total_rows = 0

    for mlbam_id in pitcher_ids:
        try:
            df = statcast_pitcher(game_date, game_date, mlbam_id)
            if df is None or df.empty:
                continue
            game_df = _transform_pitcher_game(df)
            if game_df.empty:
                continue
            _upsert_to_snowflake(
                conn,
                game_df,
                "RAW_PITCHER_GAMES",
                ["mlbam_id", "game_date", "game_pk"],
            )
            total_rows += len(game_df)
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Error fetching pitcher {mlbam_id}: {e}")

    conn.close()
    log.info(f"Loaded {total_rows} pitcher game rows")
    return total_rows


def update_game_results() -> int:
    """Fetch yesterday's game outcomes from MLB Stats API → Snowflake GAME_RESULTS."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    conn = _get_snowflake_conn()

    resp = requests.get(
        f"{MLB_API}/schedule",
        params={
            "sportId": 1,
            "date": yesterday,
            "hydrate": "team,linescore",
        },
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


def compute_rolling_features(
    batter_rows: int,
    pitcher_rows: int,
    game_date: str = None,
    year: int = None,
    pitcher_only: bool = False,
) -> str:
    """Recompute rolling features from raw tables → feature tables.

    If game_date is provided, only recomputes for players who played on that date (incremental).
    If game_date is None, recomputes all players (full recompute).
    """
    conn = _get_snowflake_conn()
    cursor = conn.cursor()

    # ── build WHERE clause ────────────────────────────────────────────────────
    if game_date:
        batter_where = f"""WHERE mlbam_id IN (
              SELECT DISTINCT mlbam_id FROM {DATABASE}.{SCHEMA}.RAW_BATTER_GAMES 
              WHERE game_date = '{game_date}'
          ) 
          AND game_date >= DATEADD(day, -365, '{game_date}')
          ORDER BY mlbam_id, game_date"""

        pitcher_where = f"""WHERE mlbam_id IN (
              SELECT DISTINCT mlbam_id FROM {DATABASE}.{SCHEMA}.RAW_PITCHER_GAMES 
              WHERE game_date = '{game_date}'
          ) 
          AND game_date >= DATEADD(day, -365, '{game_date}')
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
        insert_fn = lambda conn, df, table: _truncate_and_bulk_insert(conn, df, table)

    # ── pull data ─────────────────────────────────────────────────────────────
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

    if not pitcher_only:
        # ── batter rolling features ───────────────────────────────────────────────
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
            log.info(f"Computed batter features: {len(batter_features)} rows")
            insert_fn(conn, batter_features, "BATTER_ROLLING_FEATURES")
            log.info(f"{year}: inserted {len(batter_features)} batter feature rows")
            del batter_features, batter_feat_rows

    # ── pitcher rolling features ──────────────────────────────────────────────
    pitcher_feat_rows = []

    for mlbam_id, df in pitcher_df.groupby("mlbam_id"):
        df = df.sort_values("game_date").reset_index(drop=True)
        new_cols = {}

        for w in WINDOWS_PITCH:
            for col in ["hr", "bfp", "fly_balls", "batted_balls_allowed"]:
                if col in df.columns:
                    new_cols[f"rollsum_{col}_{w}"] = _rolling_sum(df, col, w).values

        new_df = pd.DataFrame(new_cols, index=df.index)
        df = pd.concat([df, new_df], axis=1)

        for w in WINDOWS_PITCH:
            hr = pd.Series(
                new_cols.get(f"rollsum_hr_{w}", np.zeros(len(df))), index=df.index
            )
            bf = pd.Series(
                new_cols.get(f"rollsum_bfp_{w}", np.zeros(len(df))), index=df.index
            )
            fb = pd.Series(
                new_cols.get(f"rollsum_fly_balls_{w}", np.zeros(len(df))),
                index=df.index,
            )
            bat = pd.Series(
                new_cols.get(f"rollsum_batted_balls_allowed_{w}", np.zeros(len(df))),
                index=df.index,
            )

            df[f"hr_per_bf_{w}"] = hr / bf.replace(0, np.nan)
            df[f"fb_perc_{w}"] = fb / bat.replace(0, np.nan)

        pitcher_feat_rows.append(df)

    if pitcher_feat_rows:
        pitcher_features = pd.concat(pitcher_feat_rows, ignore_index=True)
        log.info(f"Computed pitcher features: {len(pitcher_features)} rows")
        insert_fn(conn, pitcher_features, "PITCHER_ROLLING_FEATURES")
        log.info(f"{year}: inserted {len(pitcher_features)} pitcher feature rows")
        del pitcher_features, pitcher_feat_rows

    conn.close()
    log.info("Rolling features complete")
    return "success"


def check_todays_lineups():
    """Log how many of today's games have confirmed lineups."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resp = requests.get(
        f"{MLB_API}/schedule",
        params={"sportId": 1, "date": today, "hydrate": "lineups"},
        timeout=15,
    )
    resp.raise_for_status()
    games = resp.json().get("dates", [{}])[0].get("games", [])
    total = len(games)
    confirmed = sum(1 for g in games if g.get("lineups", {}).get("homePlayers"))
    log.info(f"Todays lineups: {confirmed}/{total} confirmed")
    return confirmed, total
