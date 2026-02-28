-- Run this in Snowflake once to set up your batter environment.
-- Snowflake free trial gives you $400 of credits - more than enough for this project.

-- 1. Create the database and schema (if not already created by pitcher setup)
CREATE DATABASE IF NOT EXISTS BASEBALL;
CREATE SCHEMA IF NOT EXISTS BASEBALL.STATCAST;

-- Set session context so all subsequent statements resolve correctly
USE DATABASE BASEBALL;
USE SCHEMA STATCAST;

-- 2. Create raw batters table for statcast_batter data
--    This uses pybaseball.statcast_batter() which returns player-specific data
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.RAW_BATTERS (
    -- Identifiers
    game_pk             INTEGER,
    game_date           DATE,
    game_year           INTEGER,
    game_type           VARCHAR(5),

    -- Pitch metadata
    pitch_number        INTEGER,
    inning              INTEGER,
    inning_topbot       VARCHAR(3),
    at_bat_number       INTEGER,
    pitch_name          VARCHAR(50),
    pitch_type          VARCHAR(5),

    -- Pitcher / batter
    pitcher             INTEGER,
    batter              INTEGER,
    player_name         VARCHAR(100),
    stand               VARCHAR(1),
    p_throws            VARCHAR(1),
    home_team           VARCHAR(5),
    away_team           VARCHAR(5),

    -- Pitch physics
    release_speed       FLOAT,
    release_spin_rate   FLOAT,
    release_extension   FLOAT,
    release_pos_x       FLOAT,
    release_pos_y       FLOAT,
    release_pos_z       FLOAT,
    pfx_x               FLOAT,
    pfx_z               FLOAT,
    plate_x             FLOAT,
    plate_z             FLOAT,
    vx0                 FLOAT,
    vy0                 FLOAT,
    vz0                 FLOAT,
    ax                  FLOAT,
    ay                  FLOAT,
    az                  FLOAT,
    effective_speed     FLOAT,
    spin_axis           FLOAT,

    -- Outcome
    description         VARCHAR(100),
    events              VARCHAR(100),
    type                VARCHAR(1),
    zone                INTEGER,
    hit_location        INTEGER,
    bb_type             VARCHAR(50),
    hc_x                FLOAT,
    hc_y                FLOAT,
    hit_distance_sc     FLOAT,
    launch_speed        FLOAT,
    launch_angle        FLOAT,
    launch_speed_angle  INTEGER,

    -- Expected stats
    estimated_ba_using_speedangle       FLOAT,
    estimated_woba_using_speedangle     FLOAT,
    woba_value          FLOAT,
    woba_denom          INTEGER,
    babip_value         FLOAT,
    iso_value           FLOAT,
    delta_home_win_exp  FLOAT,
    delta_run_exp       FLOAT,

    -- Count state
    balls               INTEGER,
    strikes             INTEGER,
    outs_when_up        INTEGER,
    on_1b               INTEGER,
    on_2b               INTEGER,
    on_3b               INTEGER,
    post_home_score     INTEGER,
    post_away_score     INTEGER,

    -- Pipeline metadata
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source             VARCHAR(50) DEFAULT 'pybaseball_statcast_batter'
);

-- 3. Deduplicated view for batters
CREATE OR REPLACE VIEW BASEBALL.STATCAST.BATTERS AS
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY game_pk, at_bat_number, pitch_number
               ORDER BY _loaded_at DESC
           ) AS rn
    FROM BASEBALL.STATCAST.RAW_BATTERS
)
WHERE rn = 1;

-- 4. Aggregated batter performance view
CREATE OR REPLACE VIEW BASEBALL.STATCAST.BATTER_STATS AS
SELECT
    game_year,
    batter,
    player_name,
    COUNT(*)                            AS pa,
    SUM(CASE WHEN events IS NOT NULL AND events != '' THEN 1 ELSE 0 END) AS ab,
    SUM(CASE WHEN events IN ('home_run') THEN 1 ELSE 0 END)              AS hr,
    SUM(CASE WHEN events IN ('single') THEN 1 ELSE 0 END)                AS single,
    SUM(CASE WHEN events IN ('double', 'double_play') THEN 1 ELSE 0 END) AS double,
    SUM(CASE WHEN events IN ('triple') THEN 1 ELSE 0 END)                AS triple,
    SUM(CASE WHEN events IN ('walk', 'hit_by_pitch') THEN 1 ELSE 0 END)  AS bb_hbp,
    SUM(CASE WHEN events IN ('strikeout', 'strikeout_double_play') THEN 1 ELSE 0 END) AS so,
    SUM(CASE WHEN type = 'B' THEN 1 ELSE 0 END)                          AS balls_seen,
    SUM(CASE WHEN type = 'S' THEN 1 ELSE 0 END)                          AS strikes_seen,
    ROUND(AVG(launch_speed), 1)                                         AS avg_exit_velo,
    ROUND(AVG(launch_angle), 1)                                         AS avg_launch_angle,
    ROUND(AVG(estimated_ba_using_speedangle), 3)                       AS avg_xba,
    ROUND(AVG(estimated_woba_using_speedangle), 3)                     AS avg_xwoba,
    ROUND(SUM(delta_run_exp), 2)                                        AS total_runs_added
FROM BASEBALL.STATCAST.BATTERS
WHERE batter IS NOT NULL
GROUP BY 1, 2, 3;
