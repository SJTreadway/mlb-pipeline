-- include/sql/setup_statcast_features.sql

-- 1. Create the database and schema (if not already created by pitcher setup)
CREATE DATABASE IF NOT EXISTS BASEBALL;
CREATE SCHEMA IF NOT EXISTS BASEBALL.STATCAST;

-- Set session context so all subsequent statements resolve correctly
USE DATABASE BASEBALL;
USE SCHEMA STATCAST;

-- raw batter game rows
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.RAW_BATTER_GAMES (
    mlbam_id        INTEGER,
    game_date       DATE,
    game_pk         INTEGER,
    opponent        VARCHAR(10),
    is_home         INTEGER,
    stand           VARCHAR(1),
    p_throws        VARCHAR(1),
    opp_pitcher_id  INTEGER,
    opp_is_starter  INTEGER,
    age             FLOAT,
    ab              INTEGER,
    h               INTEGER,
    x2b             INTEGER,
    x3b             INTEGER,
    hr              INTEGER,
    bb              INTEGER,
    hbp             INTEGER,
    sf              INTEGER,
    hr_vs_r         INTEGER,
    ab_vs_r         INTEGER,
    hr_vs_l         INTEGER,
    ab_vs_l         INTEGER,
    batted_balls    INTEGER,
    ev_sum          FLOAT,
    hard_hits       INTEGER,
    sweet_spots     INTEGER,
    barrels         INTEGER,
    est_woba        FLOAT,
    est_slg         FLOAT,
    PRIMARY KEY (mlbam_id, game_date, game_pk)
);

-- raw pitcher game rows
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.RAW_PITCHER_GAMES (
    mlbam_id              INTEGER,
    game_date             DATE,
    game_pk               INTEGER,
    opponent              VARCHAR(10),
    is_home_pitcher       INTEGER,
    gs                    INTEGER,
    ip                    FLOAT,
    bfp                   INTEGER,
    hr                    INTEGER,
    r                     INTEGER,
    fly_balls             INTEGER,
    batted_balls_allowed  INTEGER,
    PRIMARY KEY (mlbam_id, game_date, game_pk)
);

-- game results
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.GAME_RESULTS (
    game_pk       INTEGER PRIMARY KEY,
    game_date     DATE,
    team_h        VARCHAR(10),
    team_v        VARCHAR(10),
    runs_h        INTEGER,
    runs_v        INTEGER,
    home_victory  INTEGER,
    run_diff      INTEGER
);

-- batter rolling features (rebuilt daily from raw)
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.BATTER_ROLLING_FEATURES (
    mlbam_id  INTEGER,
    game_date DATE,
    game_pk   INTEGER,
    -- raw cols carried through
    opponent  VARCHAR(10),
    is_home   INTEGER,
    stand     VARCHAR(1),
    p_throws  VARCHAR(1),
    opp_pitcher_id INTEGER,
    age       FLOAT,
    hr        INTEGER,
    -- rolling features added dynamically by Python
    PRIMARY KEY (mlbam_id, game_date, game_pk)
);

-- pitcher rolling features
CREATE TABLE IF NOT EXISTS BASEBALL.STATCAST.PITCHER_ROLLING_FEATURES (
    mlbam_id  INTEGER,
    game_date DATE,
    game_pk   INTEGER,
    opponent  VARCHAR(10),
    gs        INTEGER,
    PRIMARY KEY (mlbam_id, game_date, game_pk)
);