-- Run this in Snowflake once to set up team game logs tables.
-- Stores batting and pitching game logs from Baseball Reference via pybaseball.team_game_logs()

-- 1. Create the database and schema (if not already created by pitcher setup)
CREATE DATABASE IF NOT EXISTS BASEBALL;
CREATE SCHEMA IF NOT EXISTS BASEBALL.HISTORICAL;

-- Set session context so all subsequent statements resolve correctly
USE DATABASE BASEBALL;
USE SCHEMA HISTORICAL;

-- Team Batting Game Logs
CREATE TABLE IF NOT EXISTS BASEBALL.HISTORICAL.TEAM_BATTING_LOGS (
    -- Identifiers
    TEAM                        VARCHAR(5),
    SEASON                      INTEGER,
    DATE                        DATE,
    
    -- Innings
    INN                        FLOAT,
    
    -- Hits & Doubles/Triples/HR
    H                           FLOAT,
    X2B                         FLOAT,
    X3B                         FLOAT,
    HR                          FLOAT,
    
    -- Runs
    R                           FLOAT,
    RBI                         FLOAT,
    BB                          FLOAT,
    IBB                         FLOAT,
    SO                          FLOAT,
    HBP                         FLOAT,
    
    -- Bases
    SB                          FLOAT,
    CS                          FLOAT,
    LOB                         FLOAT,

    OPPSTART                    VARCHAR(100),
    OPPSTARTTHROWS              VARCHAR(1),
    
    -- Batters
    PA                         FLOAT,
    AB                         FLOAT,
    BA                         FLOAT,
    OBP                        FLOAT,
    SLG                        FLOAT,
    OPS                        FLOAT,
    TB                         FLOAT,
    GIDP                       FLOAT,
    SH                         FLOAT,
    SF                         FLOAT,
    ROE                        FLOAT,
    BABIP                      FLOAT,
    
    
    -- Pipeline metadata
    _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                    VARCHAR(50) DEFAULT 'pybaseball_team_game_logs_batting'
);

-- Team Pitching Game Logs
CREATE TABLE IF NOT EXISTS BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS (
    -- Identifiers
    TEAM                        VARCHAR(5),
    SEASON                      INTEGER,
    DATE                        DATE,
    OPP                         VARCHAR(5),
    RESULT                      VARCHAR(1),
    
    -- Innings
    INN                         FLOAT,
    IP                          FLOAT,

    RS                          FLOAT,
    RA                          FLOAT,
    
    -- Hits, Runs, Doubles/Triples/HR
    H                           FLOAT,
    R                           FLOAT,
    X2B                         FLOAT,
    X3B                         FLOAT,
    HR                          FLOAT,
    
    ER                          FLOAT,
    RBI                         FLOAT,
    BB                          FLOAT,
    IBB                         FLOAT,
    SO                          FLOAT,
    HBP                         FLOAT,
    BK                          FLOAT,
    WP                          FLOAT,
    BF                          FLOAT,
    ERA                         FLOAT,
    FIP                         FLOAT,
    GB                          FLOAT,
    FB                          FLOAT,
    LD                          FLOAT,
    PU                          FLOAT,
    IR                          FLOAT,
    "IS"                        FLOAT,
    
    -- Bases
    SB                         FLOAT,
    CS                         FLOAT,
    PO                         FLOAT,

    -- Batters
    AB                         FLOAT,
    GIDP                       FLOAT,
    SF                         FLOAT,
    ROE                        FLOAT,
    BABIP                      FLOAT,
    
    
    -- Pipeline metadata
    _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                    VARCHAR(50) DEFAULT 'pybaseball_team_game_logs_pitching'
);
