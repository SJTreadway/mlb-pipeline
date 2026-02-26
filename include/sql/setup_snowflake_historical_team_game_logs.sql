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
    Team                        VARCHAR(5),
    Season                      INTEGER,
    Game                        INTEGER,
    Date                        DATE,
    Home                        BOOLEAN,
    
    -- Game Result
    W                           INTEGER,
    L                           INTEGER,
    T                           INTEGER,
    Win                         INTEGER,
    Loss                        INTEGER,
    Save                        VARCHAR(10),
    
    -- Runs
    R                           INTEGER,
    RA                          INTEGER,
    
    -- Hits & Doubles/Triples/HR
    H                           INTEGER,
    X2B                         INTEGER,
    X4B                         INTEGER,
    HR                          INTEGER,
    
    -- RBIs & Runs
    RBI                        INTEGER,
    BB                          INTEGER,
    IBB                        INTEGER,
    SO                          INTEGER,
    HBP                         INTEGER,
    
    -- Bases
    SB                         INTEGER,
    CS                         INTEGER,
    
    -- LOB & Errors
    LOB                        INTEGER,
    E                          INTEGER,
    
    -- Pitchers (for decision)
    OppStart                   VARCHAR(100),
    
    -- Game Info
    "D/N"                      VARCHAR(1),
    Attend                     INTEGER,
    cLI                        FLOAT,
    WPA                        FLOAT,
    aLI                        FLOAT,
    "WPA+"                     FLOAT,
    "WPA-"                     FLOAT,
    cWPA                       FLOAT,
    "cLI+"                     FLOAT,
    "cLI-"                     FLOAT,
    
    -- Innings
    Inn                        INTEGER,
    
    -- Extra Innings
    X_Inn                      INTEGER,
    
    -- Batters
    BFP                        INTEGER,
    BF_Pit                     INTEGER,
    IR                         INTEGER,
    "IS"                       INTEGER,
    
    -- Pipeline metadata
    _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                    VARCHAR(50) DEFAULT 'pybaseball_team_game_logs_batting'
);

-- Team Pitching Game Logs
CREATE TABLE IF NOT EXISTS BASEBALL.HISTORICAL.TEAM_PITCHING_LOGS (
    -- Identifiers
    Team                        VARCHAR(5),
    Season                      INTEGER,
    Game                        INTEGER,
    Date                        DATE,
    Home                        BOOLEAN,
    
    -- Game Result
    W                           INTEGER,
    L                           INTEGER,
    T                           INTEGER,
    Win                         INTEGER,
    Loss                        INTEGER,
    Save                        VARCHAR(10),
    BlownSave                   VARCHAR(10),
    
    -- Runs
    R                           INTEGER,
    RA                          INTEGER,
    
    -- Hits & Doubles/Triples/HR
    H                           INTEGER,
    X2B                         INTEGER,
    X3B                         INTEGER,
    HR                          INTEGER,
    
    -- Walks & Strikeouts
    BB                          INTEGER,
    IBB                        INTEGER,
    SO                          INTEGER,
    HBP                         INTEGER,
    
    -- Wild Pitches & Balks
    WP                          INTEGER,
    BK                          INTEGER,
    
    -- Batters Faced
    BF                         INTEGER,
    
    -- Pitch Count
    Pit                        INTEGER,
    Str                        INTEGER,
    GSc                        INTEGER,
    
    -- Stolen Bases Allowed
    SB                         INTEGER,
    CS                         INTEGER,
    
    -- LOB
    LOB                        INTEGER,
    
    -- ERA
    ERA                       FLOAT,
    
    -- Game Info
    "D/N"                     VARCHAR(1),
    Attend                     INTEGER,
    cLI                        FLOAT,
    WPA                        FLOAT,
    aLI                        FLOAT,
    "WPA+"                     FLOAT,
    "WPA-"                     FLOAT,
    cWPA                       FLOAT,
    "cLI+"                     FLOAT,
    "cLI-"                     FLOAT,
    
    -- Innings
    IP                         FLOAT,
    Inn                        INTEGER,
    
    -- Extra Innings
    X_Inn                      INTEGER,
    
    -- Pipeline metadata
    _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                    VARCHAR(50) DEFAULT 'pybaseball_team_game_logs_pitching'
);
