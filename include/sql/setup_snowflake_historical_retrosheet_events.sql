-- Run this in Snowflake once to set up Retrosheet events tables.

-- 1. Create the database and schema
CREATE DATABASE IF NOT EXISTS BASEBALL;
CREATE SCHEMA IF NOT EXISTS BASEBALL.HISTORICAL;

-- Set session context so all subsequent statements resolve correctly
USE DATABASE BASEBALL;
USE SCHEMA HISTORICAL;

-- Retrosheet Events via pybaseball
CREATE TABLE IF NOT EXISTS BASEBALL.HISTORICAL.RETROSHEET_EVENTS (
    -- Identifiers
    TEAM                        VARCHAR(5),
    SEASON                      INTEGER,
    DATE                        DATE,
    
    -- Pipeline metadata
    _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                    VARCHAR(50) DEFAULT 'pybaseball_retrosheet_events'
);