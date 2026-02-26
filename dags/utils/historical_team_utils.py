"""
historical_team_utils.py
====================
Helper functions for historical_team_batting_logs and historical_team_pitching_logs Airflow DAGs
"""

from __future__ import annotations

import logging

import pandas as pd
from pybaseball import team_game_logs, season_game_logs, cache

logger = logging.getLogger(__name__)

# 2020 was shortened due to COVID - July 23 to Sept 27.
SEASONS = list[int](range(1980, 2026))

TEAMS = [
  "FLA", "LAA", "CAL", "MON",
  "MIL", "HOU", "ANA", "BAL",
  "BOS", "CHW", "CLE", "DET",
  "KCR", "MIN", "NYY", "OAK",
  "SEA", "TBR", "TEX", "TOR",
  "ARI", "ATL", "CHC", "CIN",
  "COL", "LAD", "SDP", "MIA",
  "NYM", "PHI", "PIT", "SFG",
  "STL", "TBD", "WSN"
]

cache.enable()

def get_game_data_by_team(team, stat_type):
    game_df = pd.DataFrame()
    for year in SEASONS:
        temp_df = pd.DataFrame()
        try:
            temp_df = team_game_logs(year, team, stat_type)
            temp_df['Team'] = team
            temp_df['Season'] = year
            game_df = pd.concat((game_df, temp_df))
            logger.info(f'Game {stat_type} data loaded for {team} in {year}')
        except Exception as error:
            logger.info(f'Unable to load game {stat_type} data for {team} in {year} with error: {error}')
    return game_df

def get_season_game_logs():
    event_df = pd.DataFrame()

    for year in SEASONS:
        try:
            temp_df = season_game_logs(year)
            temp_df['season'] = year
            event_df = pd.concat((event_df, temp_df))
            print(f'Event data loaded for {year}')
        except ValueError as error:
            print(error)

    # add additional columns to dataframe
    event_df['run_diff'] = event_df['home_score'].copy() - event_df['visiting_score'].copy()
    event_df['home_victory'] = (event_df['run_diff'] > 0).astype(int)
    event_df['run_total'] = event_df['home_score'].copy() + event_df['visiting_score'].copy()
    event_df['date_dblhead'] = (event_df['date'].copy().astype(str) + event_df['game_num'].copy().astype(str)).astype(int)
    return event_df