"""
historical_team_utils.py
====================
Helper functions for historical_team_batting_logs and historical_team_pitching_logs Airflow DAGs
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from pybaseball import team_game_logs, cache

logger = logging.getLogger(__name__)

# 2020 was shortened due to COVID - July 23 to Sept 27.
SEASONS = list(range(1980, datetime.now().year + 1))

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

def get_game_data_by_team(team, stat_type) -> pd.DataFrame:
    game_df = pd.DataFrame()
    for year in SEASONS:
        temp_df = pd.DataFrame()
        try:
            temp_df = team_game_logs(year, team, stat_type)
            temp_df['Team'] = team
            temp_df['Season'] = year
            game_df = pd.concat((game_df, temp_df))
            logger.info(f'Game {stat_type} data loaded for {team} in {year}')
        except RuntimeError as error:
            logger.info(f'Unable to load game {stat_type} data for {team} in {year}')
    return game_df