"""
historical_team_utils.py
=========================
Helper functions for historical_team_batting_logs and historical_team_pitching_logs Airflow DAGs
"""

from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from pybaseball import cache
from utils.bref import BRefSession

logger = logging.getLogger(__name__)

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
  "STL", "TBD", "WSN", "ATH"
]

cache.enable()

_BREF_URL = "https://www.baseball-reference.com/teams/tgl.cgi?team={}&t={}&year={}"

SESSION = BRefSession()

def postprocess_game_batting_data(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data

    # Map scraped columns to database schema
    column_mapping = {
        ('Team', ''): 'Team',
        ('Season', ''): 'Season',
        ('Unnamed: 2_level_0', 'Date'): 'Date',

        ('Batting Stats', 'H'): 'H',
        ('Batting Stats', '2B'): 'X2B',
        ('Batting Stats', '3B'): 'X3B',
        ('Batting Stats', 'HR'): 'HR',

        ('Batting Stats', 'R'): 'R',

        ('Batting Stats', 'RBI'): 'RBI',
        ('Batting Stats', 'BB'): 'BB',
        ('Batting Stats', 'IBB'): 'IBB',
        ('Batting Stats', 'SO'): 'SO',
        ('Batting Stats', 'HBP'): 'HBP',

        ('Batting Stats', 'SB'): 'SB',
        ('Batting Stats', 'CS'): 'CS',

        ('Batting Stats', 'LOB'): 'LOB',

        ('Opp Starter', 'Player'): 'OppStart',
        ('Opp Starter', 'T'): 'OppStartThrows',

        ('Score', 'Inn'): 'Inn',

        ('Batting Stats', 'PA'): 'PA',
        ('Batting Stats', 'AB'): 'AB',
        ('Batting Stats', 'BA'): 'BA',
        ('Batting Stats', 'OBP'): 'OBP',
        ('Batting Stats', 'SLG'): 'SLG',
        ('Batting Stats', 'OPS'): 'OPS',
        ('Batting Stats', 'TB'): 'TB',
        ('Batting Stats', 'GIDP'): 'GIDP',
        ('Batting Stats', 'SH'): 'SH',
        ('Batting Stats', 'SF'): 'SF',
        ('Batting Stats', 'ROE'): 'ROE',
        ('Batting Stats', 'BAbip'): 'BABIP',
    }
    
    # Filter to only columns present in the mapping
    valid_cols = [col for col in data.columns if col in column_mapping]
    data = data[valid_cols]
    
    # Directly assign flat column names — rename() doesn't handle tuple columns correctly
    data.columns = [column_mapping[col] for col in data.columns]
    
    # Convert numeric columns
    numeric_cols = data.columns.difference(['Team', 'Season', 'Date', 'OppStart', 'OppStartThrows'])
    data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric, errors="coerce")
    data = data.dropna(subset=numeric_cols, how='all')  # drop pure header rows

    # Force dtypes so Snowflake doesn't see any object/variant columns
    for col in numeric_cols:
        data[col] = data[col].astype('float64')
    
    return data.reset_index(drop=True)

def postprocess_game_pitching_data(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data

    # Map scraped columns to database schema
    column_mapping = {
        ('Team', ''): 'Team',
        ('Season', ''): 'Season',
        ('Unnamed: 2_level_0', 'Date'): 'Date',

        ('Unnamed: 4_level_0', 'Opp'): 'Opp',
        
        ('Score', 'Inn'): 'Inn',
        ('Score', 'RS'): 'RS',
        ('Score', 'RA'): 'RA',

        ('Pitching Stats', 'IP'): 'IP',
        ('Pitching Stats', 'H'): 'H',
        ('Pitching Stats', 'R'): 'R',
        ('Pitching Stats', 'ER'): 'ER',
        ('Pitching Stats', 'HR'): 'HR',
        ('Pitching Stats', 'BB'): 'BB',
        ('Pitching Stats', 'IBB'): 'IBB',
        ('Pitching Stats', 'SO'): 'SO',
        ('Pitching Stats', 'HBP'): 'HBP',
        ('Pitching Stats', 'BK'): 'BK',
        ('Pitching Stats', 'WP'): 'WP',
        ('Pitching Stats', 'BF'): 'BF',
        ('Pitching Stats', 'ERA'): 'ERA',
        ('Pitching Stats', 'FIP'): 'FIP',
        ('Pitching Stats', 'GB'): 'GB',
        ('Pitching Stats', 'FB'): 'FB',
        ('Pitching Stats', 'LD'): 'LD',
        ('Pitching Stats', 'PU'): 'PU',
        ('Pitching Stats', 'PU'): 'PU',
        ('Pitching Stats', 'IR'): 'IR',
        ('Pitching Stats', 'IS'): 'Inh_Score',
        ('Pitching Stats', 'SB'): 'SB',
        ('Pitching Stats', 'CS'): 'CS',
        ('Pitching Stats', 'PO'): 'PO',
        ('Pitching Stats', 'AB'): 'AB',
        ('Pitching Stats', '2B'): 'X2B',
        ('Pitching Stats', '3B'): 'X3B',
        ('Pitching Stats', 'GIDP'): 'GIDP',
        ('Pitching Stats', 'SF'): 'SF',
        ('Pitching Stats', 'ROE'): 'ROE',
        ('Pitching Stats', 'BAbip'): 'BABIP',

    }
    
    # Filter to only columns present in the mapping
    valid_cols = [col for col in data.columns if col in column_mapping]
    data = data[valid_cols]
    
    # Directly assign flat column names — rename() doesn't handle tuple columns correctly
    data.columns = [column_mapping[col] for col in data.columns]
    
    # Convert numeric columns
    numeric_cols = data.columns.difference(['Team', 'Season', 'Date', 'Opp'])
    data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric, errors="coerce")
    data = data.dropna(subset=numeric_cols, how='all')  # drop pure header rows

    # Force dtypes so Snowflake doesn't see any object/variant columns
    for col in numeric_cols:
        data[col] = data[col].astype('float64')
    
    return data.reset_index(drop=True)

def get_game_data_by_team(team, season, stat_type):
    t_param = "b" if stat_type == "batting" else "p"
    data = pd.DataFrame()
    try:
        content = SESSION.get(_BREF_URL.format(team, t_param, season)).content
        
        soup = BeautifulSoup(content, 'lxml')
        table_id = "players_standard_{}".format(stat_type)
        table = soup.find('table', {'id': table_id})
        if table is None:
            logger.info(f'Table with id {table_id} not found on page')
        data = pd.read_html(StringIO(str(table)))[0]
        data.dropna(subset=['Date'], inplace=True) # remove rows with missing dates (mostly these are the Total rows adding all values for given column)
        
        if not data.empty:
            data['Team'] = team
            data['Season'] = season
            logger.info(f'Game {stat_type} data loaded for {team} in {season}')
    except Exception as error:
        logger.info(f'Unable to load game {stat_type} data for {team} in {season}')
    if data is None or data.empty:
        return data
    return postprocess_game_batting_data(data) if t_param == 'b' else postprocess_game_pitching_data(data)