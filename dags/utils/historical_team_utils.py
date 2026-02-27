"""
historical_team_utils.py
=========================
Helper functions for historical_team_batting_logs and historical_team_pitching_logs Airflow DAGs
"""

from __future__ import annotations

import logging
import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pybaseball import cache, season_game_logs

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
  "STL", "TBD", "WSN"
]

cache.enable()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

_URL = "https://www.baseball-reference.com/teams/tgl.cgi?team={}&t={}&year={}"

def postprocess_game_data(data: pd.DataFrame) -> pd.DataFrame:
    #print(data.columns)
    data.drop([('Unnamed: 0_level_0', 'Rk')], axis=1, inplace=True)  # drop index column
    data = data.iloc[:-1]
    repl_dict = {
        "Gtm" : "Game",
        "Unnamed: 3_level_1": "Home",
        "#": "NumPlayers",
        "Opp. Starter (GmeSc)": "OppStart",
        "Pitchers Used (Rest-GameScore-Dec)": "PitchersUsed"
    }
    data = data.rename(columns= repl_dict).copy()
    data[('Unnamed: 3_level_0','Home')] = data[('Unnamed: 3_level_0','Home')].isnull()  # '@' if away, empty if home
    data = data[data[('Unnamed: 1_level_0','Game')] != 'Gtm'].copy()  # drop empty month rows
    data = data.apply(pd.to_numeric, errors="ignore")
    data[('Unnamed: 1_level_0','Game')] = data[('Unnamed: 1_level_0','Game')].astype(int)
    return data.reset_index(drop=True)

def get_game_data_by_team(team, stat_type):
    game_df = pd.DataFrame()
    t_param = "b" if stat_type == "batting" else "p"
    for year in SEASONS:
        temp_df = pd.DataFrame()
        for attempt in range(3):
            try:
                response = requests.get(_URL.format(team, t_param, year), headers=HEADERS, timeout=30)
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}")
                
                soup = BeautifulSoup(response.text, 'html.parser')
                table_id = "players_standard_{}".format(stat_type)
                table = soup.find('table', {'id': table_id})
                if table is None:
                    raise RuntimeError(f"Table '{table_id}' not found on page")
                data = pd.read_html(StringIO(str(table)))[0]
                
                if data:
                    temp_df = pd.DataFrame(data)
                    temp_df['Team'] = team
                    temp_df['Season'] = year
                    game_df = pd.concat((game_df, temp_df), ignore_index=True)
                    logger.info(f'Game {stat_type} data loaded for {team} in {year}')
                break
            except Exception as error:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    logger.info(f'Retry {attempt + 1} for {team} {year}: {error}')
                else:
                    logger.info(f'Unable to load game {stat_type} data for {team} in {year} with error: {error}')
    return postprocess_game_data(game_df)

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