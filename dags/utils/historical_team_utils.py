"""
historical_team_utils.py
=========================
Helper functions for historical_team_batting_logs and historical_team_pitching_logs Airflow DAGs
"""

from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pybaseball import cache, season_game_logs
from utils.bref import BRefSession

logger = logging.getLogger(__name__)

SEASONS = list[int](range(1980, 2026))

WINDOW_SIZE = [162, 90, 30]

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

_URL = "https://www.baseball-reference.com/teams/tgl.cgi?team={}&t={}&year={}"

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

def postprocess_retrosheet_event_data(event_df, season):
    event_df.drop(columns=[
        'date', 'game_num', 'misc', 'acquisition_info', 'visiting_team_game_num', 'home_team_game_num',
        'completion_info', 'forfeit_info', 'protest_info', 'visiting_line_score',
        'home_line_score', 'ump_home_name', 'ump_first_name', 'ump_second_name',
        'ump_third_name', 'ump_lf_name', 'ump_rf_name', 'visiting_manager_name',
        'home_manager_name', 'winning_pitcher_id', 'winning_pitcher_name', 'losing_pitcher_id',
        'losing_pitcher_name', 'save_pitcher_id', 'save_pitcher_name', 'game_winning_rbi_id',
        'game_winning_rbi_name',
    ], axis=1, inplace=True)

    event_df.rename(columns={
        'visiting_team': 'team_v',
        'visiting_team_league': 'league_v',
        'home_team': 'team_h',
        'home_team_league': 'league_h',
        'visiting_score': 'runs_v',
        'home_score': 'runs_h',
        'time_of_game_minutes': 'duration',
        # visiting team stats
        'visiting_abs': 'AB_v',
        'visiting_hits': 'H_v',
        'visiting_doubles': 'x2B_v',
        'visiting_triples': 'x3B_v',
        'visiting_homeruns': 'HR_v',
        'visiting_rbi': 'RBI_v',
        'visiting_sac_hits': 'sac_hits_v',
        'visiting_sac_flies': 'sac_flies_v',
        'visiting_hbp': 'HBP_v',
        'visiting_bb': 'BB_v',
        'visiting_iw': 'IBB_v',
        'visiting_k': 'K_v',
        'visiting_sb': 'SB_v',
        'visiting_cs': 'CS_v',
        'visiting_gdp': 'GDP_v',
        'visiting_ci': 'CI_v',
        'visiting_lob': 'LOB_v',
        'visiting_pitchers_used': 'num_pitchers_v',
        'visiting_individual_er': 'IER_v',
        'visiting_er': 'ER_v',
        'visiting_wp': 'WP_v',
        'visiting_balks': 'balks_v',
        'visiting_po': 'PO_v',
        'visiting_assists': 'assists_v',
        'visiting_errors': 'ERR_v',
        'visiting_pb': 'PB_v',
        'visiting_dp': 'x2P_v',
        'visiting_tp': 'x3P_v',
        'visiting_starting_pitcher_id': 'starting_pitcher_id_v',
        'visiting_starting_pitcher_name': 'starting_pitcher_name_v',
        'visiting_1_id': 'batter1_id_v',
        'visiting_2_id': 'batter2_id_v',
        'visiting_3_id': 'batter3_id_v',
        'visiting_4_id': 'batter4_id_v',
        'visiting_5_id': 'batter5_id_v',
        'visiting_6_id': 'batter6_id_v',
        'visiting_7_id': 'batter7_id_v',
        'visiting_8_id': 'batter8_id_v',
        'visiting_9_id': 'batter9_id_v',
        'visiting_1_name': 'batter1_name_v',
        'visiting_2_name': 'batter2_name_v',
        'visiting_3_name': 'batter3_name_v',
        'visiting_4_name': 'batter4_name_v',
        'visiting_5_name': 'batter5_name_v',
        'visiting_6_name': 'batter6_name_v',
        'visiting_7_name': 'batter7_name_v',
        'visiting_8_name': 'batter8_name_v',
        'visiting_9_name': 'batter9_name_v',
        'visiting_1_pos': 'batter1_pos_v',
        'visiting_2_pos': 'batter2_pos_v',
        'visiting_3_pos': 'batter3_pos_v',
        'visiting_4_pos': 'batter4_pos_v',
        'visiting_5_pos': 'batter5_pos_v',
        'visiting_6_pos': 'batter6_pos_v',
        'visiting_7_pos': 'batter7_pos_v',
        'visiting_8_pos': 'batter8_pos_v',
        'visiting_9_pos': 'batter9_pos_v',
        'visiting_manager_id': 'manager_v',
        # home team stats
        'home_abs': 'AB_h',
        'home_hits': 'H_h',
        'home_doubles': 'x2B_h',
        'home_triples': 'x3B_h',
        'home_homeruns': 'HR_h',
        'home_rbi': 'RBI_h',
        'home_sac_hits': 'sac_hits_h',
        'home_sac_flies': 'sac_flies_h',
        'home_hbp': 'HBP_h',
        'home_bb': 'BB_h',
        'home_iw': 'IBB_h',
        'home_k': 'K_h',
        'home_sb': 'SB_h',
        'home_cs': 'CS_h',
        'home_gdp': 'GDP_h',
        'home_ci': 'CI_h',
        'home_lob': 'LOB_h',
        'home_pitchers_used': 'num_pitchers_h',
        'home_individual_er': 'IER_h',
        'home_er': 'ER_h',
        'home_wp': 'WP_h',
        'home_balks': 'balks_h',
        'home_po': 'PO_h',
        'home_assists': 'assists_h',
        'home_errors': 'ERR_h',
        'home_pb': 'PB_h',
        'home_dp': 'x2P_h',
        'home_tp': 'x3P_h',
        'home_starting_pitcher_id': 'SP_h',
        'home_starting_pitcher_id': 'starting_pitcher_id_h',
        'home_starting_pitcher_name': 'starting_pitcher_name_h',
        'home_1_id': 'batter1_id_h',
        'home_2_id': 'batter2_id_h',
        'home_3_id': 'batter3_id_h',
        'home_4_id': 'batter4_id_h',
        'home_5_id': 'batter5_id_h',
        'home_6_id': 'batter6_id_h',
        'home_7_id': 'batter7_id_h',
        'home_8_id': 'batter8_id_h',
        'home_9_id': 'batter9_id_h',
        'home_1_name': 'batter1_name_h',
        'home_2_name': 'batter2_name_h',
        'home_3_name': 'batter3_name_h',
        'home_4_name': 'batter4_name_h',
        'home_5_name': 'batter5_name_h',
        'home_6_name': 'batter6_name_h',
        'home_7_name': 'batter7_name_h',
        'home_8_name': 'batter8_name_h',
        'home_9_name': 'batter9_name_h',
        'home_1_pos': 'batter1_pos_h',
        'home_2_pos': 'batter2_pos_h',
        'home_3_pos': 'batter3_pos_h',
        'home_4_pos': 'batter4_pos_h',
        'home_5_pos': 'batter5_pos_h',
        'home_6_pos': 'batter6_pos_h',
        'home_7_pos': 'batter7_pos_h',
        'home_8_pos': 'batter8_pos_h',
        'home_9_pos': 'batter9_pos_h',
        'home_manager_id': 'manager_h',
    }, inplace=True)

    event_df.rename(columns={'Season': 'season'}, inplace=True)

    team_data_dict = {}
    for team in event_df.team_v.unique():
        team_data_dict[team] = create_team_df(event_df, team)

    ## Create a variety of summarized statistics for each game
    ## For each game, we look up the home and visiting team in the team
    ## data dictionary, and then look up the game, and pull the relevant stats

    stats = ['BATAVG', 'OBP', 'SLG', 'OBS', 'SB', 'CS', 'ERR']
    teams = ['h', 'v']

    # Initialize arrays
    arrays = {f"{stat}_{window}_{team}": np.zeros(event_df.shape[0])
            for stat in stats for window in WINDOW_SIZE for team in teams}

    # Populate the arrays
    for i, (index, row) in enumerate(event_df.iterrows()):
        if i % 1000 == 0:
            print(i)

        home_team = row['team_h']
        visit_team = row['team_v']
        game_index = row['date_dblhead']

        for window in WINDOW_SIZE:
            for stat in stats:
                arrays[f'{stat}_{window}_h'][i] = team_data_dict[home_team].loc[game_index, f'rollsum_{stat}_{window}']
                arrays[f'{stat}_{window}_v'][i] = team_data_dict[visit_team].loc[game_index, f'rollsum_{stat}_{window}']

    # Add arrays to DataFrame
    for key, value in arrays.items():
        event_df[key] = value

    print(f'Postprocessing Retrosheet events data complete for season {season}')
    return event_df

# strip away suffix, e.g., '_h', '_v', for given column (retrosheet events data)
def strip_suffix(col, suffix):
  return col[:-len(suffix)] if col.endswith(suffix) else col

def get_game_data_by_team(team, season, stat_type):
    t_param = "b" if stat_type == "batting" else "p"
    data = pd.DataFrame()
    try:
        content = SESSION.get(_URL.format(team, t_param, season)).content
        
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

def get_season_game_logs(season):
    event_df = season_game_logs(season)
    event_df['season'] = season

    # add additional columns to dataframe
    event_df['run_diff'] = event_df['home_score'].copy() - event_df['visiting_score'].copy()
    event_df['home_victory'] = (event_df['run_diff'] > 0).astype(int)
    event_df['run_total'] = event_df['home_score'].copy() + event_df['visiting_score'].copy()
    event_df['date_dblhead'] = (event_df['date'].copy().astype(str) + event_df['game_num'].copy().astype(str)).astype(int)

    print(f'Event data loaded for {season}')
    return postprocess_retrosheet_event_data(event_df, season)


# create team dataframe to easily aggregate rolling window game data
def create_team_df(df, team):
    visiting_cols = [col for col in df.columns if not col.endswith('_h')]
    visiting_cols_stripped = [strip_suffix(col, '_v') for col in visiting_cols]
    home_cols = [col for col in df.columns if not col.endswith('_v')]
    home_cols_stripped = [strip_suffix(col, '_h') for col in home_cols]

    df_team_v = df[(df.team_v == team)]
    opp = df_team_v['team_h']
    df_team_v = df_team_v[visiting_cols]
    df_team_v.columns = visiting_cols_stripped
    df_team_v['home_game'] = 0
    df_team_v['opponent'] = opp

    df_team_h = df[(df.team_h == team)]
    opp = df_team_h['team_v']
    df_team_h = df_team_h[home_cols]
    df_team_h.columns = home_cols_stripped
    df_team_h['home_game'] = 1
    df_team_h['opponent'] = opp

    df_team = pd.concat((df_team_h, df_team_v))
    df_team.sort_values(['date_dblhead'], inplace=True)

    for winsize in WINDOW_SIZE:
        suff = str(winsize)
        for raw_col in ['AB', 'H', 'x2B', 'x3B', 'HR', 'BB', 'runs', 'SB', 'CS', 'ERR']:
            new_col = 'rollsum_' + raw_col + '_' + suff
            df_team[new_col] = df_team[raw_col].rolling(winsize, closed='left').sum()

        df_team['rollsum_BATAVG_' + suff] = df_team['rollsum_H_' + suff] / df_team['rollsum_AB_' + suff]
        df_team['rollsum_OBP_' + suff] = (df_team['rollsum_H_' + suff] + df_team['rollsum_BB_' + suff]) / (
        df_team['rollsum_BB_' + suff] + df_team['rollsum_AB_' + suff])
        df_team['rollsum_SLG_' + suff] = (df_team['rollsum_H_' + suff] + df_team['rollsum_x2B_' + suff] +
                                        2 * df_team['rollsum_x3B_' + suff] + 3 * df_team['rollsum_HR_' + suff]) / (
                                        df_team['rollsum_AB_' + suff])
        df_team['rollsum_OBS_' + suff] = df_team['rollsum_OBP_' + suff] + df_team['rollsum_SLG_' + suff]

    df_team.set_index('date_dblhead', inplace=True)
    return df_team