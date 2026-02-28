import pandas as pd
import numpy as np
import requests
from io import StringIO

ODDSHARK_NUM_TO_TEAM_DICT = {
    26995 + i: team for i, team in enumerate([
        'PHI', 'SDN', 'SFN', 'ANA', 'DET', 'CIN', 'NYA', 'TEX', 'TBA', 'COL',
        'MIN', 'KCA', 'ARI', 'BAL', 'ATL', 'TOR', 'SEA', 'MIL', 'PIT', 'NYN',
        'LAN', 'OAK', 'WAS', 'CHA', 'SLN', 'CHN', 'BOS', 'MIA', 'HOU', 'CLE'
    ])
}

HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def line_to_prob(line):
    prob_underdog = 100/(np.abs(line)+100) # this is the probability for the underdog
    add_term = ((1-np.sign(line))/2) # 0 if negative, 1 if positive
    mult_factor = np.sign(line) # -1 if negative, 1 if positive
    # if line is positive, team is underdog, give 0 + 1*prob_underdog
    # if line is negative, team is favorites, give 1 + (-1)*prob_underdog
    imp_prob = add_term + mult_factor * prob_underdog
    return imp_prob

def get_historical_odds(event_df):
    df = event_df.copy()
    df_odds_dict = {}
    for i in range(26995, 27025):
        team_name = ODDSHARK_NUM_TO_TEAM_DICT[i]
        df_odds_dict[team_name] = {}
        for season in range(2021,2024):
            url = 'https://www.oddsshark.com/stats/gamelog/baseball/mlb/'+str(i)+'?season='+str(season)
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()  # Will raise error if still blocked
            df_temp = pd.read_html(StringIO(response.text))[0]
            df_temp = df_temp[df_temp.Game=='REG']
            df_temp['team_source'] = team_name
            df_temp['season'] = season
            df_temp['date_numeric'] = pd.to_datetime(df_temp.Date).astype(str).str.replace('-','')
            df_temp['game_no'] = np.arange(1,df_temp.shape[0]+1)
            df_temp['prob_implied'] = line_to_prob(df_temp['Line'])
            next_game_date = np.concatenate((df_temp['date_numeric'].iloc[1:],[0]))
            previous_game_date = np.concatenate(([0], df_temp['date_numeric'].iloc[:-1]))
            game_1_dblheader = (df_temp.date_numeric.to_numpy()==next_game_date).astype(int)
            game_2_dblheader = (df_temp.date_numeric.to_numpy()==previous_game_date).astype(int)*2
            df_temp['dblheader_num'] = game_1_dblheader+game_2_dblheader

            df_temp['date_dblhead'] = (df_temp.date_numeric.astype(str) + df_temp.dblheader_num.astype(str)).astype(int)
            df_temp.set_index('date_dblhead', inplace=True)
            df_odds_dict[team_name][season] = df_temp

    implied_prob_h = np.zeros(df.shape[0])
    implied_prob_v = np.zeros(df.shape[0])
    over_under = np.zeros(df.shape[0])
    ou_result = np.full(df.shape[0],'', dtype=object)
    for ind, row in df.iterrows():
        if row.season < 2022:
            continue
        else:
            season = row['season']
            home_team = row['team_h']
            visit_team = row['team_v']
            date_dblh = row['date_dblhead']
            try:
                implied_prob_h[ind] = df_odds_dict[home_team][season].loc[date_dblh,'prob_implied']
                over_under[ind] = df_odds_dict[home_team][season].loc[date_dblh,'Total']
                ou_result[ind] = df_odds_dict[home_team][season].loc[date_dblh,'OU']
            except KeyError:
                print(f'Game not found wrt home_team:{home_team} vs {visit_team} date_dbl {date_dblh}')
            try:
                implied_prob_v[ind] = df_odds_dict[visit_team][season].loc[date_dblh,'prob_implied']
            except KeyError:
                print(f'Game not found wrt visit_team:{visit_team} vs {home_team} date_dbl {date_dblh}')
    df['implied_prob_h'] = implied_prob_h
    df['implied_prob_v'] = implied_prob_v
    df['implied_prob_h_mid'] = (implied_prob_h + (1-implied_prob_v)) / 2
    df['over_under_line'] = over_under
    df['over_under_result'] = ou_result
    return df