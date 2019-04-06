import datetime
import pandas as pd
import pyarrow as pa

from triple_triple_etl.log import get_logger


logger = get_logger()


def convert_data_to_df(data: dict, season: str = '2015-16'):
    all_dfs = [
        pd.DataFrame(
            data=games['rowSet'],
            columns=[x.lower() for x in games['headers']]
        ) 
        for games in data['resultSets']
    ]
    # join all dfs (usually just one), add season
    df = pd.concat(all_dfs, ignore_index=True, sort=False)
    df.loc[:, 'season'] = season

    return df


def get_play_by_play(play_data: dict, season: str = '2015-16'):
    logger.info('Get play by play data')

    df = convert_data_to_df(data=play_data, season=season)

    # convert timestamp
    df.loc[:, 'wctimestring'] = df.wctimestring.apply(
        lambda x: 
            datetime.datetime.strptime(x, '%I:%M %p').strftime('%H:%M:%S')
            if pd.notnull(x) else x
    )
    # enforce datatypes
    dtype = {    
        'game_id':'object',
        'eventnum':'float64',
        'eventmsgtype':'float64',
        'eventmsgactiontype':'float64',
        'period':'float64',
        'wctimestring':'object',
        'pctimestring':'object',
        'homedescription':'object',
        'neutraldescription':'object',
        'visitordescription':'object',
        'score':'object',
        'scoremargin':'object',
        'person1type':'float64',
        'player1_id':'float64',
        'player1_name':'object',
        'player1_team_id':'float64',
        'player1_team_city':'object',
        'player1_team_nickname':'object',
        'player1_team_abbreviation':'object',
        'person2type':'float64',
        'player2_id':'float64',
        'player2_name':'object',
        'player2_team_id':'float64',
        'player2_team_city':'object',
        'player2_team_nickname':'object',
        'player2_team_abbreviation':'object',
        'person3type':'float64',
        'player3_id':'float64',
        'player3_name':'object',
        'player3_team_id':'float64',
        'player3_team_city':'object',
        'player3_team_nickname':'object',
        'player3_team_abbreviation':'object'
    }
    df = df.astype(dtype)
    # convert to pyarrow table
    return pa.Table.from_pandas(df, preserve_index=False)


def get_boxscore(
        bs_data: dict,
        traditional_player: str = 'traditional', # or 'player'
        season: str = '2015-16'
):
    logger.info('Get {} box score data'.format(traditional_player))

    df = convert_data_to_df(data=bs_data, season=season)
    # convert minutes to datetime
    df.loc[:, 'min'] = df['min'].apply(lambda x: x.replace(':', '.'))
    
    if traditional_player == 'traditional':
        # rename min column to minutes (to avoid built-in min, pass)
        rename_cols = {'min': 'minutes'} 
        # enforce dtype       
        dtype = {
            'game_id': 'object',
            'team_id': 'int64',
            'team_abbreviation': 'object',
            'team_city': 'object',
            'player_id': 'float64',
            'player_name': 'object',
            'start_position': 'object',
            'comment': 'object',
            'minutes': 'object',
            'fgm': 'float64',
            'fga': 'float64',
            'fg_pct': 'float64',
            'fg3m': 'float64',
            'fg3a': 'float64',
            'fg3_pct': 'float64',
            'ftm': 'float64',
            'fta': 'float64',
            'ft_pct': 'float64',
            'oreb': 'float64',
            'dreb': 'float64',
            'reb': 'float64',
            'ast': 'float64',
            'stl': 'float64',
            'blk': 'float64',
            'to': 'float64',
            'pf': 'float64',
            'pts': 'float64',
            'plus_minus': 'float64',
            'team_name': 'object',
            'starters_bench': 'object',
            'season': 'object',
        }
    # if not traditional 
    elif traditional_player == 'player':
        rename_cols = {'min': 'minutes', 'pass': 'passes'} 
        dtype = {
            'game_id': 'object',
            'team_id': 'float64',
            'team_abbreviation': 'object',
            'team_city': 'object',
            'player_id': 'float64',
            'player_name': 'object',
            'start_position': 'object',
            'comment': 'object',
            'minutes': 'object',
            'spd': 'float64',
            'dist': 'float64',
            'orbc': 'float64',
            'drbc': 'float64',
            'rbc': 'float64',
            'tchs': 'float64',
            'sast': 'float64',
            'ftast': 'float64',
            'passes': 'float64',
            'ast': 'float64',
            'cfgm': 'float64',
            'cfga': 'float64',
            'cfg_pct': 'float64',
            'ufgm': 'float64',
            'ufga': 'float64',
            'ufg_pct': 'float64',
            'fg_pct': 'float64',
            'dfgm': 'float64',
            'dfga': 'float64',
            'dfg_pct': 'float64',
            'team_name': 'object',
            'season': 'object',
        }

    df.rename(columns=rename_cols, inplace=True)
    df = df.astype(dtype)
    # convert to pyarrow table
    return pa.Table.from_pandas(df, preserve_index=False)

