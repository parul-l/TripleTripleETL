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
        lambda x: datetime.datetime.strptime(x, '%I:%M %p').strftime('%H:%M:%S')
    )
    # enforce datatypes
    dtype = {    
        'game_id':'object',
        'eventnum':'int64',
        'eventmsgtype':'int64',
        'eventmsgactiontype':'int64',
        'period':'int64',
        'wctimestring':'object',
        'pctimestring':'object',
        'homedescription':'object',
        'neutraldescription':'object',
        'visitordescription':'object',
        'score':'object',
        'scoremargin':'object',
        'person1type':'float64',
        'player1_id':'int64',
        'player1_name':'object',
        'player1_team_id':'float64',
        'player1_team_city':'object',
        'player1_team_nickname':'object',
        'player1_team_abbreviation':'object',
        'person2type':'int64',
        'player2_id':'int64',
        'player2_name':'object',
        'player2_team_id':'float64',
        'player2_team_city':'object',
        'player2_team_nickname':'object',
        'player2_team_abbreviation':'object',
        'person3type':'int64',
        'player3_id':'int64',
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
        traditional_player: 'traditional', # or 'player'
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
            'fgm': 'int64',
            'fga': 'int64',
            'fg_pct': 'float64',
            'fg3m': 'int64',
            'fg3a': 'int64',
            'fg3_pct': 'float64',
            'ftm': 'int64',
            'fta': 'int64',
            'ft_pct': 'float64',
            'oreb': 'int64',
            'dreb': 'int64',
            'reb': 'int64',
            'ast': 'int64',
            'stl': 'int64',
            'blk': 'int64',
            'to': 'int64',
            'pf': 'int64',
            'pts': 'int64',
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
            'team_id': 'int64',
            'team_abbreviation': 'object',
            'team_city': 'object',
            'player_id': 'float64',
            'player_name': 'object',
            'start_position': 'object',
            'comment': 'object',
            'minutes': 'object',
            'spd': 'float64',
            'dist': 'float64',
            'orbc': 'int64',
            'drbc': 'int64',
            'rbc': 'int64',
            'tchs': 'int64',
            'sast': 'int64',
            'ftast': 'int64',
            'passes': 'int64',
            'ast': 'int64',
            'cfgm': 'int64',
            'cfga': 'int64',
            'cfg_pct': 'float64',
            'ufgm': 'int64',
            'ufga': 'int64',
            'ufg_pct': 'float64',
            'fg_pct': 'float64',
            'dfgm': 'int64',
            'dfga': 'int64',
            'dfg_pct': 'float64',
            'team_name': 'object',
            'season': 'object',
        }

    df.rename(columns=rename_cols, inplace=True)
    df = df.astype(dtype)
    # convert to pyarrow table
    return pa.Table.from_pandas(df, preserve_index=False)

