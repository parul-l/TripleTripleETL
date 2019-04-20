from datetime import datetime
import requests

import numpy as np
import pandas as pd
import pyarrow as pa

from triple_triple_etl.log import get_logger
from triple_triple_etl.constants import DATATABLES_DIR

logger = get_logger()

# TODO: get_player_info: get start/end date of player on specific team
# TODO: get_team_info: get start/end date of teams and conference, division, city, state


def get_player_info(game_data_dict: dict, season: str = '2015-2016'):
    col_order = [
        'season',
        'gameid',
        'playerid',
        'firstname',
        'lastname',
        'teamid',
        'position',
        'jersey',
        'startdate',
        'enddate'
    ]

    # initiate list to collect home/visitor dataframes
    player_dfs = []
    # get home/visitor player_info
    logger.info('Getting home and visitor player info df')
    for loc in ['home', 'visitor']:
        df = pd.DataFrame(data=game_data_dict['events'][0][loc]['players'])
        # add team id column to df
        df['teamid'] = game_data_dict['events'][0][loc]['teamid']
        # collect df
        player_dfs.append(df)

    # combine home/visitor dataframes
    df = pd.concat(player_dfs, axis=0)
    # add start/end_date columns
    df['startdate'] = '1970-01-01'
    df['enddate'] = '1970-01-01'

    # add gameid and season
    df['gameid'] = game_data_dict['gameid']
    df['season'] = season

    # enforce dtype
    dtype = {
        'season': 'object',
        'gameid': 'object',
        'playerid': 'int64',
        'firstname': 'object',
        'lastname': 'object',
        'teamid': 'int64',
        'position': 'object',
        'jersey': 'object',
        'startdate': 'object',
        'enddate': 'object',
    }

    df = df.astype(dtype)

    # convert to pyarrow table
    return pa.Table.from_pandas(df[col_order], preserve_index=False)


def get_team_info(game_data_dict: dict, season: str = '2015-2016'):
    col_order = [
        'season',
        'gameid',
        'teamid',
        'name',
        'conference',
        'division',
        'city',
        'state',
        'startdate',
        'enddate'
    ]
    logger.info('Getting home and visitor team info df')
    team_data = []
    for loc in ['home', 'visitor']:
        team_id = game_data_dict['events'][0][loc]['teamid']
        name = game_data_dict['events'][0][loc]['name']
        # add start/end date columns
        team_data.append([
            team_id,
            name,
            '1970-01-01',
            '1970-01-01'
        ])

    df = pd.concat([
        pd.DataFrame(data=team_data, columns=np.array(col_order[2:])[[0, 1, -2, -1]]),
        pd.DataFrame(columns=col_order[4:8]) # adding null info for now
    ], axis=1)

    # add gameid and season
    df['gameid'] = game_data_dict['gameid']
    df['season'] = season

    # enforce dtypes
    dtype = {
        'season': 'object',
        'gameid': 'object',
        'teamid': 'int64',
        'name': 'object',
        'conference': 'object',
        'division': 'object',
        'city': 'object',
        'state': 'object',
        'startdate': 'object',
        'enddate': 'object'
    }
    df = df.astype(dtype)

    # convert to pyarrow table
    return pa.Table.from_pandas(df[col_order], preserve_index=False)



def get_game_info(game_data_dict: dict, season: str = '2015-2016'):
    col_order = [
        'season',
        'gameid', 
        'gamedate', 
        'home_teamid', 
        'visitor_teamid'
    ]
    logger.info('Getting game info data')
    df = pd.DataFrame.from_dict([{
        'season': season,
        'gameid': game_data_dict['gameid'],
        'gamedate': game_data_dict['gamedate'], # leave date as string for athena queries
        'home_teamid': game_data_dict['events'][0]['home']['teamid'],
        'visitor_teamid': game_data_dict['events'][0]['visitor']['teamid']
    }])

    # enforce dtype
    dtype = {
        'season': 'object',
        'gameid': 'object', 
        'gamedate': 'object', 
        'home_teamid': 'int64', 
        'visitor_teamid': 'int64'
    }
    df = df.astype(dtype)

    # convert to pyarrow table
    return pa.Table.from_pandas(df[col_order], preserve_index=False)


def get_game_position_info(game_data_dict: dict, season: str = '2015-2016'):
    col_order = [
        'season',
        'gameid',
        'eventid',
        'moment_num',
        'timestamp_dts',
        'timestamp_utc',
        'period',
        'periodclock',
        'shotclock',
        'teamid',
        'playerid',
        'x_coordinate',
        'y_coordinate',
        'z_coordinate',
    ]
    logger.info('Getting game position data')
    gameid = game_data_dict['gameid']
    player_position_data = []

    for event in game_data_dict['events']:
        eventid = int(event['eventId'])
        moments = event['moments']
        for moment_num, moment in enumerate(moments):
            # moment info
            period = moment[0]
            timestamp_dts = datetime.fromtimestamp(moment[1] / 1000.).strftime('%F %TZ')
            timestamp_utc = moment[1]
            periodclock = moment[2]
            shotclock = moment[3]

            moment_data = [
                gameid, 
                eventid,
                moment_num,
                timestamp_dts,
                timestamp_utc, 
                period, 
                periodclock, 
                shotclock
            ]
            # add moment data to player position for given moment
            for player_data in moment[5]:
                player_position_data.append(moment_data + player_data)
    df = pd.DataFrame(data=player_position_data, columns=col_order[1:])
    # add season
    df['season'] = season

    # enforce datatype
    dtype = {
        'season': 'object',
        'gameid': 'object',
        'eventid': 'int64',
        'moment_num': 'int64',
        'timestamp_dts': 'object',
        'timestamp_utc': 'int64',
        'period': 'int64',
        'periodclock': 'float64',
        'shotclock': 'float64',
        'teamid': 'int64',
        'playerid': 'int64',
        'x_coordinate': 'float64',
        'y_coordinate': 'float64',
        'z_coordinate': 'float64'
    }
    df = df.astype(dtype)
    # convert to pyarrow table
    return pa.Table.from_pandas(df[col_order], preserve_index=False)
