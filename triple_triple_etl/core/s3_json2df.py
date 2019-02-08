from datetime import datetime
import requests

import numpy as np
import pandas as pd

from triple_triple_etl.log import get_logger
from triple_triple_etl.constants import DATATABLES_DIR

logger = get_logger()

# TODO: get_player_info: get start/end date of player on specific team
# TODO: get_team_info: get start/end date of teams and conference, division, city, state


def get_player_info(game_data_dict: dict):
    col_order = [
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
    df['startdate'] = datetime.strptime('1970-01-01', '%Y-%M-%d')
    df['enddate'] = datetime.strptime('1970-01-01', '%Y-%M-%d')

    return df[col_order]


def get_team_info(game_data_dict: dict):
    col_order = [
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
            datetime.strptime('1970-01-01', '%Y-%M-%d'),
            datetime.strptime('1970-01-01', '%Y-%M-%d'),
        ])

    df = pd.concat([
        pd.DataFrame(data=team_data, columns=np.array(col_order)[[0, 1, -2, -1]]),
        pd.DataFrame(columns=col_order[2:6]) # adding null info for now
    ], axis=1)

    return df[col_order]


def get_game_info(game_data_dict: dict):
    col_order = [
        'gameid', 
        'gamedate', 
        'home_teamid', 
        'visitor_teamid'
    ]
    logger.info('Getting game info data')
    df = pd.DataFrame.from_dict([{
        'gameid': game_data_dict['gameid'],
        'gamedate': datetime.strptime(game_data_dict['gamedate'], '%Y-%m-%d'),
        'home_teamid': game_data_dict['events'][0]['home']['teamid'],
        'visitor_teamid': game_data_dict['events'][0]['visitor']['teamid']
    }])

    return df[col_order]


def get_game_position_info(game_data_dict: dict):
    col_order = [
        'gameid',
        'eventid',
        'timestamp',
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
        for moment in moments:
            # moment info
            period = moment[0]
            timestamp = datetime.fromtimestamp(moment[1] / 1000.)
            periodclock = moment[2]
            shotclock = moment[3]

            moment_data = [
                gameid, 
                eventid, 
                timestamp, 
                period, 
                periodclock, 
                shotclock
            ]
            # add moment data to player position for given moment
            for player_data in moment[5]:
                player_position_data.append(moment_data + player_data)
    return pd.DataFrame(data=player_position_data, columns=col_order)
