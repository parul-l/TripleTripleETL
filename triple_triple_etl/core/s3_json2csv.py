from datetime import datetime
import requests

# from bs4 import BeautifulSoup
import pandas as pd

from triple_triple_etl.log import get_logger
from triple_triple_etl.constants import DATATABLES_DIR

logger = get_logger()

# TODO: get_player_info: get start/end date of player on specific team
# TODO: get_team_info: get start/end date of teams and conference, division, city, state

# def get_team_history(wiki_url):
#     r = requests.get(wiki_url)
#     soup = BeautifulSoup(r.content)
#     table_classes = {'class': 'navbox wikitable'}
#     wikitables = soup.findAll("table", table_classes)
#
#     division = [
#         'Atlantic',
#         'Central',
#         'Southeast',
#         'Northwest',
#         'Pacific',
#         'Southwest'
#     ]
#
#     conference = ['Eastern Conference', 'Western Conference']
#
#     for row in wikitables[0].findAll(['tr'])[2:]:
#         print row.text  # need to figure out how to compartamentalize


def get_player_info(game_data_dict: dict):
    col_order = [
        'playerid',
        'firstname',
        'lastname', 
        'position',
        'teamid',
        'startdate',
        'enddate'
    ]

    # initiate list to collect home/visitor dataframes
    player_dfs = []
    # get home/visitor player_info
    logger.info('Getting home and visitor player info df')
    for loc in ['home', 'visitor']:
        df = pd.DataFrame(game_data_dict['events'][0][loc]['players'])
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


def get_team_info(game_data_dict):
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
        pd.DataFrame(team_data, columns=np.array(col_order)[[0, 1, -2, -1]]),
        pd.DataFrame(columns=col_order[2:6]) # adding null info for now
    ], axis=1)

    return df[col_order]


def get_game_info(game_data_dict):
    game_info = {
        'gameid': game_data_dict['gameid'],
        'starttime': datetime.strptime(game_data_dict['gamedate'], '%Y-%m-%d'),
        'home_teamid': game_data_dict['events'][0]['home']['teamid'],
        'visitor_teamid': game_data_dict['events'][0]['visitor']['teamid']
    }

    df = pd.DataFrame.from_dict([game_info])
    return df[['gameid', 'starttime', 'home_teamid', 'visitor_teamid']]


def get_game_position_info(game_data_dict):
    col_headers = [
        'game_id',
        'event_id',
        'time_stamp',
        'period',
        'period_clock',
        'shot_clock',
        'team_id',
        'player_id',
        'x_coordinate',
        'y_coordinate',
        'z_coordinate',
    ]
    game_id = game_data_dict['gameid']

    player_position_data = []
    for event in game_data_dict['events']:
        event_id = int(event['eventId'])
        moments = event['moments']
        for moment in moments:
            period = moment[0]
            time_stamp = datetime\
                .fromtimestamp(moment[1] / 1000.)
            period_clock = moment[2]
            shot_clock = moment[3]

            for player_data in moment[5]:
                player_position_data.append(
                    [game_id, event_id, time_stamp,
                     period, period_clock, shot_clock] +
                    player_data
                )
    return pd.DataFrame(player_position_data, columns=col_headers)


def get_all_s3_tables(game_data_dict):
    return {
        'players': get_player_info(game_data_dict),
        'teams': get_team_info(game_data_dict),
        'games': get_game_info(game_data_dict),
        'game_positions': get_game_position_info(game_data_dict)
    }
