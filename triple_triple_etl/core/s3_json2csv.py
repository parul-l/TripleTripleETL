from datetime import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd


from triple_triple_etl.constants import DATATABLES_DIR


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


def get_player_info(game_data_dict):
    col_headers = [
        'id',
        'last_name',
        'first_name',
        'team_id',
        'jersey_number',
        'position',
        'start_date',
        'end_date'
    ]

    player_dfs = []
    # get home/visitor player_info
    for loc in ['home', 'visitor']:
        df = pd.DataFrame.from_dict(
            game_data_dict['events'][0][loc]['players']
        )
        df['team_id'] = game_data_dict['events'][0][loc]['teamid']
        player_dfs.append(df)

    df = pd.concat(player_dfs).rename(columns={
        'firstname': 'first_name',
        'lastname': 'last_name',
        'playerid': 'id',
        'jersey': 'jersey_number'
    })

    # add start/end_date null columns
    df['start_date'] = datetime.strptime('1970-01-01', '%Y-%M-%d')
    df['end_date'] = datetime.strptime('1970-01-01', '%Y-%M-%d')
    return df[col_headers]


def get_team_info(game_data_dict):
    col_headers = [
        'id',
        'name',
        'conference',
        'division',
        'city',
        'state',
        'start_date',
        'end_date'
    ]
    team_data = []
    for loc in ['home', 'visitor']:
        team_id = game_data_dict['events'][0][loc]['teamid']
        name = game_data_dict['events'][0][loc]['name']
        team_data.append([team_id, name])
    df = pd.concat([
        pd.DataFrame(team_data, columns=col_headers[:2]),
        pd.DataFrame(columns=col_headers[2:6])
    ])
    df['start_date'] = datetime.strptime('1970-01-01', '%Y-%M-%d')
    df['end_date'] = datetime.strptime('1970-01-01', '%Y-%M-%d')

    return df[col_headers].astype({'id': 'int'})


def get_game_info(game_data_dict):
    game_info = {
        'id': game_data_dict['gameid'],
        'start_time': datetime.strptime(
            game_data_dict['gamedate'], '%Y-%m-%d'
        ),
        'home_team_id': game_data_dict['events'][0]['home']['teamid'],
        'visitor_team_id': game_data_dict['events'][0]['visitor']['teamid']
    }

    df = pd.DataFrame.from_dict([game_info])
    return df[['id', 'start_time', 'home_team_id', 'visitor_team_id']]


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
