from datetime import datetime
import pandas as pd


def time_in_seconds(time):
    t = time.split(':')
    return int(t[0]) * 60 + int(t[1])


def score_in_int(score):
    if score:
        return int(score.split('-')[0]), int(score.split('-')[1])
    else:
        return None, None

def get_df_play_by_play(play_data):
    headers_play_by_play = [
        "game_id",
        "event_id",
        "period",
        "wctimestring",
        "pctimestring",
        "home_description",
        "neutral_description",
        "visitor_description",
        "score_visitor",
        "score_home",
        "player1_type",
        "player1_id",
        "player1_name",
        "player1_team_id",
        "player2_type",
        "player2_id",
        "player2_name",
        "player2_team_id",
        "player3_type",
        "player3_id",
        "player3_name",
        "player3_team_id"
    ]

    play_by_play = []

    for each_play in play_data['resultSets'][0]['rowSet']:
        play_by_play.append([
            each_play[0],
            each_play[1],
            each_play[4],
            datetime.strptime(each_play[5], '%I:%M %p'),
            time_in_seconds(each_play[6]),
            each_play[7],
            each_play[8],
            each_play[9],
            score_in_int(each_play[10])[0],
            score_in_int(each_play[10])[1],
            each_play[12],
            each_play[13],
            each_play[14],
            each_play[15],
            each_play[19],
            each_play[20],
            each_play[21],
            each_play[22],
            each_play[26],
            each_play[27],
            each_play[28],
            each_play[29]]
        )
    # fillna with -1    
    na_values = {
        'player1_type': -1,
        'player2_type': -1,
        'player3_type': -1,
        'player1_team_id': -1,
        'player2_team_id': -1,
        'player3_team_id': -1

    }
    dtype_values = {
        'player1_type': 'int64',
        'player2_type': 'int64',
        'player3_type': 'int64',
        'player1_team_id': 'int64',
        'player2_team_id': 'int64',
        'player3_team_id': 'int64'

    }

    return pd.DataFrame(play_by_play, columns=headers_play_by_play)\
             .fillna(value=na_values)\
             .astype(dtype=dtype_values)


def get_df_box_score(box_score_data, traditional_or_playertracking=0):
    headers_common = [
        'game_id',
        'team_id',
        'team_abbreviation',
        'team_city',
        'player_id',
        'player_name',
        'start_position',
        'comment',
        'minutes'
    ]

    headers_box_score_traditional = [
       'field_goal_made_2',
        'field_goal_attempts_2',
        'field_goal_percent_2',
        'field_goal_made_3',
        'field_goal_attempts_3',
        'field_goal_percent_3',
        'free_throw_made',
        'free_throw_attempts',
        'free_throw_percent',
        'offensive_rebound',
        'defensive_rebound',
        'total_rebounds',
        'assists',
        'steals',
        'blocks',
        'turnovers',
        'personal_fouls',
        'points',
        'plus_minus'
    ]

    headers_box_score_player_tracking = [
        'avg_speed_mph',
        'distance_mph',
        'offensive_rebounds_chances',
        'defensive_rebounds_chances',
        'rebound_chances',
        'touches',
        'secondary_assists',
        'free_throw_assists',
        'passes',
        'assists',
        'contested_field_goals_made',
        'contested_field_goals_attempted',
        'contested_field_goals_percent',
        'uncontested_field_goals_made',
        'uncontested_field_goals_attempted',
        'uncontested_field_goals_percent',
        'field_goal_percent',
        'field_goals_defended_at_rim_made',
        'field_goals_defended_at_rim_attempted',
        'field_goals_defended_at_rim_percent'
    ]

    if traditional_or_playertracking == 0:
        headers = headers_common + headers_box_score_traditional
    elif traditional_or_playertracking == 1:
        headers = headers_common + headers_box_score_player_tracking

    df_box_score = pd.DataFrame(
        box_score_data['resultSets'][0]['rowSet'],
        columns=headers
    )
    df_box_score.minutes = pd.to_datetime(
        df_box_score.minutes, format='%M:%S'
    )
    df_box_score.minutes = df_box_score.minutes.map(
        lambda x: (x.minute) * 60 + x.second
    )

    return df_box_score


def get_all_nbastats_tables(
        play_data,
        box_score_traditional_data,
        box_score_player_tracking_data):
    return {
        'play_by_play': get_df_play_by_play(play_data),
        'box_score_traditional': get_df_box_score(box_score_traditional_data, 0),
        'box_score_player_tracking': get_df_box_score(box_score_player_tracking_data, 1)
    }
