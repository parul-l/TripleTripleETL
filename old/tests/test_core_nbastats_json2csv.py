import unittest
import mock

import pandas.api.types as ptypes

from tests.fixtures.mock_nbastats_rawdata import (
    mock_play_data,
    mock_bs_traditional_data,
    mock_bs_player_tracking_data,
)
from triple_triple_etl.core.nbastats_json2csv import (
    time_in_seconds,
    score_in_int,
    get_df_play_by_play,
    get_df_box_score,
    get_all_nbastats_tables
)

class TestNBAStatsJson2Csv(unittest.TestCase):
    """Test for nbastats_json2csv.py"""

    def test_time_in_seconds(self):
        times = ['10:19', '5:00']
        
        self.assertEqual(
            first=time_in_seconds(times[0]),
            second=619
        )
        self.assertEqual(
            first=time_in_seconds(times[1]),
            second=300
        )

    def test_score_in_int(self):
        scores = ['2-4', None]
        self.assertEqual(
            first=score_in_int(scores[0]),
            second=(2, 4)
        )
        self.assertEqual(
            first=score_in_int(scores[1]),
            second=(None, None)
        )

    def test_get_df_play_by_play(self, data=mock_play_data):
        df = get_df_play_by_play(data)
        
        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                "game_id",
                "event_id",
                "event_msg_type",
                "event_msg_action_type",
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
            }
        )
        # column types are accurate
        col_object = [
            'game_id',
            'home_description',
            'neutral_description',
            'visitor_description',
            'player1_name',
            'player2_name',
            'player3_name'
        ]
        col_num = [
            'event_id',
            'event_msg_type',
            'event_msg_action_type',
            'period',
            'pctimestring',
            'score_visitor',
            'score_home',
            'player1_type',
            'player1_id',
            'player1_team_id',
            'player2_type',
            'player2_id',
            'player2_team_id',
            'player3_type',
            'player3_id',
            'player3_team_id'
        ]
        
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )
        self.assertTrue(ptypes.is_datetime64_any_dtype(df['wctimestring']))

    def test_get_df_box_score_traditional(
            self,
            data=mock_bs_traditional_data):
        df = get_df_box_score(data, traditional_or_playertracking=0)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'game_id',
                'team_id',
                'team_abbreviation',
                'team_city',
                'player_id',
                'player_name',
                'start_position',
                'comment',
                'minutes',
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
            }
        )
        # column types are accurate
        col_object = [
            'game_id',
            'team_abbreviation',
            'team_city',
            'player_name',
            'start_position',
            'comment'
        ]
        col_num = [col for col in list(df) if col not in col_object]

        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )

    def test_get_df_box_score_player_tracking(
            self,
            data=mock_bs_player_tracking_data):
        df = get_df_box_score(data, traditional_or_playertracking=1)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'game_id',
                'team_id',
                'team_abbreviation',
                'team_city',
                'player_id',
                'player_name',
                'start_position',
                'comment',
                'minutes',
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
            }
        )
        # column types are accurate
        col_object = [
            'game_id',
            'team_abbreviation',
            'team_city',
            'player_name',
            'start_position',
            'comment'
        ]
        col_num = [col for col in list(df) if col not in col_object]

        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )        


    def test_get_all_nbastats_tables(self):
        play_data_mock = mock.Mock()
        box_score_traditional_data_mock = mock.Mock()
        box_score_player_tracking_data_mock = mock.Mock()

        get_df_play_by_play_mock = mock.Mock()
        get_df_box_score_mock = mock.Mock()

        path = 'triple_triple_etl.core.nbastats_json2csv'
        patches = {
            'get_df_play_by_play': get_df_play_by_play_mock,
            'get_df_box_score': get_df_box_score_mock
        }

        with mock.patch.multiple(path, **patches):
            get_all_nbastats_tables(
                play_data=play_data_mock,
                box_score_traditional_data=box_score_traditional_data_mock,
                box_score_player_tracking_data=box_score_player_tracking_data_mock
            )
        get_df_play_by_play_mock.assert_called_once_with(play_data_mock)
        get_df_box_score_mock.assert_has_calls(
            [
                mock.call(box_score_traditional_data_mock, 0),
                mock.call(box_score_player_tracking_data_mock, 1)
            ],
            any_order=True
        )
if __name__ == '__main__':
    unittest.main()
