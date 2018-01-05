import copy

import unittest
import pandas.api.types as ptypes


from tests.fixtures.mock_s3rawdata import data
from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.core.s3_json2csv import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info,
    get_all_tables_dict
)


class TestS3Json2Csv(unittest.TestCase):
    """Tests for json2csv.py"""
    def setUp(self):
        self.data = copy.deepcopy(data)

    def test_get_player_info(self):
        df = get_player_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'id',
                'last_name',
                'first_name',
                'team_id',
                'jersey_number',
                'position',
                'start_date',
                'end_date'
            }
        )
        # column types are accurate
        col_object = [
            'id',
            'last_name',
            'first_name',
            'jersey_number',
            'position'
        ]
        col_dt = ['start_date', 'end_date']

        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_datetime64_any_dtype(df[col]) for col in col_dt)
        )
        self.assertTrue(ptypes.is_numeric_dtype(df['team_id']))

        # only two team_ids
        self.assertEqual(first=len(set(df.team_id.values)), second=2)

        # possible positions
        allowed_position = ['F', 'G', 'C', 'F-G', 'F-C']
        self.assertTrue(
            set(df.position.values).issubset(allowed_position)
        )

    def test_get_team_info(self):
        df = get_team_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'id',
                'name',
                'conference',
                'division',
                'city',
                'state',
                'start_date',
                'end_date'
            }
        )
        # column types are accurate
        col_object = [
            'name',
            'conference',
            'division',
            'city',
            'state'
        ]
        col_dt = ['start_date', 'end_date']
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_datetime64_any_dtype(df[col]) for col in col_dt)
        )
        self.assertTrue(ptypes.is_numeric_dtype(df['id']))

        # only two team_ids
        self.assertEqual(first=len(set(df.id.values)), second=2)

    def test_get_game_info(self):
        df = get_game_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'id',
                'start_time',
                'home_team_id',
                'visitor_team_id',
            }
        )
        # column types are accurate
        self.assertTrue(ptypes.is_string_dtype(df['id']))
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col]) for col in ['home_team_id', 'visitor_team_id'])
        )
        self.assertTrue(ptypes.is_datetime64_any_dtype(df['start_time']))

    def test_get_game_position_info(self):
        df = get_game_position_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
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
            }
        )
        # column types are accurate
        col_num = [
            'event_id',
            'period',
            'period_clock',
            'shot_clock',
            'team_id',
            'player_id',
            'x_coordinate',
            'y_coordinate',
            'z_coordinate',
        ]

        self.assertTrue(ptypes.is_string_dtype(df['game_id']))
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col]) for col in col_num)
        )
        self.assertTrue(ptypes.is_datetime64_any_dtype(df['time_stamp']))

        # check ranges are correct
        self.assertTrue(set(df.period) < {1, 2, 3, 4})
        self.assertTrue(
            all(df.period_clock.values <= 720) and all(df.period_clock.values >= 0)
        )
        self.assertTrue(
            all(df.shot_clock.values <= 24) and
            all(df.shot_clock.values >= 0)
        )
        self.assertTrue(
            all(df.x_coordinate.values <= 100) and
            all(df.x_coordinate.values >= -5)
        )
        self.assertTrue(
            all(df.y_coordinate.values <= 55) and
            all(df.y_coordinate.values >= -5)
        )

    def test_get_all_tables_dict(self):
        all_tables = get_all_tables_dict(self.data)

        self.assertEqual(
            first=set(all_tables.keys()),
            second={'players', 'teams', 'games', 'game_positions'}
        )


if __name__ == '__main__':
    unittest.main()
