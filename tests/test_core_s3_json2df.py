import copy

import unittest
import pandas.api.types as ptypes

from tests.fixtures.mock_s3rawdata import data
from triple_triple_etl.core.s3_json2df import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)


class TestS3Json2Df(unittest.TestCase):
    """Tests for json2csv.py"""
    def setUp(self):
        self.data = copy.deepcopy(data)

    def test_get_player_info(self):
        df = get_player_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'playerid',
                'firstname',
                'lastname',
                'teamid',
                'position',
                'jersey',
                'startdate',
                'enddate'
            }
        )
        # column types are accurate
        col_int = ['playerid', 'teamid']
        col_object = [
            'lastname',
            'firstname',
            'jersey',
            'position'
        ]
        col_dt = ['startdate', 'enddate']

        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_datetime64_any_dtype(df[col]) for col in col_dt)
        )
        self.assertTrue(ptypes.is_numeric_dtype(df[col]) for col in col_int)

        # only two team_ids
        self.assertEqual(first=len(set(df.teamid.values)), second=2)

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
                'teamid',
                'name',
                'conference',
                'division',
                'city',
                'state',
                'startdate',
                'enddate'
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
        col_dt = ['startdate', 'enddate']
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_datetime64_any_dtype(df[col]) for col in col_dt)
        )
        self.assertTrue(ptypes.is_numeric_dtype(df['teamid']))

        # only two team_ids
        self.assertEqual(first=len(set(df.teamid.values)), second=2)

    def test_get_game_info(self):
        df = get_game_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'gameid',
                'gamedate',
                'home_teamid',
                'visitor_teamid',
            }
        )
        # column types are accurate
        self.assertTrue(ptypes.is_string_dtype(df['gameid']))
        self.assertTrue(all(
            ptypes.is_numeric_dtype(df[col]) for col in ['home_teamid', 'visitor_teamid']
        ))
        self.assertTrue(ptypes.is_datetime64_any_dtype(df['gamedate']))

    def test_get_game_position_info(self):
        df = get_game_position_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
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
            }
        )
        # column types are accurate
        col_num = [
            'eventid',
            'period',
            'periodclock',
            'shotclock',
            'teamid',
            'playerid',
            'x_coordinate',
            'y_coordinate',
            'z_coordinate',
        ]

        self.assertTrue(ptypes.is_string_dtype(df['gameid']))
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col]) for col in col_num)
        )
        self.assertTrue(ptypes.is_datetime64_any_dtype(df['timestamp']))

        # check ranges are correct
        self.assertTrue(set(df.period) < {1, 2, 3, 4})
        self.assertTrue(
            all(df.periodclock.values <= 720) and 
            all(df.periodclock.values >= 0)
        )
        self.assertTrue(
            all(df.shotclock.values <= 24) and
            all(df.shotclock.values >= 0)
        )
        self.assertTrue(
            all(df.x_coordinate.values <= 100) and
            all(df.x_coordinate.values >= -5)
        )
        self.assertTrue(
            all(df.y_coordinate.values <= 55) and
            all(df.y_coordinate.values >= -5)
        )



if __name__ == '__main__':
    unittest.main()
