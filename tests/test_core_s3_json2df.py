import copy

import pandas.api.types as ptypes
import pyarrow as pa
import unittest


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
        table = get_player_info(self.data)

        # all columns exist and named correctly
        self.assertEqual(
            first=set(table.schema.names),
            second={
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
            }
        )
        # column types are accurate
        col_int = ['playerid', 'teamid']
        col_object = [
            'season',
            'gameid',
            'lastname',
            'firstname',
            'jersey',
            'position',
            'startdate',
            'enddate'
        ]

        object_idx = [table.schema.names.index(col) for col in col_object]
        self.assertTrue(
            all(pa.string() == table.schema.types[idx] for idx in object_idx)
        )

        int_idx = [table.schema.names.index(col) for col in col_int]
        self.assertTrue(
            all(pa.int64() == table.schema.types[idx] for idx in int_idx)
        )
        
        # only two team_ids
        self.assertEqual(first=len(set(table.column('teamid'))), second=2)

        # possible positions
        allowed_position = ['F', 'G', 'C', 'F-G', 'F-C']
        self.assertTrue(
            set(table.column('position')).issubset(allowed_position)
        )

    def test_get_team_info(self):
        table = get_team_info(self.data)
        df = table.to_pandas()

        # all columns exist and named correctly
        self.assertEqual(
            first=set(table.schema.names),
            second={
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
            }
        )
        # column types are accurate
        col_object = [
            'season',
            'gameid',
            'name',
            'conference',
            'division',
            'city',
            'state',
            'startdate',
            'enddate'
        ]
        
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )

        self.assertTrue(ptypes.is_numeric_dtype(df['teamid']))

        # only two team_ids
        self.assertEqual(first=len(set(df.teamid.values)), second=2)

    def test_get_game_info(self):
        table = get_game_info(self.data)
        df = table.to_pandas()

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
                'season',
                'gameid',
                'gamedate',
                'home_teamid',
                'visitor_teamid',
            }
        )


        # column types are accurate
        col_object = [
            'season',
            'gameid',
            'gamedate'
        ]
        col_int = [
            'home_teamid',
            'visitor_teamid'
        ]
        
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in col_object)
        )
        self.assertTrue(all(
            ptypes.is_numeric_dtype(df[col]) for col in col_int)
        )

    def test_get_game_position_info(self):
        table = get_game_position_info(self.data)
        df = table.to_pandas()

        # all columns exist and named correctly
        self.assertEqual(
            first=set(df.columns),
            second={
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
            }
        )
        # column types are accurate
        col_num = [
            'eventid',
            'moment_num',
            'period',
            'periodclock',
            'shotclock',
            'teamid',
            'playerid',
            'x_coordinate',
            'y_coordinate',
            'z_coordinate',
        ]

        self.assertTrue(
            all(ptypes.is_string_dtype(df[col]) for col in ['gameid', 'season'])
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col]) for col in col_num)
        )

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
