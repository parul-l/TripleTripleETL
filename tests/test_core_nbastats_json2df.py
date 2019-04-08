import copy

import pandas.api.types as ptypes
import pyarrow as pa
import unittest

from triple_triple_etl.core.nbastats_json2df import (
    convert_data_to_df,
    get_play_by_play,
    get_boxscore
)
from tests.fixtures.mock_nbastats_data import (
    mock_data_pbp,
    mock_data_bs_traditional,
    mock_data_bs_player,
    mock_df_uploaded
)


class TestNBAStatsJson2Df(unittest.TestCase):
    """Tests for nbastats_json2df.py """

    def test_convert_data_to_df(self):
        df = convert_data_to_df(mock_data_bs_traditional)
        # check season was added
        self.assertTrue(expr='season' in df.columns)
    

    def test_get_play_by_play(self):
        table = get_play_by_play(mock_data_pbp)
        df = table.to_pandas()

        col_object = [
            'game_id', 'wctimestring', 'pctimestring', 'homedescription',
            'neutraldescription', 'visitordescription', 'score', 'scoremargin', 
            'player1_name', 'player1_team_city', 'player1_team_nickname',
            'player1_team_abbreviation', 'player2_name', 'player2_team_city',
            'player2_team_nickname', 'player2_team_abbreviation', 'player3_name',
            'player3_team_city', 'player3_team_nickname', 'player3_team_abbreviation',
            'season',
        ]
        col_num = [
            'eventnum', 'eventmsgtype', 'eventmsgactiontype',
            'person1type', 'player1_id', 'player1_team_id', 
            'person2type', 'player2_id', 'player2_team_id',
            'person3type', 'player3_id', 'player3_team_id',
            'period', 'video_available_flag'
        ]
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )
        self.assertEqual(
            first=df.columns.shape[0],
            second=len(col_object + col_num)
        )
    
    def test_get_boxscore_traditional(self):
        table = get_boxscore(
            bs_data=mock_data_bs_traditional,
            traditional_player='traditional'
        )
        df = table.to_pandas()
        col_object = [
            'game_id', 'team_abbreviation', 'team_city',
            'player_name', 'start_position', 'comment',
            'minutes', 'team_name', 'starters_bench', 'season'
        ]
        col_num = [
            'team_id', 'player_id', 'fgm', 'fga', 'fg_pct', 'fg3m',
            'fg3a', 'fg3_pct', 'ftm', 'fta', 'ft_pct', 'oreb', 'dreb',
            'reb', 'ast', 'stl', 'blk', 'to', 'pf', 'pts', 'plus_minus'
        ]
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )
        self.assertEqual(
            first=df.columns.shape[0],
            second=len(col_object + col_num)
        )

    def test_get_boxscore_player(self):
        table = get_boxscore(
            bs_data=mock_data_bs_player,
            traditional_player='player'
        )
        df = table.to_pandas()
        col_object = [
            'game_id',
            'team_abbreviation',
            'team_city',
            'player_name',
            'start_position',
            'comment',
            'minutes',
            'team_name',
            'season'
        ]
        col_num = [
            'team_id', 'player_id', 'spd', 'dist', 'orbc', 'drbc', 'rbc',
            'tchs', 'sast', 'ftast', 'passes', 'ast', 'cfgm', 'cfga',
            'cfg_pct', 'ufgm', 'ufga', 'ufg_pct', 'fg_pct', 'dfgm', 'dfga', 'dfg_pct',
        ]
        self.assertTrue(
            all(ptypes.is_string_dtype(df[col])
                for col in col_object)
        )
        self.assertTrue(
            all(ptypes.is_numeric_dtype(df[col])
                for col in col_num)
        )
        self.assertEqual(
            first=df.columns.shape[0],
            second=len(col_object + col_num)
        )


if __name__ == '__main__':
    unittest.main()