import mock
import os

import unittest

from tests.fixtures.mock_rawdata import data
from tests.helper_methods import create_mock_context_manager
from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.core.s3_json2csv import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)
from triple_triple_etl.load.postgres.postgres_helper import (
    csv2postgres,
    save_all_tables
)


class TestPostgresHelper(unittest.TestCase):
    """Tests for postgres_helper.py"""

    def test_csv2postgres(self):
        mock_file = mock.Mock()
        cursor_mock = mock.Mock()
        get_cursor_mock = create_mock_context_manager([cursor_mock])

        patches = {
            'get_cursor': get_cursor_mock,
            'open': create_mock_context_manager([mock_file])
        }

        path = 'triple_triple_etl.load.postgres.postgres_helper'
        with mock.patch.multiple(path, create=True, **patches):
            csv2postgres(filepath='some.csv')

        cursor_mock.copy_from.assert_called_once_with(mock_file, 'some', sep=',', null='')
        mock_file.readline.assert_called_once()
    
    def test_save_all_tables(self, data=data):
        all_tables = {
            'players': get_player_info(data),
            'teams': get_team_info(data),
            'games': get_game_info(data),
            'game_positions': get_game_position_info(data)            
        }
        save_all_tables(all_tables)

        filenames = [
            'players.csv',
            'teams.csv',
            'games.csv',
            'game_positions.csv'
        ]
        for f in filenames:
            filepath = os.path.join(DATATABLES_DIR, f)
            self.assertTrue(os.path.exists(filepath))


if __name__ == '__main__':
    unittest.main()
