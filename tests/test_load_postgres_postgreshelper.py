import os
import unittest
import mock
import yaml

from tests.fixtures.mock_s3rawdata import data
from tests.helper_methods import create_mock_context_manager

from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.core.s3_json2csv import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)
from triple_triple_etl.load.postgres.postgres_helper import (
    get_primary_keys,
    csv2postgres_pkeys,
    csv2postgres_no_pkeys,
    save_all_tables
)


class TestPostgresHelper(unittest.TestCase):
    """Tests for postgres_helper.py"""

    def test_get_primary_keys(self):
        mock_schema = {
            'tables': {
                'games': {
                    'columns': [
                        {'name': 'id', 'constraints': ['primary key']},
                        {'name': 'start_time'},
                        {'name': 'home_team_id'},
                        {'name': 'visitor_team_id'}
                    ]
                }
            }
        }

        open_mock = create_mock_context_manager([yaml.dump(mock_schema)])

        path = 'triple_triple_etl.load.postgres.postgres_helper.open'
        with mock.patch(path, open_mock, create=True):
            ans = get_primary_keys(tablename='games')

        assert ans == ['id']

    def test_csv2postgres_pkeys(self):
        cursor_mock = mock.Mock()
        get_cursor_mock = create_mock_context_manager([cursor_mock])
        get_primary_keys_mock = mock.Mock(return_value=['key1', 'key2'])

        patches = {
            'get_cursor': get_cursor_mock,
            'get_primary_keys': get_primary_keys_mock,
            'open': create_mock_context_manager(['a_file'])
        }

        path = 'triple_triple_etl.load.postgres.postgres_helper'
        with mock.patch.multiple(path, create=True, **patches):
            csv2postgres_pkeys(
                tablename='something',
                filepath='some.txt',
            )

        cursor_mock.copy_expert.assert_called_once_with(mock.ANY, 'a_file')

    def test_csv2postgres_no_pkeys(self):
        mock_file = mock.Mock()
        cursor_mock = mock.Mock()
        get_cursor_mock = create_mock_context_manager([cursor_mock])

        patches = {
            'get_cursor': get_cursor_mock,
            'open': create_mock_context_manager([mock_file])
        }

        path = 'triple_triple_etl.load.postgres.postgres_helper'
        with mock.patch.multiple(path, create=True, **patches):
            csv2postgres_no_pkeys(filepath='some.csv')

        cursor_mock.copy_from.assert_called_once_with(mock_file, 'some', sep=',', null='')
        cursor_mock.execute.assert_called_once_with(mock.ANY)
        mock_file.readline.assert_called_once()
    
    def test_save_all_tables(self, input_data=data):
        all_tables = {
            'players': get_player_info(input_data),
            'teams': get_team_info(input_data),
            'games': get_game_info(input_data),
            'game_positions': get_game_position_info(input_data)            
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
