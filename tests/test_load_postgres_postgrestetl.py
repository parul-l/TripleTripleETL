import mock

import unittest


from triple_triple_etl.constants import config
from triple_triple_etl.load.postgres.postgres_etl import (
    csv2postgres,
    PostgresETL,
    get_connection,
    get_cursor
)

from tests.helper_methods import create_mock_context_manager


class TestPostgresELT(unittest.TestCase):
    """Tests for postgres_etl.py"""

    def test_get_connection(self):
        connection_mock = mock.Mock()

        psycopg2_mock = mock.Mock()
        psycopg2_mock.connect.return_value = connection_mock

        with mock.patch('triple_triple_etl.load.postgres.postgres_etl.psycopg2', psycopg2_mock):
            with get_connection():
                pass

        connection_mock.close.assert_called_once_with()
        psycopg2_mock.connect.assert_called_once_with(
            **config['postgres']
        )
        assert connection_mock.autocommit is True

    def test_get_cursor(self):
        cursor_mock = mock.Mock()
        connection_mock = mock.Mock()
        connection_mock.cursor.return_value = cursor_mock
        get_connection_mock = create_mock_context_manager([connection_mock])

        path = 'triple_triple_etl.load.postgres.postgres_etl.get_connection'
        with mock.patch(path, get_connection_mock):
            with get_cursor() as cursor:
                cursor.thing()

        get_connection_mock.assert_called_once_with()
        connection_mock.cursor.assert_called_once_with()
        cursor_mock.close.assert_called_once_with()
        cursor_mock.thing.assert_called_once_with()

    def test_csv2postgres(self):
        mock_file = mock.Mock()
        cursor_mock = mock.Mock()
        get_cursor_mock = create_mock_context_manager([cursor_mock])

        patches = {
            'get_cursor': get_cursor_mock,
            'open': create_mock_context_manager([mock_file])
        }

        path = 'triple_triple_etl.load.postgres.postgres_etl'
        with mock.patch.multiple(path, create=True, **patches):
            csv2postgres(filepath='some.csv')

        cursor_mock.copy_from.assert_called_once_with(mock_file, 'some', sep=',', null='')
        mock_file.readline.assert_called_once()


if __name__ == '__main__':
    unittest.main()
