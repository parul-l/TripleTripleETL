import mock

import unittest

from triple_triple_etl.constants import config
from triple_triple_etl.load.postgres.postgres_connection import (
    get_connection,
    get_cursor
)
from tests.helper_methods import create_mock_context_manager


class TestPostgresConnectionCursor(unittest.TestCase):
    def test_get_connection(self):
        connection_mock = mock.Mock()

        psycopg2_mock = mock.Mock()
        psycopg2_mock.connect.return_value = connection_mock

        with mock.patch(
            'triple_triple_etl.load.postgres.postgres_connection.psycopg2', psycopg2_mock
        ):
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

        path = (
            'triple_triple_etl.load.postgres.'         
            'postgres_connection.get_connection'
        )
        with mock.patch(path, get_connection_mock):
            with get_cursor() as cursor:
                cursor.thing()

        get_connection_mock.assert_called_once_with()
        connection_mock.cursor.assert_called_once_with()
        cursor_mock.close.assert_called_once_with()
        cursor_mock.thing.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
        
