import unittest
import mock

from tests.helper_methods import create_mock_context_manager
from triple_triple_etl.load.postgres.s3_postgres_etl import (
    get_season,
    S3PostgresETL
)

class TestS3PostgresETLHelper(unittest.TestCase):
    def test_get_season(self):
        filename = '2015-2016/someteams'
        assert get_season(filename) == '2015-16'


class TestS3PostgresETL(unittest.TestCase):
    """Tests for s3_postgres_etl.py"""

    def test_extract_from_s3(self):
        schema_file_mock=mock.Mock()
        tempfile_mock = mock.Mock(return_value='1971-1972/filename')
        bucket_mock = mock.Mock(return_value='nba-player-positions')
        etl = S3PostgresETL(
            filename=tempfile_mock.return_value,
            schema_file=schema_file_mock
        )

        s3download_mock = mock.Mock(return_value='some.txt')
        extract2dir_mock = mock.Mock()

        tempfile_mock.mkdtemp.return_value = '/tmp/random_letters'

        patches = {
            'extract2dir': extract2dir_mock,
            's3download': s3download_mock,
            'tempfile': tempfile_mock
        }
        path = 'triple_triple_etl.load.postgres.s3_postgres_etl'
        with mock.patch.multiple(path, **patches):
            directory = etl.extract_from_s3()
        s3download_mock.assert_called_once_with(
            bucket_name=bucket_mock.return_value,
            filename=tempfile_mock.return_value
        )
        assert etl.filename == tempfile_mock.return_value
        assert directory == '/tmp/random_letters'

    def test_transform(self):
        filepath_mock = mock.Mock()
        os_mock = mock.Mock()
        os_mock.listdir.return_value = ['thingy']

        json_mock = mock.Mock()
        json_mock.load.return_value = {'gameid': 1}

        shutil_mock = mock.Mock()

        get_all_s3_tables_mock = mock.Mock(return_value='a_dict')
        save_all_tables_mock = mock.Mock()

        etl = S3PostgresETL(
            filename='1971-1972/filename',
            storage_dir='here',
            schema_file='some_schema_file')
        etl.tmp_dir = '/tmp/random'

        patches = {
            'os': os_mock,
            'json': json_mock,
            'get_all_s3_tables': get_all_s3_tables_mock,
            'save_all_tables': save_all_tables_mock,
            'shutil': shutil_mock,
            'open': create_mock_context_manager([filepath_mock])
        }
        path = 'triple_triple_etl.load.postgres.s3_postgres_etl'
        with mock.patch.multiple(path, **patches):
            etl.transform()

        assert etl.game_id == 1
        os_mock.path.join.assert_called_once_with('/tmp/random', 'thingy')
        get_all_s3_tables_mock.assert_called_once_with({'gameid': 1})
        save_all_tables_mock.assert_called_once_with(
            'a_dict',
            storage_dir='here'
        )
        shutil_mock.rmtree.assert_called_once_with('/tmp/random')

    def test_load(self):
        csv2postgres_no_pkeys_mock = mock.Mock()
        csv2postgres_pkeys_mock = mock.Mock()
        
        patches = {
            'csv2postgres_no_pkeys': csv2postgres_no_pkeys_mock,
            'csv2postgres_pkeys': csv2postgres_pkeys_mock
        }

        etl = S3PostgresETL(
            filename='1971-1972/filename',
            storage_dir='here',
            schema_file='some_schema_file'
        )

        path = 'triple_triple_etl.load.postgres.s3_postgres_etl'
    
        with mock.patch.multiple(path, **patches):
            etl.load(filepath='some.csv')
        csv2postgres_pkeys_mock.assert_called_once_with(
            tablename='some',
            filepath='some.csv',
            schema_file='some_schema_file'
        )    
        
        with mock.patch.multiple(path, **patches):
            etl.load(filepath='game_positions.csv')
        csv2postgres_no_pkeys_mock.assert_called_once_with(
            filepath='game_positions.csv'
        )

    def test_run(self):
        filenames = [
            'here/games.csv',
            'here/players.csv',
            'here/teams.csv',
            'here/game_positions.csv'
        ]
        os_mock = mock.Mock()
        os_mock.path.join.side_effect = filenames

        etl = S3PostgresETL(
            filename='1971-1972/filename',
            storage_dir='here',
            schema_file='schema_file'
        )
        etl.extract_from_s3 = mock.Mock(return_value='/some/dir')
        etl.transform = mock.Mock()
        etl.load = mock.Mock()

        path = 'triple_triple_etl.load.postgres.s3_postgres_etl.os'
        with mock.patch(path, os_mock):
            etl.run()
    
        etl.load.assert_has_calls([mock.call(f) for f in filenames])
        os_mock.remove.assert_has_calls([mock.call(f) for f in filenames])


if __name__ == '__main__':
    unittest.main()
