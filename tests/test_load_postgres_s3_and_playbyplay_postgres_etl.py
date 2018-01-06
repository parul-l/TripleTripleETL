import unittest

import mock

from triple_triple_etl.load.postgres.s3_and_playbyplay_postgres_etl import
    (
        S3andPlaybyPlayPostgresETL
    )

class TestS3andPlaybyPlayPostgresETL(unittest.TestCase):
    """ Test for s3_and_playbyplay_postgres_etl.py"""

    def test_run(self):
        s3_etl_mock = mock.Mock()
        s3_etl_mock.game_id = '1234'
        s3_etl_mock.season = '2480-81'
        s3_etl_mock.run = mock.Mock()

        params = {'game_id': None, 'season': None}
        playbyplay_etl_mock = mock.Mock()
        playbyplay_etl_mock.params = params
        playbyplay_etl_mock.run = mock.Mock()

        etl = S3andPlaybyPlayPostgresETL(
            s3_etl=s3_etl_mock,
            playbyplay_etl=playbyplay_etl_mock
        )
        etl.run()
        
        s3_etl_mock.run.assert_called_once_with()
        playbyplay_etl_mock.run.assert_called_once_with()
        assert playbyplay_etl_mock.params['game_id'] == '1234'
        assert playbyplay_etl_mock.params['season'] == '2480-81'


if __name__ == '__main__':
    unittest.main()
