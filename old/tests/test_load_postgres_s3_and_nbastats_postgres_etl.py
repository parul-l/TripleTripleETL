import unittest
import mock

from triple_triple_etl.load.postgres.s3_and_nbastats_postgres_etl import
    (
        S3andNBAStatsPostgresETL
    )

class TestS3andNBAStatsPostgresETL(unittest.TestCase):
    """ Test for s3_and_nbastats_postgres_etl.py"""

    def test_run(self):
        s3_etl_mock = mock.Mock()
        s3_etl_mock.game_id = '1234'
        s3_etl_mock.season = '2480-81'
        s3_etl_mock.run = mock.Mock()

        params = {'game_id': None, 'season': None}
        playbyplay_etl_mock = mock.Mock()
        playbyplay_etl_mock.params = params
        playbyplay_etl_mock.run = mock.Mock()

        boxscore_traditional_etl_mock = mock.Mock()
        boxscore_traditional_etl_mock.params = params
        boxscore_traditional_etl_mock.run = mock.Mock()

        boxscore_player_tracking_etl_mock = mock.Mock()
        boxscore_player_tracking_etl_mock.params = params
        boxscore_player_tracking_etl_mock.run = mock.Mock()


        etl = S3andNBAStatsPostgresETL(
            s3_etl=s3_etl_mock,
            playbyplay_etl=playbyplay_etl_mock,
            boxscore_traditional_etl=boxscore_traditional_etl_mock,
            boxscore_player_tracking_etl=boxscore_player_tracking_etl_mock
        )
        etl.run()
        
        s3_etl_mock.run.assert_called_once_with()
        
        nbastats_etls = [
            playbyplay_etl_mock,
            boxscore_traditional_etl_mock,
            boxscore_player_tracking_etl_mock
        ]
        for nba_etl in nbastats_etls:

            nba_etl.run.assert_called_once_with()
            assert nba_etl.params['game_id'] == '1234'
            assert nba_etl.params['season'] == '2480-81'


if __name__ == '__main__':
    unittest.main()
