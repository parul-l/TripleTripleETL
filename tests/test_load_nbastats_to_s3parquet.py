import copy
import io
import boto3
import moto
import os
import pandas as pd
import shutil
import tempfile
import unittest
import unittest.mock as mock

from tests.fixtures.mock_nbastats_data import (
    # mock_gamelog_data,
    mock_df_uploaded
)
from triple_triple_etl.load.storage.nbastats_to_s3parquet import (
    get_file_idx_in_uploaded,
    NBAStatsS3ETL,
    upload_all_nbastats
)
from triple_triple_etl.constants import (
    BASE_URL_PLAY,
    BASE_URL_BOX_SCORE_TRADITIONAL,
    BASE_URL_BOX_SCORE_PLAYER_TRACKING,
    DESTINATION_BUCKET,
    META_DIR,
    NBASTATS_PARAMS
)

class TestHelper(unittest.TestCase):
    """Test get_file_idx_in_uploaded"""
    def test_file_exists(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            gameid='1234'
        )
        assert idx == 0

    def test_file_dne(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            gameid='9999'
        )
        assert idx == mock_df_uploaded.shape[0]

 
class TestNBAStatsS3ETL(unittest.TestCase):
    """ Tests for NBAStatsS3ETL """

    def test_metadata(self):
        # mock functions
        get_upload_metadata_mock = mock.Mock(return_value=mock_df_uploaded)
        get_file_idx_in_uploaded_mock = mock.Mock()
        
        patches = {
            'get_uploaded_metadata': get_upload_metadata_mock,
            'get_file_idx_in_uploaded': get_file_idx_in_uploaded_mock
        }
        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet'
        with mock.patch.multiple(path, **patches):   
            etl = NBAStatsS3ETL(
                gameid='someid',
                gamedate='04-01-2019',
                team1='mock-team1',
                team2='mock-team2',
                game_data_type='playbyplay',
                season='2015-2016',
                destination_bucket='nba-game-info',
            )
            etl.df_uploaded = mock_df_uploaded
            etl.metadata()
        
        get_upload_metadata_mock.assert_called_once_with(
            etl.uploaded_filepath,
            columns=list(mock_df_uploaded.columns)
        )
        get_file_idx_in_uploaded_mock.assert_called_once_with(
            df_uploaded=mock_df_uploaded,
            gameid=etl.gameid
        )


    def test_extract(self):
        # mock functions
        get_data_mock = mock.Mock()

        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet.get_data'
        with mock.patch(path, get_data_mock):
            etl = NBAStatsS3ETL(
                gameid='someid',
                gamedate='04-01-2019',
                team1='mock-team1',
                team2='mock-team2',
                game_data_type='playbyplay',
                season='2015-2016',
                destination_bucket='nba-game-info',
            )
            etl.extract()
        
        get_data_mock.assert_called_once_with(
            base_url=etl.base_url,
            params=etl.params
        )

    def test_transform_playbyplay(self):
        # mock functions
        data = {'some': 'data'}
        get_play_by_play_mock = mock.Mock(return_value='table_playbyplay')
        pq_mock = mock.Mock()
        pq_mock.write_to_dataset = mock.Mock()
        
        patches = {
            'get_play_by_play': get_play_by_play_mock,
            'pq': pq_mock
        }        
        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet'        
        with mock.patch.multiple(path, **patches):
            etl = NBAStatsS3ETL(
                gameid='someid',
                gamedate='04-01-2019',
                team1='mock-team1',
                team2='mock-team2',
                game_data_type='playbyplay',
                season='2015-2016',
                destination_bucket='nba-game-info',
            )
            etl.transform(data=data)

        # check functions are called        
        get_play_by_play_mock.assert_called_once_with(data, season='2015-2016')
        pq_mock.write_to_dataset.assert_called_once_with(
            table=get_play_by_play_mock.return_value,
            root_path=etl.tmp_dir,
            partition_cols=['season', 'game_id'],
            compression='snappy',
            preserve_index=False
        )

    def test_transform_boxscore(self):
        # mock functions
        data = {'some': 'data'}
        get_boxscore_mock = mock.Mock(return_value='table_boxscore')
        pq_mock = mock.Mock()
        pq_mock.write_to_dataset = mock.Mock()        

        patches = {
            'get_boxscore': get_boxscore_mock,
            'pq': pq_mock
        }

        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet'
        for game_type in ['boxscore_traditional', 'boxscore_player']:
            with mock.patch.multiple(path, **patches):
                etl = NBAStatsS3ETL(
                    gameid='someid',
                    gamedate='04-01-2019',
                    team1='mock-team1',
                    team2='mock-team2',
                    game_data_type=game_type,
                    season='2015-2016',
                    destination_bucket='nba-game-info',
                )
                etl.transform(data=data)
                
        expected_calls = [
            mock.call(data, season='2015-2016', traditional_player='traditional'),
            mock.call(data, season='2015-2016', traditional_player='player')    
        ]
        get_boxscore_mock.assert_has_calls(expected_calls, any_order=False)        
        self.assertEqual(pq_mock.write_to_dataset.call_count, 2)

    @moto.mock_s3
    def test_load(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('nba-game-info')
        bucket.create()

        etl = NBAStatsS3ETL(
            gameid='someid',
            gamedate='04-01-2019',
            team1='mock-team1',
            team2='mock-team2',
            game_data_type='playbyplay',
            season='2015-2016',
            destination_bucket='nba-game-info',
        )
        # TODO: How to test this function

    def test_cleanup(self):

        etl = NBAStatsS3ETL(
            gameid='someid',
            gamedate='04-01-2019',
            team1='mock-team1',
            team2='mock-team2',
            game_data_type='playbyplay',
            season='2015-2016',
            destination_bucket='nba-game-info',
        )
        # update some attributes
        etl.tmp_dir = '/tmp/somedir'
        etl.df_uploaded = copy.deepcopy(mock_df_uploaded)
        etl.file_idx = 1
        # mock functions
        etl.df_uploaded.to_parquet = mock.Mock()

        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet.shutil'
        with mock.patch(path, mock.Mock()) as s:
            etl.cleanup()
            s.rmtree.assert_called_once_with(etl.tmp_dir)


    def test_run(self):
        # etl mock functions
        metadata_mock = mock.Mock()
        extract_mock = mock.Mock(return_value={'some': 'data'})   
        transform_mock = mock.Mock(return_value='mock_df')
        load_mock = mock.Mock()
        cleanup_mock = mock.Mock()

        etl = NBAStatsS3ETL(
            gameid='someid',
            gamedate='04-01-2019',
            team1='mock-team1',
            team2='mock-team2',
            game_data_type='playbyplay',
            season='2015-2016',
            destination_bucket='nba-game-info',
        )
        etl.metadata = metadata_mock
        etl.extract = extract_mock
        etl.transform = transform_mock
        etl.load = load_mock
        etl.cleanup = cleanup_mock

        # run the etl and check functions are called
        etl.run()

        # check functions are called
        metadata_mock.assert_called_once_with()
        extract_mock.assert_called_once_with()
        transform_mock.assert_called_once_with(extract_mock.return_value)
        load_mock.assert_called_once_with()
        cleanup_mock.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()