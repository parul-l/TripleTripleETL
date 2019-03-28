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

from tests.fixtures.mock_nbastats_gamelog_rawdata import (
    mock_gamelog_data,
    mock_df_uploaded
)
from triple_triple_etl.load.storage.nbastats_to_s3parquet import (
    get_first_last_date_season,
    append_upload_metadata,
    NBAStatsGameLogsS3ETL
)
from triple_triple_etl.constants import BASE_URL_GAMELOG, META_DIR, TEST_DIR


class TestHelpers(unittest.TestCase):
    def test_first_last_date_season(self):
        
        get_data_mock = mock.Mock(return_value=mock_gamelog_data)
        
        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet.get_data'
        with mock.patch(path, get_data_mock):
            result_first = get_first_last_date_season(
                season='2015-16',
                datefrom='10/01/2015',
                dateto='10/31/2015',
                first=1
            )
            
            result_last = get_first_last_date_season(
                season='2015-16',
                datefrom='10/01/2015',
                dateto='10/31/2015',
                first=0
            )
        
        assert result_first == '10/27/2015' # from mock data
        assert result_last == '10/31/2015' # from mock data
    

    def test_append_upload_metadata(self):
        data_cols = ['season', 'game_date', 'game_id', 'random']
        upload_cols = [
            'season', 'game_date', 'game_id',
            'gamelog_uploadedFLG', 'lastuploadDTS',
            'base_url', 'params'
        ]
        df_data_mock = pd.DataFrame([list('abcd')], columns=data_cols)
        df_upload_mock = pd.DataFrame([list('efghijk')], columns=upload_cols)
        
        df_result = append_upload_metadata(
            df_data=df_data_mock,
            df_upload=df_upload_mock,
            lastuploadDTS='mock-time',
            base_url='mock-url',
            params='mock-params',
            uploadFLG=1
        )

        self.assertEqual(first=df_result.shape[0], second=2)
        # check a few columns
        self.assertEqual(first=set(df_result.season), second={'a', 'e'})
        self.assertEqual(first=set(df_result.params), second={'k', 'mock-params'})
        self.assertEqual(first=set(df_result.game_id), second={'g', 'c'})


class TestNBAStatsGameLogsS3ETL(unittest.TestCase):
    """Tests for nbastats_to_s3parquet.py"""
    
    def test_metadata(self):    
        # mock functions
        get_upload_metadata_mock = mock.Mock(return_value=mock_df_uploaded)
        
        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet.get_uploaded_metadata'
        with mock.patch(path, get_upload_metadata_mock):   
            etl = NBAStatsGameLogsS3ETL(
                datefrom='01/23/2016',
                dateto='03/23/2016',
                season_year='2015-2016',
                destination_bucket='nba-game-info',
            )
            etl.metadata()
        
        get_upload_metadata_mock.assert_called_once_with(
            etl.uploaded_filepath,
            columns=list(mock_df_uploaded.columns)
        )

    def test_extract(self):
        # mock functions
        get_data_mock = mock.Mock()

        path = 'triple_triple_etl.load.storage.nbastats_to_s3parquet.get_data'
        with mock.patch(path, get_data_mock):
            etl = NBAStatsGameLogsS3ETL(
                datefrom='01/23/2016',
                dateto='03/23/2016',
                season_year='2015-2016',
                destination_bucket='nba-game-info',
            ) 
            etl.extract()
        
        get_data_mock.assert_called_once_with(
            base_url=etl.base_url,
            params=etl.params
        )

    def test_transform(self):

        etl = NBAStatsGameLogsS3ETL(
            datefrom='01/23/2016',
            dateto='03/23/2016',
            season_year='2015-2016',
            destination_bucket='nba-game-info',
        )
        # re-assign tmp_dir for testing purposes
        etl.tmp_dir = os.path.join(TEST_DIR, 'test_tmp')
        etl.transform(mock_gamelog_data)

        # check files/dir as expected
        # check season
        self.assertEqual(
            first=set(os.listdir(etl.tmp_dir)),
            second={'season={}'.format(etl.season_year)}
        )
        # check games
        season_dir = os.path.join(etl.tmp_dir, 'season={}'.format(etl.season_year))
        self.assertEqual(
            first=set(os.listdir(season_dir)),
            second={'game_id=02', 'game_id=03'}
        )
        # remove created dir
        shutil.rmtree(season_dir)

    @moto.mock_s3
    def test_load(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('nba-game-info')
        bucket.create()

        etl = NBAStatsGameLogsS3ETL(
            datefrom='01/23/2016',
            dateto='03/23/2016',
            season_year='2015-2016',
            destination_bucket='nba-game-info',
        )

    # TODO: How to test this function


    def test_cleanup(self):
        etl = NBAStatsGameLogsS3ETL(
            datefrom='01/23/2016',
            dateto='03/23/2016',
            season_year='2015-2016',
            destination_bucket='nba-game-info',
        )
        # update some attributes
        etl.tmp_dir = '/tmp/somedir'
        etl.df_uploaded = copy.deepcopy(mock_df_uploaded)

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

    
        etl = NBAStatsGameLogsS3ETL(
            datefrom='01/23/2016',
            dateto='03/23/2016',
            season_year='2015-2016',
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
        load_mock.assert_called_once_with(transform_mock.return_value)
        cleanup_mock.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()