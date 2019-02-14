import copy
import mock
import unittest

from tests.fixtures.mock_s37z_to_s3parquet_data import mock_df_uploaded
from triple_triple_etl.load.storage.s37z_to_s3parquet import (
    S3FileFormatETL,
    transform_upload_all_games
)

class TestS3FileFormatETL(unittest.TestCase):
    """Tests for s37z_to_s3parquet.py"""

    
    def test_metadata_file_exist(self):    
        inputfile_mock = mock.Mock(return_value='somefile.7z')
        source_bucket_mock = mock.Mock(return_value='nba-player-positions')
        destination_bucket_mock = mock.Mock(return_value='nba-game-info')
        season_year = '2015-2016'

        read_parquet_mock = mock.Mock()
        read_parquet_mock.read_parquet.return_value = mock_df_uploaded
        patches = {'pd': read_parquet_mock}
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'
        
        
        etl = S3FileFormatETL(
            input_filename=inputfile_mock,
            source_bucket=source_bucket_mock,
            destination_bucket=destination_bucket_mock,
            season_year=season_year
        )
        with mock.patch.multiple(path, **patches):   
        # call metadata function
            etl.metadata()
        self.assertEqual(etl.df_uploaded.shape[0], 3, msg='{}'.format(etl.df_uploaded))

        # test columns are as expected
        # self.assertEqual(
        #     first=set(etl.df_uploaded.columns),
        #     second={
        #         'input_filename',
        #         'gameinfo_uploadedFLG',
        #         'gameposition_uploadedFLG',
        #         'playersinfo_uploadedFLG',
        #         'teamsinfo_uploadedFLG',
        #         'lastuploadDTS'
        #     }
        # )
        # test if input file does not exist
        # assert etl.file_idx == etl.df_uploaded.shape[0]
        
        # # test if input file does exist
        
        # self.assertEqual(
        #     first=etl.file_idx,
        #     second=1,
        #     msg='{}'.format(etl.df_uploaded)
        # )


if __name__ == '__main__':
    unittest.main()