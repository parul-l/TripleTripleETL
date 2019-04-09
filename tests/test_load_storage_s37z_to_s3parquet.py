import copy
import io
import boto3
import moto
import os
import tempfile
import unittest
import unittest.mock as mock

from tests.fixtures.mock_s37z_to_s3parquet_data import mock_df_uploaded
from tests.fixtures.mock_s3rawdata import data
from triple_triple_etl.load.storage.s37z_to_s3parquet import (
    get_file_idx_in_uploaded,
    S3FileFormatETL,
    transform_upload_all_games
)
from triple_triple_etl.constants import META_DIR


class TestFileIdx(unittest.TestCase):
    def test_file_exists(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            input_filename='somefile.7z'
        )
        assert idx == 1

    def test_file_dne(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            input_filename='someotherfile.7z'
        )
        assert idx == mock_df_uploaded.shape[0]


class TestS3FileFormatETL(unittest.TestCase):
    """Tests for s37z_to_s3parquet.py"""
    
    def test_metadata(self):    
        inputfile_mock = mock.Mock(return_value='somefile.7z')
        source_bucket_mock = mock.Mock(return_value='nba-player-positions')
        destination_bucket_mock = mock.Mock(return_value='nba-game-info')
        season = '2015-2016'

        
        etl = S3FileFormatETL(
            input_filename=inputfile_mock,
            source_bucket=source_bucket_mock,
            destination_bucket=destination_bucket_mock,
            season=season
        )
        get_uploaded_metadata_mock = mock.Mock(return_value=mock_df_uploaded)
        get_file_idx_uploaded_mock = mock.Mock()
        
        patches = {
            'get_uploaded_metadata': get_uploaded_metadata_mock,
            'get_file_idx_in_uploaded': get_file_idx_uploaded_mock
        }
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'
        with mock.patch.multiple(path, **patches):   
            etl.metadata()
        
        get_uploaded_metadata_mock.assert_called_once_with(
            etl.uploaded_filepath,
            columns=list(mock_df_uploaded.columns)
        )
        get_file_idx_uploaded_mock.assert_called_once_with(
            df_uploaded=mock_df_uploaded,
            input_filename=inputfile_mock
        )

    def test_meta_file_exists(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            input_filename='somefile.7z'
        )
        assert idx == 1

    def test_meta_file_dne(self):
        idx = get_file_idx_in_uploaded(
            df_uploaded=mock_df_uploaded,
            input_filename='someotherfile.7z'
        )
        assert idx == mock_df_uploaded.shape[0]

    def test_extract_from_s3(self):
        # etl input mocks
        inputfile_mock = mock.Mock(return_value='somefile.7z')
        source_bucket_mock = mock.Mock(return_value='nba-player-positions')
        destination_bucket_mock = mock.Mock(return_value='nba-game-info')
        season = '2015-2016'

        # function mocks
        s3download_mock = mock.Mock(return_value='some.txt')
        extract2dir_mock = mock.Mock()
        tempfile_mock = mock.Mock()
        tempfile_mock.mkdtemp.return_value = 'tmp/somedir'

        # set up mock data
        open_mock = mock.mock_open()
        os_mock = mock.Mock()
        os_mock.listdir.return_value = ['mock_s3rawdata.py']
        os_mock.path.join = os.path.join
        json_mock = mock.Mock()
        json_mock.load.return_value = data

        # instantiate etl
        etl = S3FileFormatETL(
            input_filename=inputfile_mock,
            source_bucket=source_bucket_mock,
            destination_bucket=destination_bucket_mock,
            season=season
        )
        # test given open mock
        patches = {
            'extract2dir': extract2dir_mock,
            's3download': s3download_mock,
            'tempfile': tempfile_mock,
            'open': open_mock,
            'os': os_mock,
            'json': json_mock
        }
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'

        with mock.patch.multiple(path, **patches):
            extract_output = etl.extract_from_s3()
        
        # assert statements
        s3download_mock.assert_called_once_with(
            bucket_name=source_bucket_mock,
            filename=inputfile_mock
        )
        extract2dir_mock.assert_called_once_with(
            filepath=s3download_mock.return_value,
            directory=tempfile_mock.mkdtemp.return_value
        )

        self.assertEqual(
            first=etl.gameid,
            second=data['gameid'],
            msg='{}, {}'.format(etl.gameid, data['gameid'])
        )
        self.assertEqual(
            first=set(extract_output.keys()),
            second=set(data.keys())
        )

        # test by removing 'open_mock'
        patches.pop('open')
        with mock.patch.multiple(path, **patches):
            extract_output = etl.extract_from_s3()
        self.assertEqual(
            first=extract_output,
            second={}
        )
    def test_transform(self):
        # etl input mocks
        inputfile_mock = 'somefile.7z'
        source_bucket_mock = 'nba-player-positions'
        destination_bucket_mock = 'nba-game-info'
        season = '2015-2016'

        # function input mocks
        tablename = 'sometable'
        transform_function_mock = mock.Mock()

        # built in function mocks
        pq_mock = mock.Mock()
        pq_mock.write_to_dataset = mock.Mock()
        os_mock = mock.Mock()
        os_mock.listdir = mock.Mock(return_value=['somefilename.parquet'])

        # set up mocks
        tempfile_mock = mock.Mock()
        tempfile_mock.mkdtemp.return_value = 'tmp/somedir'

        # instantiate etl and update etl.tmp_dir
        etl = S3FileFormatETL(
            input_filename=inputfile_mock,
            source_bucket=source_bucket_mock,
            destination_bucket=destination_bucket_mock,
            season=season
        )
        etl.tmp_dir = tempfile_mock.mkdtemp.return_value
        etl.gameid = data['gameid']

        # collect functions to patch
        patches = {
            'pq': pq_mock,
            'os': os_mock 
        }
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'
        with mock.patch.multiple(path, **patches):
            etl._transform(
                data=data,
                tablename=tablename,
                transform_function=transform_function_mock
            )
        self.assertEqual(
            first=set(etl.data_paths.keys()),
            second={tablename}
        )

        # check transform_function called are called
        transform_function_mock.assert_called_once_with(data)


    @moto.mock_s3
    def test_load(self):
        # etl input
        destination_bucket = 'nba-game-info-test'
        season = '2015-2016'
        gameid = '1234'

        # _load input
        tablename = 'sometable'

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(destination_bucket)
        bucket.create()

        with tempfile.NamedTemporaryFile() as tmp_file:
            filename = tmp_file.name
            etl = S3FileFormatETL(
                input_filename=filename,
                source_bucket='nba-player-positions-test',
                destination_bucket=destination_bucket,
                season=season
            )        
            # update some attributes
            etl.gameid = gameid
            etl.df_uploaded = copy.deepcopy(mock_df_uploaded)
            etl.file_idx = 1
            etl.data_paths = {tablename: filename}
            
            etl._load(tablename=tablename)

        key = '{}/season={}/gameid={}/{}.parquet.snappy'.format(
            tablename,
            season,
            gameid,
            os.path.basename(filename)
        )
        objects = list(bucket.objects.filter(Prefix=key))
        assert len(objects) == 1

    def test_transform_tables(self):
        # instantiate etl 
        etl = S3FileFormatETL(
            input_filename='somefile.7z',
            source_bucket='nba-player-positions',
            destination_bucket='nba-game-info',
            season='2015-2016'
        )
        # mock functions
        get_game_info_mock = mock.Mock()
        get_player_info_mock = mock.Mock()
        get_team_info_mock = mock.Mock()
        get_game_position_info_mock = mock.Mock()
        transform_mock = mock.Mock()
        

        patches = {
            'get_game_info': get_game_info_mock,
            'get_player_info': get_player_info_mock,
            'get_team_info': get_team_info_mock,
            'get_game_position_info': get_game_position_info_mock,
        }
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'
        with mock.patch.multiple(path, **patches):
            etl._transform = transform_mock   
            etl.transform_game(data)
            etl.transform_player(data)
            etl.transform_team(data)
            etl.transform_position(data)
        
        expected_calls = [
            mock.call(
                data=data, 
                tablename='gameinfo', 
                transform_function=get_game_info_mock
            ),
            mock.call(
                data=data, 
                tablename='playerinfo', 
                transform_function=get_player_info_mock
            ),
            mock.call(
                data=data, 
                tablename='teaminfo', 
                transform_function=get_team_info_mock
            ),
            mock.call(
                data=data, 
                tablename='gameposition', 
                transform_function=get_game_position_info_mock
            )
        ]

    def test_load_tables(self):
        # instantiate etl 
        etl = S3FileFormatETL(
            input_filename='somefile.7z',
            source_bucket='nba-player-positions',
            destination_bucket='nba-game-info',
            season='2015-2016'
        )
        load_mock = mock.Mock()
        etl._load = load_mock

        etl.load_game()        
        etl.load_player()
        etl.load_team()    
        etl.load_position()
    
        expected_calls = [
            mock.call(tablename='gameinfo'),
            mock.call(tablename='playerinfo'),
            mock.call(tablename='teaminfo'),
            mock.call(tablename='gameposition')
        ]
        load_mock.assert_has_calls(expected_calls, any_order=False)

    def test_cleanup(self):
        etl = S3FileFormatETL(
            input_filename='somefile.7z',
            source_bucket='nba-player-positions-test',
            destination_bucket='nba-game-info-test',
            season='2015-2016'
        )
        # update some attributes
        etl.tmp_dir = '/tmp/somedir'
        etl.df_uploaded = copy.deepcopy(mock_df_uploaded)
        etl.file_idx = 1

        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet.shutil'
        with mock.patch(path, mock.Mock()) as s:
            etl.cleanup()
            s.rmtree.assert_called_once_with(etl.tmp_dir)

    def test_run(self):
        # etl mock functions
        metadata_mock = mock.Mock()
        extract_mock = mock.Mock(return_value={'some': 'data'})   
        transform_game_mock = mock.Mock()
        transform_player_mock = mock.Mock()
        transform_team_mock = mock.Mock()
        transform_position_mock = mock.Mock()
        load_game_mock = mock.Mock()
        load_player_mock = mock.Mock()
        load_team_mock = mock.Mock()
        load_position_mock = mock.Mock()
        cleanup_mock = mock.Mock()
        # transform mock functions
        get_game_info_mock = mock.Mock()
        get_player_info_mock = mock.Mock()
        get_team_info_mock = mock.Mock()
        get_game_position_info_mock = mock.Mock()
    
        patches = {
            'get_game_info': get_game_info_mock,
            'get_player_info': get_player_info_mock,
            'get_team_info': get_team_info_mock,
            'get_game_position_info': get_game_position_info_mock,
        }
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet'
        with mock.patch.multiple(path, **patches):
            etl = S3FileFormatETL(
                input_filename='somefile.7z',
                source_bucket='nba-player-positions-test',
                destination_bucket='nba-game-info-test',
                season='2015-2016'
            )
            etl.metadata = metadata_mock
            etl.extract_from_s3 = extract_mock
            
            etl.transform_game = transform_game_mock
            etl.transform_player = transform_player_mock
            etl.transform_team = transform_team_mock
            etl.transform_position = transform_position_mock
            
            etl.load_game = load_game_mock
            etl.load_player = load_player_mock
            etl.load_team = load_team_mock
            etl.load_position = load_position_mock
            
            etl.cleanup = cleanup_mock

            # run the etl and check functions are called
            etl.run()

        # check functions are called
        metadata_mock.assert_called_once_with()
        extract_mock.assert_called_once_with()
        transform_game_mock.assert_called_once_with(extract_mock.return_value)
        transform_player_mock.assert_called_once_with(extract_mock.return_value)
        transform_team_mock.assert_called_once_with(extract_mock.return_value)
        transform_position_mock.assert_called_once_with(extract_mock.return_value)
        load_game_mock.assert_called_once_with()
        load_player_mock.assert_called_once_with()
        load_team_mock.assert_called_once_with()
        load_position_mock.assert_called_once_with()
        cleanup_mock.assert_called_once_with()


class TestTransformUploadAll(unittest.TestCase):
    """test transform_upload_all_games.py """

    def test_tranform_upload_all_games(self):
        # input parameters
        season = '2015-2016'
        all_files = ['file1.7z', 'file2.7z', 'file3.7z']
        idx = [1, 2]
        source_bucket = 'nba-position-data-test'
        destination_bucket = 'nba-info-test'

        # function mocks
        etl_mock = mock.Mock()
        S3FileFormatETL_mock = mock.Mock(return_value=etl_mock)
        etl_mock.run = mock.Mock()
        
        path = 'triple_triple_etl.load.storage.s37z_to_s3parquet.S3FileFormatETL'
        with mock.patch(path, S3FileFormatETL_mock):
            transform_upload_all_games(
                season=season,
                all_files=all_files,
                idx=idx,
                source_bucket=source_bucket,
                destination_bucket=destination_bucket
            )
        assert S3FileFormatETL_mock.call_count == len(idx)
        assert etl_mock.run.call_count == len(idx)



if __name__ == '__main__':
    unittest.main()