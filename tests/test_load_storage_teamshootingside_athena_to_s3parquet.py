# TODO: Need to test TeamShootingSideETL

import copy
import boto3
import moto
import os
import pandas as pd
import unittest
import unittest.mock as mock

from tests.fixtures.mock_teamshootingside_athena_to_s3parquet import (
    mock_df_uploaded
)
from triple_triple_etl.core.s3 import get_bucket_content
from triple_triple_etl.load.storage.teamshooting_athena_to_s3parquet import (
    get_file_idx_in_uploaded,
    TeamShootingSideALL,
    TeamShootingSideETL
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

 
class TestTeamShootingSideALL(unittest.TestCase):
    """ Tests for TeamShootingSideALL """

    def test_metadata(self):
        # mock functions
        get_upload_metadata_mock = mock.Mock(return_value=mock_df_uploaded)
        get_file_idx_in_uploaded_mock = mock.Mock()
        

        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet.get_uploaded_metadata'
        )
        with mock.patch(path, get_upload_metadata_mock):   
            etl = TeamShootingSideALL()
            etl.df_uploaded = mock_df_uploaded
            etl.metadata()
        
        get_upload_metadata_mock.assert_called_once_with(
            etl.uploaded_filepath,
            columns=list(mock_df_uploaded.columns)
        )

    def test_execute_query(self):
        athena_mock = mock.Mock()

        os_mock = mock.Mock()
        os_mock.path.join.return_value = 'some/path/to/sql/query.txt'
        
        open_mock =mock.mock_open(read_data='SELECT * FROM sometable')

        execute_athena_query_mock = mock.Mock()
        execute_athena_query_mock.return_value = {'QueryExecutionId': '1234'}

        patches = {
            'athena': athena_mock,
            'os': os_mock,
            'open': open_mock,
            'execute_athena_query': execute_athena_query_mock         
        }
        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet'
        )
        with mock.patch.multiple(path, **patches):
            etl = TeamShootingSideALL()
            etl.execute_query(output_filename='test')

        execute_athena_query_mock.assert_called_once_with(
            query=etl.query,
            database=etl.database,
            boto3_client=athena_mock,
            output_filename='test'
        )

    def test_extract(self):
        # mock functions
        s3_mock = mock.Mock()
        s3_mock.Bucket = mock.Mock()
        s3_mock.Bucket.download_file = mock.Mock()
        
        tmp_mock = mock.Mock()
        tmp_mock.mkdtemp.return_value = '/fake/output/dir'

        patches = {
            's3': s3_mock,
            'tempfile': tmp_mock         
        }

        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet'
        )
        with mock.patch.multiple(path, **patches):
            etl = TeamShootingSideALL()
            output_filepath = etl.extract(s3key='fake_s3_key')
        
        self.assertEqual(
            first=output_filepath,
            second='/fake/output/dir/team_shooting_side.csv'
        )
        s3_mock.Bucket.assert_called_once_with(etl.source_bucket)

    def test_transform(self):
        # mock functions
        df_mock = pd.DataFrame([[1, 2], [3, 4]], columns=['season', 'gameid'])
        pd_mock = mock.Mock()
        pd_mock.read_csv.return_value = df_mock
        
        pq_mock = mock.Mock()
        pq_mock.write_to_dataset = mock.Mock()

        pa_mock = mock.Mock()
        pa_mock.Table.from_pandas.return_value = 'some_table'

        patches = {
            'pa': pa_mock,
            'pd': pd_mock,
            'pq': pq_mock
        }        
        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet'
        )
        with mock.patch.multiple(path, **patches):
            etl = TeamShootingSideALL()
            etl.transform(output_filepath='some/path')

        # check functions are called        
        pq_mock.write_to_dataset.assert_called_once_with(
            table='some_table',
            root_path=etl.tmp_dir,
            partition_cols=['season', 'gameid'],
            compression='snappy',
            preserve_index=False
        )

    @moto.mock_s3
    def test_load(self):
        fake_bucket = 'fake-bucket'
        s3resource = boto3.resource('s3', region_name='us-east-1')
        bucket = s3resource.Bucket(fake_bucket)
        bucket.create()
        
        columns = ['season', 'gameid', 'period', 'game_date', 'team_abbreviation']
        data = [
            ['2015-2016', '1234', 1, '2016-04-23', 'FAN'],
            ['2015-2016', '1234', 1, '2016-04-23', 'FUN']
        ]
        df_mock = pd.DataFrame(data=data, columns=columns)
        
        os_mock = mock.Mock()
        os_mock.path.join.return_value = 'some/season/filepath/'
        os_mock.listdir.side_effect = [
            ['season=2015-2016'],
            ['gameid=1234'], 
            ['test_filename']
        ]
        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet.os'
        )

        with mock.patch(path, os_mock):
            etl = TeamShootingSideALL()
            etl.destination_bucket = fake_bucket
            etl.metadata()
            etl.df_uploaded = copy.deepcopy(mock_df_uploaded)
            etl.load(df_mock)

        # ans = get_bucket_content(
        #     bucket_name=fake_bucket,
        #     prefix='team_shooting_side/',
        #     delimiter='/'
        # )

        # TODO: How to test this function

    def test_cleanup(self):

        etl = TeamShootingSideALL()
        # update some attributes
        etl.tmp_dir = '/tmp/somedir'
        etl.df_uploaded = copy.deepcopy(mock_df_uploaded)
        etl.file_idx = 1
        # mock functions
        etl.df_uploaded.to_parquet = mock.Mock()

        path = (
            'triple_triple_etl.load.storage.'
            'teamshooting_athena_to_s3parquet.shutil'
        )
        with mock.patch(path, mock.Mock()) as s:
            etl.cleanup()
            s.rmtree.assert_called_once_with(etl.tmp_dir)


    def test_run(self):
        # etl mock functions
        metadata_mock = mock.Mock()
        execute_query_mock = mock.Mock(return_value='mock_s3key')
        extract_mock = mock.Mock(return_value='mock/filepath')   
        transform_mock = mock.Mock(return_value='mock_df')
        load_mock = mock.Mock()
        cleanup_mock = mock.Mock()

        etl = TeamShootingSideALL()
        etl.metadata = metadata_mock
        etl.execute_query = execute_query_mock
        etl.extract = extract_mock
        etl.transform = transform_mock
        etl.load = load_mock
        etl.cleanup = cleanup_mock

        # run the etl and check functions are called
        etl.run()

        # check functions are called
        metadata_mock.assert_called_once_with()
        execute_query_mock.assert_called_once_with()
        extract_mock.assert_called_once_with(execute_query_mock.return_value)
        transform_mock.assert_called_once_with(extract_mock.return_value)
        load_mock.assert_called_once_with(transform_mock.return_value)
        cleanup_mock.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()