import copy
import boto3
import moto
import numpy as np
import os
import pandas as pd
import unittest
import unittest.mock as mock

from triple_triple_etl.constants import SQL_DIR
from triple_triple_etl.core.s3 import get_bucket_content
from triple_triple_etl.load.storage.player_actions import (
    get_file_idx_in_uploaded,
    PlayerActionsETL
)

athena = boto3.client('athena', region_name='us-east-1')

mock_df_uploaded = pd.DataFrame(
    data=[
        ['2015-2016', '1234', "{'fake': 'params'}"] + 2 * [np.nan],
        ['2015-2016', '4321', "{'fake': 'params'}"] + 2 * [np.nan]
    ],
    columns=[
        'season',
        'gameid',
        'params',
        'uploadedFLG',
        'lastuploadDTS'
    ]
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


class TestPlayerActionsETL(unittest.TestCase):
    """ Tests for ClosestToBallETL """

    def test_get_query(self):
        # mock functions
        data_read = 'select * from blah'
        open_mock = mock.mock_open(read_data=data_read)

        path = (
            'triple_triple_etl.load.storage.'
            'player_actions.open'
        )

        player_action_path = os.path.join(SQL_DIR, 'sql1.sql')
        with mock.patch(path, open_mock):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.get_query(sql_path='sql1.sql')
            

            open_mock.assert_called_once_with(player_action_path)

    def test_create_tmp_table(self):
        execute_athena_query_mock = mock.Mock(return_value='some_return')
        check_table_exists_mock = mock.Mock()

        patches = {
            'execute_athena_query': execute_athena_query_mock,
            'check_table_exists': check_table_exists_mock
        }
        path = 'triple_triple_etl.load.storage.player_actions'

        with mock.patch.multiple(path, **patches):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.get_query()
            etl.create_tmp_table()

            self.assertEqual(
                first=etl.s3keys['create_player_actions_tmp'],
                second='some_return'
            )
            check_table_exists_mock.assert_called_once_with(
                database_name=etl.database,
                table_name='player_actions_tmp',
                max_time=300
            )

    def test_move_tmp_to_final(self):
        get_bucket_content_mock = mock.Mock(return_value=['contents_list'])
        copy_bucket_contents_mock = mock.Mock()
        s3client_mock = mock.Mock()

        patches = {
            'get_bucket_content': get_bucket_content_mock,
            'copy_bucket_contents': copy_bucket_contents_mock,
            's3client': s3client_mock
        }
        path = 'triple_triple_etl.load.storage.player_actions'

        with mock.patch.multiple(path, **patches):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.move_tmp_to_final()

            get_bucket_content_mock.assert_called_once_with(
                bucket_name=etl.destination_bucket,
                prefix='player_actions_tmp',
                delimiter=''
            )
            copy_bucket_contents_mock.assert_called_once_with(
                copy_source_keys=get_bucket_content_mock.return_value,
                destination_bucket=etl.destination_bucket,
                destination_folder='player_actions',
                s3client=s3client_mock
            )


    def test_alter_tables(self):
        all_files = [
            'somekey/season=2015-2016/gameid=1234/file1',
            'somekey/season=2015-2016/gameid=9876/file2',
        ]

        data_read = 'select * from blah'
        open_mock = mock.mock_open(read_data=data_read)
        execute_athena_query_mock = mock.Mock()
        athena_mock = mock.Mock()

        patches = {
            'open': open_mock,
            'execute_athena_query': execute_athena_query_mock,
            'athena': athena_mock
        }
        path = 'triple_triple_etl.load.storage.player_actions'
            
        with mock.patch.multiple(path, **patches):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.alter_table(all_files)
            self.assertEqual(
                first=execute_athena_query_mock.call_count,
                second=len(all_files)
            )

    def test_drop_tmp_tables(self):
        execute_athena_query_mock = mock.Mock()
        athena_mock = mock.Mock()

        path = 'triple_triple_etl.load.storage.player_actions'

        patches = {
            'execute_athena_query': execute_athena_query_mock,
            'athena': athena_mock
        }
        with mock.patch.multiple(path, **patches):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.drop_tmp_table()
            
            execute_athena_query_mock.assert_called_once_with(
                query='DROP TABLE IF EXISTS nba.player_actions_tmp',
                database=etl.database,
                output_filename='drop_table_player_actions_tmp',
                boto3_client=athena_mock
            )
            
    def test_cleanup(self):
        get_bucket_content_mock = mock.Mock()
        get_bucket_content_mock.side_effect = [
            ['player_actions/somekey/season=2015-2016/gameid=1234/file1',
             'player_actions/somekey/season=2015-2016/gameid=9876/file2'],
        ]
        remove_bucket_contents_mock = mock.Mock(return_value=1)
        get_uploaded_metadata_mock = mock.Mock(
            return_value=copy.deepcopy(mock_df_uploaded))
        update_metadata_mock = mock.Mock()

        patches = {
            'get_bucket_content': get_bucket_content_mock,
            'remove_bucket_contents': remove_bucket_contents_mock,
            'get_uploaded_metadata': get_uploaded_metadata_mock
        }
        path = 'triple_triple_etl.load.storage.player_actions'

        with mock.patch.multiple(path, **patches):
            etl = PlayerActionsETL(
                season='2015-2016',
                gameid_bounds=['0010', '0020'],
            )
            etl.cleanup()

            # test df_uploaded is updated
            self.assertEqual(
                first=etl.df_uploaded.shape[0],
                second=3,
                msg=etl.df_uploaded
            )
            # check remove_bucket_contents called appropriate times
            self.assertEqual(
                first=remove_bucket_contents_mock.call_count,
                second=2
            )

    def test_run(self):
        get_query_mock = mock.Mock()
        create_tmp_table_mock = mock.Mock()
        move_tmp_to_final_mock = mock.Mock(
            return_value=['some_list'])
        alter_table_mock = mock.Mock()
        drop_tmp_table_mock = mock.Mock()
        cleanup_mock = mock.Mock()

        etl = PlayerActionsETL(
            season='2015-2016',
            gameid_bounds=['0010', '0020'],
        )

        etl.get_query = get_query_mock
        etl.create_tmp_table = create_tmp_table_mock
        etl.move_tmp_to_final = move_tmp_to_final_mock
        etl.alter_table = alter_table_mock
        etl.drop_tmp_table = drop_tmp_table_mock
        etl.cleanup = cleanup_mock

        # run the etl and check functions are called
        etl.run()

        # check functions are called
        etl.get_query.assert_called_once_with()
        etl.create_tmp_table.assert_called_once_with()
        etl.move_tmp_to_final.assert_called_once_with()
        etl.alter_table.assert_called_once_with(
            move_tmp_to_final_mock.return_value
        )
        etl.drop_tmp_table.assert_called_once_with()
        etl.cleanup.assert_called_once_with()



if __name__ == '__main__':
    unittest.main()
