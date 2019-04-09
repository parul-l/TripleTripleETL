import unittest
import mock

from tests.helper_methods import create_mock_context_manager
from triple_triple_etl.constants import config

from triple_triple_etl.load.postgres.nbastats_postgres_etl import NBAStatsPostgresETL


class TestNBAStatsPostgresETL(unittest.TestCase):
    """Tests for nbastats_postgres_etl.py"""

    def test_extract_from_nbastats(self):
        nba_data_mock = mock.Mock()
        get_data_mock = mock.Mock(return_value=nba_data_mock)

        etl = NBAStatsPostgresETL(
            base_url='some_base',
            params='some_params',
            data_content='0/1/2', 
            schema_file='some_schema_file'
        )

        path = 'triple_triple_etl.load.postgres.nbastats_postgres_etl.get_data'
        with mock.patch(path, get_data_mock):
            etl.extract_from_nbastats()
        
        get_data_mock.assert_called_once_with(
            base_url='some_base',
            params='some_params'
        )

    def test_transform_play_by_play(self):
        get_df_play_by_play_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.get_df_play_by_play'
        )

        for data_content in [0, 1, 2]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content,
                schema_file='some_schema_file'
            )
            etl.data = {'some': 'data'}

            if data_content == 0:
                with mock.patch(path, get_df_play_by_play_mock):
                    etl.transform_play_by_play()
                get_df_play_by_play_mock.assert_called_once_with(etl.data)
            elif data_content in [1, 2]:
                with self.assertRaises(ValueError):
                    etl.transform_play_by_play()
             
    def test_transform_box_score_traditional(self):
        get_df_box_score_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.get_df_box_score'
        )

        for data_content in [0, 1, 2]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content,
                schema_file='some_schema_file'
            )
            etl.data = {'some': 'data'}

            if data_content == 1:
                with mock.patch(path, get_df_box_score_mock):
                    etl.transform_box_score_traditional()
            
                get_df_box_score_mock.assert_called_once_with(etl.data, 0)
            elif data_content in [0, 2]:
                with self.assertRaises(ValueError):
                    etl.transform_box_score_traditional()

    def test_transform_box_score_player_tracking(self):
        get_df_box_score_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.get_df_box_score'
        )

        for data_content in [0, 1, 2]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content,
                schema_file='some_schema_file'
            )
            etl.data = {'some': 'data'}

            if data_content == 2:
                with mock.patch(path, get_df_box_score_mock):
                    etl.transform_box_score_player_tracking()

                get_df_box_score_mock.assert_called_once_with(etl.data, 1)
            elif data_content in [0, 1]:
                with self.assertRaises(ValueError):
                    etl.transform_box_score_player_tracking()
    
    def test_transform(self):
        tables_dict_mock = mock.Mock()
        save_all_tables_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.save_all_tables'
        )

        for data_content in [0, 1, 2]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content,
                schema_file='some_schema_file'
            )
            etl.transform_play_by_play = mock.Mock(
                return_value=tables_dict_mock
            )
            etl.transform_box_score_traditional = mock.Mock(
                return_value=tables_dict_mock
            )
            etl.transform_box_score_player_tracking = mock.Mock(
                return_value=tables_dict_mock
            )
            etl.storage_dir = 'here'

            with mock.patch(path, save_all_tables_mock):
                etl.transform()

            if data_content == 0:
                etl.transform_play_by_play.assert_called_once_with()
                save_all_tables_mock.assert_called_once_with(
                    tables_dict=tables_dict_mock,
                    storage_dir='here'
                )

            elif data_content == 1:
                etl.transform_box_score_traditional.assert_called_once_with()
                assert save_all_tables_mock.call_count == 2
            
            elif data_content == 2:
                etl.transform_box_score_player_tracking.assert_called_once_with()
                assert save_all_tables_mock.call_count == 3
              

    def test_load(self):
        csv2postgres_pkeys_mock = mock.Mock()
        etl = NBAStatsPostgresETL(
            base_url='some_base',
            params='some_params',
            data_content='0/1/2',
            schema_file='some_schema_file'
        )
        path = (
                'triple_triple_etl.load.postgres.nbastats_postgres_etl'
                '.csv2postgres_pkeys'
        )
        with mock.patch(path, csv2postgres_pkeys_mock):
            etl.load(filepath='some.csv')

        csv2postgres_pkeys_mock.assert_called_once_with(
            tablename='some',
            filepath='some.csv',
            schema_file='some_schema_file'
        )

    def test_run(self): 
        filenames = [
            'here/play_by_play.csv',
            'here/box_score_traditional.csv',
            'here/box_score_player_tracking.csv'
        ]
        os_mock = mock.Mock()
        os_mock.path.join.side_effect = filenames

        for data_content in [0, 1, 2]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content,
                schema_file='some_schema_file'
            )
            etl.storage_dir = 'here'
            etl.extract_from_nbastats = mock.Mock(return_value='/some/dir')
            etl.transform = mock.Mock()
            etl.load = mock.Mock()
            
            path = 'triple_triple_etl.load.postgres.nbastats_postgres_etl.os'
            with mock.patch(path, os_mock):
                etl.run()

            etl.load.assert_called_once_with(filenames[data_content])

        os_mock.remove.assert_has_calls(
            [mock.call(f) for f in filenames]
        )


if __name__ == '__main__':
    unittest.main()

