import os
import mock

import unittest

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
            data_content='1/0'
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

        for data_content in [0, 1]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content
            )
            etl.data = {'some': 'data'}

            if data_content == 0:
                with self.assertRaises(ValueError):
                    etl.transform_play_by_play()
            if data_content == 1:
                with mock.patch(path, get_df_play_by_play_mock):
                    etl.transform_play_by_play()
             
                get_df_play_by_play_mock.assert_called_once_with(etl.data)

    def test_transform_box_score(self):
        get_df_box_score_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.get_df_box_score'
        )

        for data_content in [0, 1]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content
            )
            etl.data = {'some': 'data'}

            if data_content == 0:
                with mock.patch(path, get_df_box_score_mock):
                    etl.transform_box_score()
            
            get_df_box_score_mock.assert_called_once_with(etl.data)

            if data_content == 1:
                with self.assertRaises(ValueError):
                    etl.transform_box_score()

    def test_transform(self):
        tables_dict_mock = mock.Mock()
        save_all_tables_mock = mock.Mock()
        path = (
            'triple_triple_etl.load.postgres.'
            'nbastats_postgres_etl.save_all_tables'
        )

        for data_content in [0, 1]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content
            )
            etl.transform_play_by_play = mock.Mock(return_value=tables_dict_mock)
            etl.transform_box_score = mock.Mock(return_value=tables_dict_mock)
            etl.storage_dir = 'here'

            with mock.patch(path, save_all_tables_mock):
                etl.transform()

            if data_content == 0:
                etl.transform_box_score.assert_called_once_with()
                save_all_tables_mock.assert_called_once_with(
                    tables_dict=tables_dict_mock,
                    storage_dir='here'
                )
            elif data_content == 1:
                etl.transform_play_by_play.assert_called_once_with()
                assert save_all_tables_mock.call_count == 2

    def test_load(self):
        csv2postgres_mock = mock.Mock()
        etl = NBAStatsPostgresETL(
            base_url='some_base',
            params='some_params',
            data_content='0/1'
        )
        path = 'triple_triple_etl.load.postgres.nbastats_postgres_etl.csv2postgres'
        with mock.patch(path, csv2postgres_mock):
            etl.load(filepath='some.csv')

        csv2postgres_mock.assert_called_once_with('some.csv')

    def test_run(self): 
        filenames = [
            'here/box_score.csv',
            'here/play_by_play.csv'
        ]
        os_mock = mock.Mock()
        os_mock.path.join.side_effect = filenames

        for data_content in [0, 1]:
            etl = NBAStatsPostgresETL(
                base_url='some_base',
                params='some_params',
                data_content=data_content
            )
            etl.storage_dir = 'here'
            etl.extract_from_nbastats = mock.Mock(return_value='/some/dir')
            etl.transform = mock.Mock()
        
            etl.load = mock.Mock()
            

            path = 'triple_triple_etl.load.postgres.nbastats_postgres_etl.os'
            with mock.patch(path, os_mock):
                etl.run()
            
            if data_content == 0:
                etl.load.assert_called_once_with(filenames[0])
                os_mock.remove.assert_called_once_with(filenames[0])
                
            elif data_content == 1:
                etl.load.assert_called_once_with(filenames[1])
                os_mock.remove.assert_has_calls(
                    [mock.call(f) for f in filenames]
                )


if __name__ == '__main__':
    unittest.main()

