import mock
import unittest

from triple_triple_etl.core.nbastats_get_data import get_data


class TestNBAStatsGetData(unittest.TestCase):
    """Tests for nbastats_get_data.py"""
    
    def test_get_data(self):
        response_mock = mock.Mock()
        status_values = [200, 300]
        path = 'triple_triple_etl.core.nbastats_get_data.requests.get'

        for code in status_values:        
            response_mock.status_code = code
            requests_get_mock = mock.Mock(return_value=response_mock)
       
            with mock.patch(path, requests_get_mock):
                get_data(
                    base_url='some_url',
                    params='some_params',
                    headers='headers'
                )
            requests_get_mock.assert_called_once_with(
                url='some_url',
                params='some_params',
                headers='headers'
            )

            if code == 200:
                assert not response_mock.raise_for_status.called
                response_mock.json.assert_called_once_with()
            if code == 300:
                response_mock.raise_for_status.assert_called_once_with()
                assert response_mock.json.call_count == 2


if __name__ == '__main__':
    unittest.main()
