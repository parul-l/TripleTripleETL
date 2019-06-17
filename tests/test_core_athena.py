import io

import boto3
import mock
import unittest

from triple_triple_etl.core.athena import (
    execute_athena_query,
    get_query_s3filepath,
    check_table_exists,
)
from triple_triple_etl.core.s3 import remove_bucket_contents
from triple_triple_etl.constants import ATHENA_OUTPUT


class TestAthena(unittest.TestCase):
    """Tests for athena.py"""

    def test_execute_athena_query(self):
        athena = boto3.client('athena', region_name='us-east-1')
        s3 = boto3.client('s3', region_name='us-east-1')
        
        query = 'SELECT COUNT(*) FROM nba.teaminfo'
        output_filename = 'nosetest_execute_athena_query'
        response = execute_athena_query(
            query=query,
            database='nba',
            output_filename=output_filename,
            boto3_client=athena
        )
        # check query executed
        self.assertEqual(
            first=response['ResponseMetadata']['HTTPStatusCode'],
            second=200
        )

        # remove test bucket
        key_csv = '{}/{}.csv'.format(output_filename, response['QueryExecutionId'])
        key_meta = '{}/{}.csv.metadata'.format(output_filename, response['QueryExecutionId'])
        for key in [key_csv, key_meta]:
            remove_bucket_contents(
                bucket=ATHENA_OUTPUT,
                key=key,
                max_time=0,
                s3client=s3,
            )
    
    def test_get_query_s3filepath(self):
        athena_mock = mock.Mock()
        athena_mock.get_query_execution.return_value = {
            'QueryExecution': {
                'ResultConfiguration': {
                    'OutputLocation': 'someresponse'
                }
            }
        }
        execute_athena_response = {'QueryExecutionId': 'some_id'}
        
        _ = get_query_s3filepath(
            execute_athena_response=execute_athena_response,
            boto3_client=athena_mock
        )    
        athena_mock.get_query_execution.assert_called_once_with(
            QueryExecutionId=execute_athena_response['QueryExecutionId']
        )

    def test_check_table_exists(self):
        glue_mock = mock.Mock()

        return_values = [['somelist'], []]
        expected_response = [1, 0]

        for return_value, response in zip(return_values, expected_response):
            glue_mock.get_tables.return_value = {'TableList': return_value}
            path = 'triple_triple_etl.core.athena.glue'
            with mock.patch(path, glue_mock):
                table_exists = check_table_exists(
                    database_name='some_db',
                    table_name='some_table',
                    max_time=0
                )
                self.assertEqual(first=table_exists, second=response)


if __name__ == '__main__':
    unittest.main()