import os
import boto3
import logging
import time

from triple_triple_etl.constants import ATHENA_OUTPUT


athena = boto3.client('athena')
glue = boto3.client('glue')
s3 = boto3.resource('s3')

# initiate logger
logger = logging.getLogger()
logger.setLevel('INFO')

def execute_athena_query(
        query: str,
        database: str,
        output_filename: str,
        boto3_client #: botocore.client.Athena
):

    """
        This function executes a query and stores it in the
        default athena query folder in s3.
    
    Parameters
    ----------
        query: `str`
            The sql query desired to be executed

        database: `str`
            The database where tables are stored
        
        output_filename: `str`
            A folder within the default aws-athena-query to store the results
            of the query
        
        boto3_client: `boto`?
            The boto3.client('athena') client.
    
    Returns
    -------
    A `dict` of the query execution meta data. 
    This has two keys:
        `QueryExecutionId` and `ResponseMetadata`.
    """
    return boto3_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={
            'OutputLocation': os.path.join('s3://', ATHENA_OUTPUT, output_filename)
        }
    )


def get_query_s3filepath(
        execute_athena_response: dict,
        boto3_client #: botocore.client.Athena
):
    """
        This function takes the response of the `execute_athena_query` response 
        function and returns the location of the results on s3.

    Parameters
    ----------
        execute_athena_response: `dict`
            This is the dict returned by `execute_athena_query`.
        boto3_client:
            The boto3.client('athena') client.
    Returns
    -------
    A `dict` of the query execution meta data. 
    This has two keys:
        `QueryExecution` and `ResponseMetadata`.
    """

    execution_id = execute_athena_response['QueryExecutionId']
    response = boto3_client.get_query_execution(QueryExecutionId = execution_id)

    return response['QueryExecution']['ResultConfiguration']['OutputLocation']


def check_table_exists(
        database_name: str,
        table_name: str,
        max_time: int = 0
):
    response = glue.get_tables(DatabaseName=database_name, Expression=table_name)

    time_to_appear = 0
    time_increment = 1

    while not response['TableList'] and time_to_appear <= max_time:    
        time.sleep(time_increment)
        time_to_appear += time_increment

        response = glue.get_tables(DatabaseName=database_name, Expression=table_name)

    if response['TableList']:
        logger.info('It took {} seconds for the table to appear'.format(time_to_appear))
        return 1
    else:
        logger.info('Table did not appear in the max time of {} seconds'.format(max_time))
        return 0

    

