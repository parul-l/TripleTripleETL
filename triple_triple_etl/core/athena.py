import os
import boto3


from triple_triple_etl.constants import ATHENA_OUTPUT


athena = boto3.client('athena')
s3 = boto3.resource('s3')


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
