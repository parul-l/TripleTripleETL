import boto3
import datetime
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import tempfile
import time

from triple_triple_etl.constants import ATHENA_OUTPUT, META_DIR, SQL_DIR 
from triple_triple_etl.core.athena import execute_athena_query
from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata
from triple_triple_etl.log import get_logger

s3 = boto3.resource('s3')
s3client = boto3.client('s3')
athena = boto3.client('athena')

THIS_FILENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_FILENAME = '{}.log'.format(os.path.splitext(THIS_FILENAME)[0])
logger = get_logger(output_file=LOG_FILENAME)



def create_tmp_tables(
        query: str,
        table: str,
        gameid_bounds: list,
        athena
):
    """
        This function executes a query and stores it in the
        default athena query folder in s3.
    
    Parameters
    ----------
        query: `str`
            The sql query desired to be executed
        table: `str`
            Either 'ball_dist' or 'closest_to_ball' 
        gameid_bounds: `list`
            A list with the lower and upper bounds of the 
            gameids included in the query
        athena: The boto3 athena client `boto3.client('athena')`
        
    
    Returns
    -------
    A `str` of the s3 key where the executed query is stored.
    """
    output_filename = '{}_gameids_{}_to_{}'.format(
        table,
        gameid_bounds[0],
        gameid_bounds[1]
    )
    response = execute_athena_query(
        query=query, 
        database='nba', 
        boto3_client=athena,
        output_filename=output_filename
    )
    # return s3key
    return '{}/{}.csv'.format(output_filename, response['QueryExecutionId'])


class ClosestToBallETL(object):
    def __init__(
            self, 
            season: str,
            gameid_bounds: list,     #[lower_bound, upper_bound]
            sql_filepath: str,
            source_bucket: str = ATHENA_OUTPUT
    ):
        self.season = season
        self.gameid_bounds = gameid_bounds
        self.source_bucket = source_bucket
        self.query_balldist = None
        self.query_closest_to_ball = None


    def get_queries(self, sql_path_balldist, sql_path_closest_to_ball):
        # ball_dist
        bal_dist_query_path = os.path.join(SQL_DIR, sql_path_balldist)
        closest_to_ball_query_path = os.path.join(SQL_DIR, sql_path_closest_to_ball)

        with open(bal_dist_query_path) as f:
            self.query_balldist = f.read().format(
                self.season
                self.gameid_bounds[0],
                self.gameid_bounds[1]
            )
        with open(closest_to_ball_query_path) as f:
            self.query_closest_to_ball = f.read()


    def create_ball_dist_tmp(self):
        output_filename = 'ball_dist_gameids_{}_to_{}'.format(
            self.gameid_bounds[0],
            self.gameid_bounds[1]
        )
        response = execute_athena_query(
            query=self.query_balldist, 
            database='nba', 
            boto3_client=athena,
            output_filename=output_filename
        )
        # return s3key
        return '{}/{}.csv'.format(output_filename, response['QueryExecutionId'])


    def create closest_to_ball_tmp(self):
        response = execute_athena_query(
            query=self.query_closest_to_ball, 
            database='nba', 
            boto3_client=athena,
            output_filename='gameids_{}_to_{}'.format(self.gameid_bounds[0], self.gameid_bounds[1])
        )
        # return s3key
        return '



# def get_file_idx_in_uploaded(
#         gameid: str,
#         df_uploaded: pd.DataFrame
# ):
#     # get index if file exists or create a row with that gameid
#     try:
#         return df_uploaded.query('gameid == @gameid').index[0]
#     except:
#         return df_uploaded.shape[0]

# class ClosestToBallETL(object):
#     """
#         This ETL adds the 'closest to ball' data of a specific game
#         to the `closest_to_ball` folder.
#     """
#     def __init__(
#             self,
#             gameid: str,
#             source_bucket: str = ATHENA_OUTPUT,
#             destination_bucket: str = 'nba-game-info',
#             database: str = 'nba', # database where tables are stored
#             output_filename: str = 'closest_to_ball',
#             sql_path_subdir: str = 'player',
#             sql_filename: str = 'closest_to_ball.sql'

#     ):
#         self.gameid = gameid
#         self.source_bucket = source_bucket
#         self.destination_bucket = destination_bucket
#         self.database = database
#         self.output_filename = output_filename
#         self.sql_path_subdir = sql_path_subdir
#         self.sql_filename = sql_filename
#         self.tmp_dir = None
#         self.df_uploaded = None
#         self.file_idx = None
#         self.query = None
#         self.season = None
#         self.gamedate = None

#         metadata_filename = '{}.parquet.snappy'.format(self.output_filename)
#         self.uploaded_filepath = os.path.join(META_DIR, metadata_filename)

#     def metadata(self):
#         logger.info('Getting loaded metadata')
#         # meta data of df columns
#         columns = [
#             'season',
#             'gameid',
#             'gamedate',
#             'uploadedFLG',
#             'lastuploadDTS'
#         ]

#         self.df_uploaded = get_uploaded_metadata(
#             self.uploaded_filepath,
#             columns=columns
#         )

#         self.file_idx = get_file_idx_in_uploaded(
#             df_uploaded=self.df_uploaded,
#             gameid=self.gameid
#         )

#     def execute_query(self):
#         logger.info('Executing query')
#         query_path = os.path.join(
#             SQL_DIR,
#             self.sql_path_subdir,
#             self.sql_filename
#         )

#         with open(query_path) as f:
#             self.query = f.read().format(self.gameid)
        
#         response = execute_athena_query(
#             query=self.query, 
#             database=self.database, 
#             boto3_client=athena,
#             output_filename=self.output_filename
#         )

#         # return s3key
#         return '{}/{}.csv'.format(self.output_filename, response['QueryExecutionId'])

#     def extract(self, s3key: str):
#         logger.info('Extract data locally')
#         self.tmp_dir = tempfile.mkdtemp()
#         local_output_filepath = os.path.join(self.tmp_dir, '{}.{}'.format(self.gameid, 'csv'))
        
#         try:
#             bucket = s3.Bucket(self.source_bucket)
#             bucket.download_file(Key=s3key, Filename=local_output_filepath)
#         except boto3.exceptions.botocore.client.ClientError as err:
#             logger.error(err)
#             raise
#         return local_output_filepath

#     def transform(self, local_output_filepath: str):
#         logger.info('Read .csv dataframe')
#         df = pd.read_csv(local_output_filepath, dtype={'gameid': str})

#         # extract season
#         self.season = df.season.iloc[0]
#         # extract gamedate
#         self.gamedate = df.timestamp_dts.iloc[0].split(' ')[0].replace('-', '')

#         logger.info('Convert to parquet')
#         table = pa.Table.from_pandas(df, preserve_index=False)
#         pq.write_to_dataset(
#             table=table,
#             root_path=self.tmp_dir,
#             partition_cols=['season', 'gameid'],
#             compression='snappy',
#             preserve_index=False
#         )

#     def load(self):
#         # upload to s3 team_shooting_side folder
#         try:
#             logger.info('Uploading game {} to s3'.format(self.gameid))
#             filepath = os.path.join(
#                 self.tmp_dir,
#                 'season={}'.format(self.season),
#                 'gameid={}'.format(self.gameid)
#             )
#             filename = os.listdir(filepath)[0]

#             s3.meta.client.upload_file(
#                 Filename=os.path.join(filepath, filename),
#                 Bucket=self.destination_bucket,
#                 Key = '{}/season={}/gameid={}/{}.parquet.snappy'.format(
#                     self.output_filename,
#                     self.season,
#                     self.gameid,
#                     self.gamedate
#                 )
#             ) 

#             # update metadata
#             self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 1

#         except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
#             logger.error(err)
#             self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 0


#     def cleanup(self):
#         # remove tmp directory
#         logger.info('Remove temporary directory')
#         shutil.rmtree(self.tmp_dir)

#         # update the uploadDTS stamp
#         today = datetime.datetime.utcnow().strftime('%F %TZ')
#         self.df_uploaded.loc[self.file_idx, 'lastuploadDTS'] = today
#         # update gameid
#         self.df_uploaded.loc[self.file_idx, 'gameid'] = self.gameid
#         # update gamedate
#         self.df_uploaded.loc[self.file_idx, 'gamedate'] = self.gamedate
#         # save df_uploaded
#         self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')

#     def run(self):
#         # etl start time
#         start_time = datetime.datetime.utcnow()

#         self.metadata()
#         s3key = self.execute_query()
#         # wait for file to load
#         response = s3client.list_objects(Bucket=self.source_bucket, Prefix=s3key)\
#                            .get('Contents')
        
#         time_to_appear = 0
#         while not response:
#             time.sleep(1)
#             response = s3client.list_objects(Bucket=self.source_bucket, Prefix=s3key)\
#                                .get('Contents')
#             time_to_appear += 1
        
#         logger.info('It took {} seconds for query to appear on s3'.format(time_to_appear))


#         local_output_filepath = self.extract(s3key)
#         self.transform(local_output_filepath)
#         self.load()
#         self.cleanup()

#         # etl end time
#         end_time = datetime.datetime.utcnow()
#         time_delta = round((end_time - start_time).seconds / 60., 2)
#         msg = 'It took {} minutes to load game {}.'\
#                .format(time_delta, self.gameid)
#         logger.info(msg)


# def run_all_closest_to_ball(
#         all_games: list,
#         idx: list = [],
#         source_bucket: str = ATHENA_OUTPUT,
#         destination_bucket: str = 'nba-game-info'
# ):

#     if idx:
#         all_games = np.array(all_games)[idx]

#     start_time = datetime.datetime.now()
#     for i, gameid in enumerate(all_games):
#         logger.info('Running etl file {} of {}'.format(i + 1, len(all_games)))
#         try:
#             etl = ClosestToBallETL(
#                 gameid=gameid,
#                 source_bucket=source_bucket,
#                 destination_bucket=destination_bucket
#             )
#             etl.run()
#             etl_time = datetime.datetime.now()
#             time_delta = round((etl_time - start_time).seconds / 60., 2)
#             logger.info("It's been {} minutes".format(time_delta))

#         except Exception as err:
#             logger.error('Error with file {}, {}'.format(i, gameid))
#             continue

#     end_time = datetime.datetime.now()
#     time_delta = round((end_time - start_time).seconds / 60., 2)

#     logger.info('It took {} minutes to load'.format(time_delta))


