########################################################################
# This file loads the teamshootingside tables
# Data is brought down locally before uploading again
# This costs money on aws
# See `closest_to_ball_rank` for a better method.

# This file has two pipelines to load team_shooting_side data.
# TeamShootingSideALL queries all games in the `nba.gamelog` database
# and uploads the result to s3.
# TeamShootingSideETL queries data for only one game. Useful once the initial
# table is created and we just want to append data.
########################################################################
import boto3
import datetime
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import tempfile

from triple_triple_etl.constants import ATHENA_OUTPUT, META_DIR, SQL_DIR 
from triple_triple_etl.core.athena import execute_athena_query
from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata
from triple_triple_etl.log import get_logger

s3 = boto3.resource('s3')
athena = boto3.client('athena')

THIS_FILENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_FILENAME = '{}.log'.format(os.path.splitext(THIS_FILENAME)[0])
logger = get_logger(output_file=LOG_FILENAME)


def get_file_idx_in_uploaded(
        gameid: str,
        df_uploaded: pd.DataFrame
):
    # get index if file exists or create a row with that gameid
    try:
        return df_uploaded.query('gameid == @gameid').index[0]
    except:
        return df_uploaded.shape[0]


class TeamShootingSideALL(object):
    """
        This ETL adds the 'team shooting side' data of a specific game
        to the `team_shooting_side` folder.
    """
    def __init__(
            self,
            source_bucket: str = ATHENA_OUTPUT,
            destination_bucket: str = 'nba-game-info',
            database: str = 'nba' # database where tables are stored
    ):
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.database = database
        self.tmp_dir = None
        self.df_uploaded = None
        self.query = None
        self.file_idx = None
        self.season = None
        self.gameid = None
        self.gamedate = None
        self.team1 = None
        self.team2 = None


        metadata_filename = 'teamshootingside.parquet.snappy'
        self.uploaded_filepath = os.path.join(META_DIR, metadata_filename)
    
    def metadata(self):
        logger.info('Getting loaded metadata')
        # meta data of df columns
        columns = [
            'season',
            'gameid',
            'gamedate',
            'teamid1',
            'teamid2',
            'uploadedFLG',
            'lastuploadDTS'
        ]
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )
        self.file_idx = self.df_uploaded.shape[0]
        
    def execute_query(self, output_filename='team_shooting_side'):
        logger.info('Executing query')
        query_path = os.path.join(
            SQL_DIR,
            'team_shooting_side',
            'team_shooting_side_all.sql'
        )

        with open(query_path) as f:
            self.query = f.read()
        
        response = execute_athena_query(
            query=self.query, 
            database=self.database, 
            boto3_client=athena,
            output_filename=output_filename
        )
        # return s3key
        return '{}/{}.csv'.format(output_filename, response['QueryExecutionId'])
    
    def extract(self, s3key: str):
        logger.info('Extract data locally')
        self.tmp_dir = tempfile.mkdtemp()
        output_filepath = os.path.join(self.tmp_dir, 'team_shooting_side.csv')
        
        try:
            bucket = s3.Bucket(self.source_bucket)
            bucket.download_file(Key=s3key, Filename=output_filepath)
        except boto3.exceptions.botocore.client.ClientError as err:
            logger.error(err)
            raise
        return output_filepath

    def transform(self, output_filepath: str):
        logger.info('Read .csv dataframe')
        df = pd.read_csv(output_filepath, dtype={'gameid': str})

        logger.info('Convert to parquet')
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table=table,
            root_path=self.tmp_dir,
            partition_cols=['season', 'gameid'],
            compression='snappy',
            preserve_index=False
        )
        return df
    
    def load(self, df: pd.DataFrame):
        # get season dir
        season_dir = [x for x in os.listdir(self.tmp_dir) if 'season' in x][0]
        season_filepath = os.path.join(self.tmp_dir, season_dir)
        all_files = os.listdir(season_filepath)
        
        for game in all_files:
            # upload to s3 team_shooting_side folder
            self.gameid = game.split('=')[1]
            filename = os.listdir(os.path.join(season_filepath, game))[0]
            full_path = os.path.join(season_filepath, game, filename)

            # collect meta data
            df_game = df.query('gameid == @self.gameid')\
                        .query('period == 1')
            self.season = df_game.season.iloc[0]
            self.gamedate = df_game.game_date.iloc[0].replace('-', '')
            self.team1 = df_game.team_abbreviation.iloc[0]
            self.team2 = df_game.team_abbreviation.iloc[1]        

            # update the uploadDTS stamp
            today = datetime.datetime.utcnow().strftime('%F %TZ')
            self.df_uploaded.loc[self.file_idx, 'lastuploadDTS'] = today
            # update season
            self.df_uploaded.loc[self.file_idx, 'season'] = self.season
            # update gameid
            self.df_uploaded.loc[self.file_idx, 'gameid'] = self.gameid
            # update gamedate
            self.df_uploaded.loc[self.file_idx, 'gamedate'] = self.gamedate
            # update teams
            self.df_uploaded.loc[self.file_idx, 'teamid1'] = self.team1
            self.df_uploaded.loc[self.file_idx, 'teamid2'] = self.team2

            try:
                logger.info('Uploading game {} to s3'.format(self.gameid))
                s3.meta.client.upload_file(
                    Filename=full_path,
                    Bucket=self.destination_bucket,
                    Key = '{}/season={}/gameid={}/{}{}{}.parquet.snappy'.format(
                        'team_shooting_side',
                        self.season,
                        self.gameid,
                        self.gamedate,
                        self.team1,
                        self.team2
                    )
                )
                # update metadata
                self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 1

            except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
                logger.error(err)
                self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 0
    
    def cleanup(self):
        # remove tmp directory
        logger.info('Remove temporary directory')
        shutil.rmtree(self.tmp_dir)

        # save df_uploaded
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')

    def run(self):
        # etl start time
        start_time = datetime.datetime.utcnow()

        self.metadata()
        s3key = self.execute_query()
        output_filepath = self.extract(s3key)
        df = self.transform(output_filepath)
        self.load(df)
        self.cleanup()

        # etl end time
        end_time = datetime.datetime.utcnow()
        time_delta = round((end_time - start_time).seconds / 60., 2)
        msg = 'It took {} minutes to load all data.'.format(time_delta)
        logger.info(msg)


class TeamShootingSideETL(object):
    """
        This ETL adds the 'team shooting side' data of a specific game
        to the `team_shooting_side` folder.
    """
    def __init__(
            self,
            gameid: str,
            source_bucket: str = ATHENA_OUTPUT,
            destination_bucket: str = 'nba-game-info',
            database: str = 'nba' # database where tables are stored
    ):
        self.gameid = gameid
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.database = database
        self.tmp_dir = None
        self.df_uploaded = None
        self.file_idx = None
        self.query = None
        self.team1 = None
        self.team2 = None
        self.gamedate = None

        metadata_filename = 'teamshootingside.parquet.snappy'
        self.uploaded_filepath = os.path.join(META_DIR, metadata_filename)
    
    def metadata(self):
        logger.info('Getting loaded metadata')
        # meta data of df columns
        columns = [
            'gameid',
            'teamid1',
            'teamid2',
            'uploadedFLG',
            'lastuploadDTS'
        ]

        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )

        self.file_idx = get_file_idx_in_uploaded(
            df_uploaded=self.df_uploaded,
            gameid=self.gameid
        )
        
    def execute_query(self, output_filename='team_shooting_side'):
        logger.info('Executing query')
        query_path = os.path.join(
            SQL_DIR,
            'team_shooting_side',
            'team_shooting_side_per_game.sql'
        )

        with open(query_path) as f:
            self.query = f.read().format(self.gameid)
        
        response = execute_athena_query(
            query=self.query, 
            database=self.database, 
            boto3_client=athena,
            output_filename=output_filename
        )

        # return s3key
        return '{}/{}.csv'.format(output_filename, response['QueryExecutionId'])
    
    def extract(self, s3key: str):
        logger.info('Extract data locally')
        self.tmp_dir = tempfile.mkdtemp()
        output_filepath = os.path.join(self.tmp_dir, '{}.{}'.format(self.gameid, 'csv'))
        
        try:
            bucket = s3.Bucket(self.source_bucket)
            bucket.download_file(Key=s3key, Filename=output_filepath)
        except boto3.exceptions.botocore.client.ClientError as err:
            logger.error(err)
            raise
        return output_filepath

    def transform(self, output_filepath: str):
        logger.info('Read .csv dataframe')
        df = pd.read_csv(output_filepath, dtype={'gameid': str})
        # get teams/date
        logger.info('Collect teams and date')
        self.team1 = df.team_abbreviation.unique()[0]
        self.team2 = df.team_abbreviation.unique()[1]
        self.gamedate = df.gamedate.iloc[0].replace('-', '')

        logger.info('Convert to parquet')
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table=table,
            root_path=self.tmp_dir,
            partition_cols=['season', 'gameid'],
            compression='snappy',
            preserve_index=False
        )
    
    def load(self):
        # upload to s3 team_shooting_side folder
        try:
            self.logger.info('Uploading game {} to s3'.format(self.gameid))
            filepath = os.path.join(
                self.tmp_dir,
                'season={}'.format(self.season),
                'gameid={}'.format(self.gameid)
            )
            filename = os.listdir(filepath)[0]

            s3.meta.client.upload_file(
                Filename=os.path.join(filepath, filename),
                Bucket=self.destination_bucket,
                Key = '{}/season={}/gameid={}/{}{}{}.parquet.snappy'.format(
                    'team_shooting_side',
                    self.season,
                    self.gameid,
                    self.gamedate,
                    self.team1,
                    self.team2
                )
            ) 

            # update metadata
            self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 1

        except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
            logger.error(err)
            self.df_uploaded.loc[self.file_idx, 'uploadedFLG'] = 0
    

    def cleanup(self):
        # remove tmp directory
        logger.info('Remove temporary directory')
        shutil.rmtree(self.tmp_dir)

        # update the uploadDTS stamp
        today = datetime.datetime.utcnow().strftime('%F %TZ')
        self.df_uploaded.loc[self.file_idx, 'lastuploadDTS'] = today
        # update gameid
        self.df_uploaded.loc[self.file_idx, 'gameid'] = self.gameid
        # update gamedate
        self.df_uploaded.loc[self.file_idx, 'gamedate'] = self.gamedate
        # save df_uploaded
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')

    
    def run(self):
        # etl start time
        start_time = datetime.datetime.utcnow()

        self.metadata()
        s3key = self.execute_query()
        output_filepath = self.extract(s3key)
        self.transform(output_filepath)
        self.load()
        self.cleanup()

        # etl end time
        end_time = datetime.datetime.utcnow()
        time_delta = round((end_time - start_time).seconds / 60., 2)
        msg = 'It took {} minutes to load game {}.'\
               .format(time_delta, self.gameid)
        logger.info(msg)

