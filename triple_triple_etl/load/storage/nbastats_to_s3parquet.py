import boto3
import datetime
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import shutil
import tempfile
import time

import warnings

from triple_triple_etl.constants import (
    BASE_URL_PLAY,
    BASE_URL_BOX_SCORE_TRADITIONAL,
    BASE_URL_BOX_SCORE_PLAYER_TRACKING,
    DESTINATION_BUCKET,
    META_DIR,
    NBASTATS_PARAMS
)
from triple_triple_etl.core.nbastats_get_data import get_data
from triple_triple_etl.core.nbastats_json2df import (
    get_play_by_play,
    get_boxscore
)
from triple_triple_etl.log import get_logger
from triple_triple_etl.load.storage.load_helper import (
    get_uploaded_metadata,
    format_year
)
    

warnings.simplefilter(action='ignore', category=FutureWarning)

s3 = boto3.client('s3', region_name='us-east-1')

THIS_FILENAME = os.path.splitext(os.path.basename(__file__))[0]



def get_file_idx_in_uploaded(
        df_uploaded: pd.DataFrame,
        gameid: str
):
    # get index if file exists or create a row with that filename
    try:
        return df_uploaded\
            .query('game_id == @gameid')\
            .index[0]
    except:
        return df_uploaded.shape[0]


class NBAStatsS3ETL(object):
    """
    Extract data from NBA stats API. 
    'game_data_type' is one of {'playbyplay', 'boxscore_traditional', 'boxscore_player'}
    """
    def __init__(
        self,
        gameid: str,
        gamedate: str,
        team1: str,
        team2: str,
        game_data_type: str,
        season: str = '2015-16',
        destination_bucket: str = DESTINATION_BUCKET
    ):

        self.gameid = gameid
        self.gamedate = gamedate
        self.team1 = team1
        self.team2 = team2
        self.game_data_type = game_data_type
        self.season = season
        self.destination_bucket = destination_bucket
        self.tmp_dir = tempfile.mkdtemp()
        self.uploadedFLG = None
        self.params = NBASTATS_PARAMS

        # initialize logger
        log_filename = '{}_{}.log'.format(
            self.game_data_type,
            os.path.splitext(THIS_FILENAME)[0]
        )
        self.logger = get_logger(output_file=log_filename)

        # meta data file
        meta_data_filename = os.path.join(META_DIR, THIS_FILENAME)
        self.uploaded_filepath = '{}.parquet.snappy'.format(meta_data_filename)

        # api info
        # update params
        self.params['Season'] = self.season
        self.params['GameID'] = self.gameid

        # update base url
        if self.game_data_type == 'playbyplay':
            self.base_url = BASE_URL_PLAY
        elif self.game_data_type == 'boxscore_traditional':
            self.base_url = BASE_URL_BOX_SCORE_TRADITIONAL
        elif self.game_data_type == 'boxscore_player':
            self.base_url = BASE_URL_BOX_SCORE_PLAYER_TRACKING

    def metadata(self):
        self.logger.info('Getting loaded files metadata')
        # meta data columns
        columns = [
            'season', 'game_date', 'game_id', 'team1', 'team2',
            'fileuploadedFLG', 'lastuploadDTS',
            'base_url', 'params'
        ]
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )

        # get index of filename
        self.file_idx = get_file_idx_in_uploaded(
            df_uploaded=self.df_uploaded,
            gameid=self.gameid
        )

    def extract(self):
        self.logger.info(
            'Getting {} data of game {}'.format(self.game_data_type, self.gameid)
        )
        return get_data(base_url=self.base_url, params=self.params)

    def transform(self, data):
        self.logger.info('Convert json to pyarrow table')
        if self.game_data_type == 'playbyplay':
            table = get_play_by_play(data, season=self.season)
        elif self.game_data_type == 'boxscore_traditional':
            table = get_boxscore(data, season=self.season, traditional_player='traditional')
        elif self.game_data_type == 'boxscore_player':
            table = get_boxscore(data, season=self.season, traditional_player='player')

        pq.write_to_dataset(
            table=table,
            root_path=self.tmp_dir,
            partition_cols=['season', 'game_id'],
            compression='snappy',
            preserve_index=False
        )  
    
    def load(self):
        try:
            self.logger.info(
                'Uploading {} {} to s3'.format(self.game_data_type, self.gameid)
            )
            filepath = os.path.join(
                self.tmp_dir,
                'season={}'.format(self.season),
                'game_id={}'.format(self.gameid)
            )
            filename = os.listdir(filepath)[0]
            s3.upload_file(
                Filename=os.path.join(filepath, filename),
                Bucket=self.destination_bucket,
                Key='{}/season={}/gameid={}/{}{}{}.parquet.snappy'.format(
                    self.game_data_type,
                    format_year(self.season),
                    self.gameid,
                    self.gamedate.replace('-', ''),
                    self.team1,
                    self.team2
                )
            )
            self.uploadedFLG = 1
        except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
            self.logger.error(err)
            self.uploadedFLG = 0

    def cleanup(self):
        self.logger.info('Removing tmp dir')
        # remove tmp dir
        shutil.rmtree(self.tmp_dir)
        # update metadata info
        today = datetime.datetime.utcnow().strftime('%F %TZ')

        self.logger.info('Updating upload meta data')
        self.df_uploaded.loc[self.file_idx] = [
            self.season,
            self.gamedate,
            self.gameid,
            self.team1,
            self.team2,
            self.uploadedFLG,
            today,
            self.base_url,
            str(self.params)
        ]
    
        self.df_uploaded.to_parquet(
            fname=self.uploaded_filepath,
            compression='snappy',
            index=False
        )

    def run(self):
        self.metadata()
        data = self.extract()
        self.transform(data)
        self.load()
        self.cleanup()


def upload_all_nbastats(
        df_gamelog_meta: pd.DataFrame,
        game_data_type: str,
        start_date: str = None,
        end_date: str = None,
):
    """
    Extract ALL games data from NBA stats API. 
    'game_data_type' is one of {'playbyplay', 'boxscore_traditional', boxscore_player'}
    """
    if not start_date:
        start_date = df_gamelog_meta.sort_values('game_date').game_date.iloc[0]
    if not end_date:
        end_date = df_gamelog_meta.sort_values('game_date').game_date.iloc[-1]
    
    cols = ['season', 'game_date', 'game_id', 'team1', 'team2']
    df = df_gamelog_meta[cols].drop_duplicates(subset=['game_id'])\
                 .query('@start_date <= game_date <= @end_date')
    
    start_time = datetime.datetime.now()
    for i, row in enumerate(df.values):
        try:
            etl = NBAStatsS3ETL(
                season=row[0],
                gamedate=row[1],
                gameid=row[2],
                team1=row[3],
                team2=row[4],
                game_data_type=game_data_type,
                destination_bucket=DESTINATION_BUCKET
            )
            etl.logger.info('Running game {} of {}'.format(i + 1, df.shape[0]))
            etl.run()
            
            etl_time = datetime.datetime.now()
            time_delta = round((etl_time - start_time).seconds / 60., 2)
            etl.logger.info("It's been {} minutes".format(time_delta))

            # pause every 10 games so we don't go over request limit
            if i % 20 == 0:
                print('Sleep for a min to not overload api calls')
                time.sleep(60)
        except Exception as err:
            etl.logger.error('Error with game {}, {}'.format(i, row[2]))
            continue

    end_time = datetime.datetime.now()
    time_delta = round((end_time - start_time).seconds / 60., 2)

    etl.logger.info('It took {} minutes to load'.format(time_delta))










# class NBAStatsPlayByPlayS3ETL(object):
#     def __init__(
#         self,
#         gameid: str,
#         gamedate: str,
#         team1: str,
#         team2: str,
#         season: str = '2015-16',
#         destination_bucket: str = DESTINATION_BUCKET
#     ):

#         self.gameid = gameid
#         self.gamedate = gamedate
#         self.team1 = team1
#         self.team2 = team2
#         self.season = season
#         self.destination_bucket = destination_bucket
#         self.tmp_dir = tempfile.mkdtemp()
#         self.uploadedFLG = None

#         # meta data file
#         meta_data_filename = os.path.join(META_DIR, THIS_FILENAME)
#         self.uploaded_filepath = '{}.parquet.snappy'.format(meta_data_filename)

#         # api info
#         self.base_url = BASE_URL_PLAY
#         self.params = {
#             'EndPeriod': '10',
#             'EndRange': '55800',    
#             'GameID': self.gameid,
#             'RangeType': '2',       
#             'Season': self.season,
#             'SeasonType': 'Regular Season',
#             'StartPeriod': '1',     
#             'StartRange': '0',      
#         }

#     def metadata(self):
#         logger.info('Getting loaded files metadata')
#         # meta data columns
#         columns = [
#             'season', 'game_date', 'game_id', 'team1', 'team2',
#             'fileuploadedFLG', 'lastuploadDTS',
#             'base_url', 'params'
#         ]
#         self.df_uploaded = get_uploaded_metadata(
#             self.uploaded_filepath,
#             columns=columns
#         )

#         # get index of filename
#         self.file_idx = get_file_idx_in_uploaded(
#             df_uploaded=self.df_uploaded,
#             gameid=self.gameid
#         )

#     def extract(self):
#         logger.info('Getting play by play data of game {}'.format(self.gameid))
#         return get_data(base_url=self.base_url, params=self.params)
    
#     def transform(self, data):
#         logger.info('Convert json to pyarrow table')
#         table = get_play_by_play(data)

#         pq.write_to_dataset(
#             table=table,
#             root_path=self.tmp_dir,
#             partition_cols=['season', 'game_id'],
#             compression='snappy',
#             preserve_index=False
#         )  
    
#     def load(self):
#         try:
#             logger.info('Uploading {} to s3'.format(self.gameid))
#             filepath = os.path.join(
#                 self.tmp_dir,
#                 'season={}'.format(self.season),
#                 'game_id={}'.format(self.gameid)
#             )
#             filename = os.listdir(filepath)[0]
#             s3.upload_file(
#                 Filename=os.path.join(filepath, filename),
#                 Bucket=self.destination_bucket,
#                 Key='playbyplay/season={}/gameid={}/{}{}{}.parquet.snappy'.format(
#                     format_year(self.season),
#                     self.gameid,
#                     self.gamedate.replace('-', ''),
#                     self.team1,
#                     self.team2
#                 )
#             )
#             self.uploadedFLG = 1
#         except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
#             logger.error(err)
#             self.uploadedFLG = 0

#     def cleanup(self):
#         logger.info('Removing tmp dir')
#         # remove tmp dir
#         shutil.rmtree(self.tmp_dir)
#         # update metadata info
#         today = datetime.datetime.utcnow().strftime('%F %TZ')

#         logger.info('Updating upload meta data')
#         self.df_uploaded.loc[self.file_idx] = [
#             self.season,
#             self.gamedate,
#             self.gameid,
#             self.team1,
#             self.team2,
#             self.uploadedFLG,
#             today,
#             self.base_url,
#             str(self.params)
#         ]
    
#         self.df_uploaded.to_parquet(
#             fname=self.uploaded_filepath,
#             compression='snappy',
#             index=False
#         )

#     def run(self):
#         self.metadata()
#         data = self.extract()
#         self.transform(data)
#         self.load()
#         self.cleanup()


# def upload_all_play_by_play(
#         df_gamelog_meta: pd.DataFrame,
#         start_date: str = None,
#         end_date: str = None,
# ):
#     if not start_date:
#         start_date = df_gamelog_meta.sort_values('game_date').game_date.iloc[0]
#     if not end_date:
#         end_date = df_gamelog_meta.sort_values('game_date').game_date.iloc[-1]
    
#     cols = ['season', 'game_date', 'game_id', 'team1', 'team2']
#     df = df_gamelog_meta[cols].drop_duplicates(subset=['game_id'])\
#                  .query('@start_date <= game_date <= @end_date')
    
#     start_time = datetime.datetime.now()
#     for i, row in enumerate(df.values):
#         logger.info('Running game {} of {}'.format(i + 1, df.shape[0]))
#         try:
#             etl = NBAStatsPlayByPlayS3ETL(
#                 season=row[0],
#                 gamedate=row[1],
#                 gameid=row[2],
#                 team1=row[3],
#                 team2=row[4],
#                 # destination_bucket=DESTINATION_BUCKET
#             )

#             etl.run()
#             etl_time = datetime.datetime.now()
#             time_delta = round((etl_time - start_time).seconds / 60., 2)
#             logger.info("It's been {} minutes".format(time_delta))

#             # pause every 10 games so we don't go over request limit
#             if i % 20 == 0:
#                 print('Sleep for a min to not overload api calls')
#                 time.sleep(60)
#         except Exception as err:
#             logger.error('Error with game {}, {}'.format(i, row[2]))
#             continue

#     end_time = datetime.datetime.now()
#     time_delta = round((end_time - start_time).seconds / 60., 2)

#     logger.info('It took {} minutes to load'.format(time_delta))
