import boto3
import datetime
import json
import logging
import numpy as np
import os
import pandas as pd
import tempfile
import shutil

from triple_triple_etl.core.s3_json2df import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)
from triple_triple_etl.core.s3 import s3download, extract2dir
from triple_triple_etl.log import get_logger
from triple_triple_etl.constants import META_DIR



s3 = boto3.client('s3')
logger = get_logger(output_file=__file__)


class S3FileFormatETL(object):
    def __init__(
        self,
        filename: str, 
        bucket_base: str = 'nba-player-positions',
        season_year: str = '2015-2016'
    ):
        self.filename = filename
        self.bucket_base = bucket_base
        self.season_year = season_year
        self.tmp_dir = None
        self.gameid = None
        self.data_paths = None
        self.df_uploaded = None
        self.file_idx = None
        self.uploaded_filepath = os.path.join(META_DIR, 'tranformed_s3uploaded.parquet')


    def metadata(self):
        logger.info('Getting loaded files metadata')

        if os.path.isfile(self.uploaded_filepath):
            self.df_uploaded = pd.read_parquet(self.uploaded_filepath)
        else:
            columns = [
                'filename',
                'gameinfo_uploadedFLG',
                'gameposition_uploadedFLG',
                'playersinfo_uploadedFLG',
                'teamsinfo_uploadedFLG',
                'lastuploadDTS'
            ]
            self.df_uploaded = pd.DataFrame(columns=columns)
        
        # get index if file exists or create a row with that filename
        try:
            self.file_idx = self.df_uploaded\
                .query('filename == @self.filename')\
                .index[0]
        except:
            self.file_idx = self.df_uploaded.shape[0]
            self.df_uploaded.loc[self.file_idx] = [self.filename] + [np.nan] * 5       
        

    def extract_from_s3(self):
        filepath = s3download(
            bucket_name=self.bucket_base,
            filename=self.filename
        )

        # make a new dir so that the only file
        # is the extracted one
        logger.info('Creating temporary directory')
        self.tmp_dir = tempfile.mkdtemp()

        logger.info('Extracting file')
        extract2dir(filepath, directory=self.tmp_dir)

        # open file
        try:
            logger.info('Getting data')
            filename = os.listdir(self.tmp_dir)[0]
            with open(os.path.join(self.tmp_dir, filename)) as f:
                data = json.load(f)
                self.gameid = data['gameid']
            

        # except FileNotFoundError as err:
        except Exception as err:
            logger.error(err)
            data = {}

        return data
        

    def transform(self, data: dict):
        transform_functions = {
            'gameinfo': get_game_info,
            'gameposition': get_game_position_info,
            'playersinfo': get_player_info,
            'teamsinfo': get_team_info
        }
        self.data_paths = {}
        for name, function in transform_functions.items():
            self.data_paths[name] = os.path.join(
                self.tmp_dir, '{}_{}'.format(name, self.gameid)
            )
            # execute function and save
            # TODO: update type of exception
            try:
                logger.info('Transforming {} data and saving as parquet'.format(name))
                function(data).to_parquet(self.data_paths[name])
                
            except Exception as err:
                logger.error(err)
                # update metadata
                self.df_uploaded.loc[self.file_idx, '{}_uploadedFLG'.format(name)] = 0

                # continue to next element in for loop
                continue


    def load(self):
        # Uploading files to s3  
        for name, path in self.data_paths.items():
            try:
                logger.info('Uploading {} to s3'.format(name))
                s3.upload_file(
                    Filename=path,
                    Bucket=self.bucket_base,
                    Key='{}/{}/{}_{}'.format(self.season_year, name, name, self.gameid)
                )
                # 
                self.df_uploaded.loc[self.file_idx, '{}_uploadedFLG'.format(name)] = 1
            except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
                logger.error(err)
                self.df_uploaded.loc[self.file_idx, '{}_uploadedFLG'.format(name)] = 0
                # continue to next element in for loop
                continue

    def cleanup(self):
        # remove tmp directory
        logger.info('Removing temporary directory')
        shutil.rmtree(self.tmp_dir)
        # update the uploadDTS stamp
        today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.df_uploaded.loc[self.file_idx, 'lastuploadDTS'] = today
        # save df_uploaded
        self.df_uploaded.to_parquet(self.uploaded_filepath)    

    def run(self):
        self.metadata()
        data = self.extract_from_s3()
        self.transform(data)
        self.load()
        self.cleanup()



def transform_upload_all_games(
        bucket_name: str,
        all_files: list,
        idx: list = []
):

    if idx:
        all_files = np.array(all_files)[idx]

    start_time = datetime.datetime.now()
    for i, filename in enumerate(all_files):
        logger.info('transforming/loading file {} of {}'.format(i, len(all_files)))
        try:
            etl = S3FileFormatETL(
                filename=filename,
                bucket_base=bucket_name,
                season_year='2015-2016'
            )
            etl.run()
        except Exception as err:
            logger.error('Error with file {}, {}'.format(i, filename))
            continue

    end_time = datetime.datetime.now()
    time_delta = round((end_time - start_time).seconds / 60., 2)
    print('It took {} minutes to load'.format(time_delta))


