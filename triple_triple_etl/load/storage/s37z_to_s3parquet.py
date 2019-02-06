import boto3
import json
import os
import logging
import pandas as pd
import tempfile
import shutil

from triple_triple_etl.core.s3_json2csv import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)
from triple_triple_etl.core.s3 import s3download, extract2dir
from triple_triple_etl.log import get_logger

# from constants import LOGS_DIR


s3 = boto3.client('s3')
logger = get_logger()


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

        except FileNotFoundError as err:
            logger.error(err)
            raise

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
                raise


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
            except boto3.exceptions.botocore.client.ClientError as err:
                logger.error(err)
                raise

    def cleanup(self):
        # remove tmp directory
        logger.info('Removing temporary directory')
        shutil.rmtree(self.tmp_dir)
    
    def run(self):
        data = self.extract_from_s3()
        self.transform(data)
        self.load()
        self.cleanup()
