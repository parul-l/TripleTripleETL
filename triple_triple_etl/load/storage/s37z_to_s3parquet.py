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
from triple_triple_etl.constants import (
    META_DIR,
    DESTINATION_BUCKET,
    SOURCE_BUCKET
)


s3 = boto3.client('s3')
# filename without the extension
log_filename = '{}.log'.format(os.path.splitext(os.path.basename(__file__))[0])
logger = get_logger(output_file=log_filename)


class S3FileFormatETL(object):
    def __init__(
        self,
        input_filename: str, 
        source_bucket: str = SOURCE_BUCKET,
        destination_bucket: str = DESTINATION_BUCKET,
        season_year: str = '2015-2016'
    ):
        self.input_filename = input_filename
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.season_year = season_year
        self.tmp_dir = None
        self.gameid = None
        self.data_paths = None
        self.df_uploaded = None
        self.file_idx = None
        self.uploaded_filepath = os.path.join(META_DIR, 'tranformed_s3uploaded.parquet.snappy')


    def metadata(self):
        logger.info('Getting loaded files metadata')

        if os.path.isfile(self.uploaded_filepath):
            self.df_uploaded = pd.read_parquet(self.uploaded_filepath)
        else:
            columns = [
                'input_filename',
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
                .query('input_filename == @self.input_filename')\
                .index[0]
        except:
            self.file_idx = self.df_uploaded.shape[0]
            self.df_uploaded\
                .loc[self.file_idx] = [self.input_filename] + [np.nan] * 5       
        

    def extract_from_s3(self):
        filepath = s3download(
            bucket_name=self.source_bucket,
            filename=self.input_filename
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
                self.tmp_dir, '{}_{}.parquet.snappy'.format(name, self.gameid)
            )
            # execute function and save
            # TODO: update type of exception
            try:
                logger.info('Transforming {} data and saving as parquet'.format(name))
                function(data).to_parquet(
                    fname=self.data_paths[name],
                    compression='snappy'
                )
                
            except Exception as err:
                logger.error(err)
                # update metadata
                self.df_uploaded\
                    .loc[self.file_idx, '{}_uploadedFLG'.format(name)] = 0

                # continue to next element in for loop
                continue


    def load(self):
        # Uploading files to s3  
        for tablename, path in self.data_paths.items():
            try:
                logger.info('Uploading {} to s3'.format(tablename))
                output_file = os.path.splitext(os.path.basename(self.input_filename))[0]\
                                .replace('.', '')
                s3.upload_file(
                    Filename=path,
                    Bucket=self.destination_bucket,
                    Key='{}/season={}/gameid={}/{}.parquet'.format(
                        tablename,
                        self.season_year,
                        self.gameid,
                        output_file
                    )
                )
                self.df_uploaded\
                    .loc[self.file_idx, '{}_uploadedFLG'.format(tablename)] = 1
            except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
                logger.error(err)
                self.df_uploaded\
                    .loc[self.file_idx, '{}_uploadedFLG'.format(tablename)] = 0
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
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')    

    def run(self):
        self.metadata()
        data = self.extract_from_s3()
        self.transform(data)
        self.load()
        self.cleanup()


def transform_upload_all_games(
        season_year: str,
        all_files: list,
        idx: list = [],
        source_bucket: str = SOURCE_BUCKET,
        destination_bucket: str = DESTINATION_BUCKET
):

    if idx:
        all_files = np.array(all_files)[idx]

    start_time = datetime.datetime.now()
    for i, filename in enumerate(all_files):
        logger.info('Running etl file {} of {}'.format(i + 1, len(all_files)))
        try:
            etl = S3FileFormatETL(
                input_filename=filename, 
                source_bucket=source_bucket,
                destination_bucket=destination_bucket,
                season_year=season_year
            )
            etl.run()
            etl_time = datetime.datetime.now()
            time_delta = round((etl_time - start_time).seconds / 60., 2)
            logger.info("It's been {} minutes".format(time_delta))

        except Exception as err:
            logger.error('Error with file {}, {}'.format(i, filename))
            continue

    end_time = datetime.datetime.now()
    time_delta = round((end_time - start_time).seconds / 60., 2)

    logger.info('It took {} minutes to load'.format(time_delta))


