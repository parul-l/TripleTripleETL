import boto3
import datetime
import json
import logging
import numpy as np
import os
import pandas as pd
import pyarrow.parquet as pq
import tempfile
import shutil

from triple_triple_etl.core.s3_json2df import (
    get_player_info,
    get_team_info,
    get_game_info,
    get_game_position_info
)
from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata
from triple_triple_etl.core.s3 import s3download, extract2dir
from triple_triple_etl.log import get_logger
from triple_triple_etl.constants import (
    META_DIR,
    DESTINATION_BUCKET,
    SOURCE_BUCKET
)


s3 = boto3.client('s3')
# current filename without the extension
LOG_FILENAME = '{}.log'.format(os.path.splitext(os.path.basename(__file__))[0])
logger = get_logger(output_file=LOG_FILENAME)


def get_file_idx_in_uploaded(
        df_uploaded: pd.DataFrame,
        input_filename: str
):
    # get index if file exists or create a row with that filename
    try:
        return df_uploaded\
            .query('input_filename == @input_filename')\
            .index[0]
    except:
        return df_uploaded.shape[0]
  

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
        self.data_paths = {}
        self.df_uploaded = None
        self.file_idx = None
        
        meta_data_filename = 'rawpositiontransformed_s3uploaded.parquet.snappy'
        self.uploaded_filepath = os.path.join(META_DIR, meta_data_filename)


    def metadata(self):
        logger.info('Getting loaded files metadata')
        # meta data df columns
        columns = [
            'input_filename',
            'gameinfo_uploadedFLG',
            'gameposition_uploadedFLG',
            'playerinfo_uploadedFLG',
            'teaminfo_uploadedFLG',
            'lastuploadDTS'
        ]
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )
        self.file_idx = get_file_idx_in_uploaded(
            df_uploaded=self.df_uploaded,
            input_filename=self.input_filename
        )
        # add row to df_uploaded if file DNE (ie file_idx == len(df_uploaded))
        if self.file_idx == self.df_uploaded.shape[0]:
            self.df_uploaded.loc[self.file_idx] = [self.input_filename] + [np.nan] * 5

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
        extract2dir(filepath=filepath, directory=self.tmp_dir)

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

    def _transform(self, data, tablename, transform_function):
        # this generic transform function is used to transform data in to 
        # four tables
        try:    
            logger.info('Transforming {} data and saving as parquet'.format(tablename))
            table_dir = os.path.join(
                self.tmp_dir,
                tablename,
                'season={}'.format(self.season_year),
                'gameid={}'.format(self.gameid)
            )
            pq.write_to_dataset(
                table=transform_function(data),
                root_path=os.path.join(self.tmp_dir, tablename),
                partition_cols=['season', 'gameid'],
                compression='snappy',
                preserve_index=False
            )

            # collect table filepath
            self.data_paths[tablename] = os.path.join(table_dir, os.listdir(table_dir)[0])
        
        except Exception as err:
            logger.error(err)
            # update metadata
            self.df_uploaded\
                .loc[self.file_idx, '{}_uploadedFLG'.format(tablename)] = 0

    def _load(self, tablename: str):
        # this generic load function is used to load data all
        # four tables in to s3
        path = self.data_paths[tablename]
        try:
            logger.info('Uploading {} to s3'.format(tablename))
            output_file = os.path\
                            .splitext(os.path.basename(self.input_filename))[0]\
                            .replace('.', '')
            s3.upload_file(
                Filename=path,
                Bucket=self.destination_bucket,
                Key='{}/season={}/gameid={}/{}.parquet.snappy'.format(
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


    def transform_game(self, data: dict):
        self._transform(
            data=data, 
            tablename='gameinfo', 
            transform_function=get_game_info
        )

    def transform_player(self, data: dict):
        self._transform(
            data=data, 
            tablename='playerinfo', 
            transform_function=get_player_info
        )

    def transform_team(self, data: dict):
        self._transform(
            data=data, 
            tablename='teaminfo', 
            transform_function=get_team_info
        )

    def transform_position(self, data: dict):
        self._transform(
            data=data, 
            tablename='gameposition', 
            transform_function=get_game_position_info
        )


    def load_game(self):
        self._load(tablename='gameinfo')

    def load_player(self):
        self._load(tablename='playerinfo')

    def load_team(self):
        self._load(tablename='teaminfo')

    def load_position(self):
        self._load(tablename='gameposition')



    # def transform(self, data: dict):
    #     transform_functions = {
    #         'gameinfo': get_game_info,
    #         'gameposition': get_game_position_info,
    #         'playerinfo': get_player_info,
    #         'teaminfo': get_team_info
    #     }
    #     self.data_paths = {}
    #     for name, function in transform_functions.items():  
    #         # execute function and save
    #         # TODO: update type of exception
    #         try:
    #             logger.info('Transforming {} data and saving as parquet'.format(name))

    #             table_dir = os.path.join(
    #                 self.tmp_dir,
    #                 name,
    #                 'season={}'.format(self.season_year),
    #                 'gameid={}'.format(self.gameid)
    #             )
    #             pq.write_to_dataset(
    #                 table=function(data),
    #                 root_path=os.path.join(self.tmp_dir, name),
    #                 partition_cols=['season', 'gameid'],
    #                 compression='snappy',
    #                 preserve_index=False
    #             )

    #             # collect table filepath
    #             self.data_paths[name] = os.path.join(table_dir, os.listdir(table_dir)[0])
                
    #         except Exception as err:
    #             logger.error(err)
    #             # update metadata
    #             self.df_uploaded\
    #                 .loc[self.file_idx, '{}_uploadedFLG'.format(name)] = 0

    #             # continue to next element in for loop
    #             continue

    # def load(self):
    #     # Uploading files to s3  
    #     for tablename, path in self.data_paths.items():
    #         try:
    #             logger.info('Uploading {} to s3'.format(tablename))
    #             output_file = os.path.splitext(os.path.basename(self.input_filename))[0]\
    #                             .replace('.', '')
    #             s3.upload_file(
    #                 Filename=path,
    #                 Bucket=self.destination_bucket,
    #                 Key='{}/season={}/gameid={}/{}.parquet.snappy'.format(
    #                     tablename,
    #                     self.season_year,
    #                     self.gameid,
    #                     output_file
    #                 )
    #             )
    #             self.df_uploaded\
    #                 .loc[self.file_idx, '{}_uploadedFLG'.format(tablename)] = 1
    #         except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
    #             logger.error(err)
    #             self.df_uploaded\
    #                 .loc[self.file_idx, '{}_uploadedFLG'.format(tablename)] = 0
    #             # continue to next element in for loop
    #             continue

    def cleanup(self):
        # remove tmp directory
        logger.info('Removing temporary directory')
        shutil.rmtree(self.tmp_dir)
        # update the uploadDTS stamp
        today = datetime.datetime.utcnow().strftime('%F %TZ')
        self.df_uploaded.loc[self.file_idx, 'lastuploadDTS'] = today
        # save df_uploaded
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')

    def run(self):
        self.metadata()

        data = self.extract_from_s3()

        # self.transform_game(data)
        # self.transform_player(data)
        # self.transform_team(data)
        self.transform_position(data)

        # self.load_game()
        # self.load_player()
        # self.load_team()
        self.load_position()

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


