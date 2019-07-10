# (1) nba-event-codes.csv is parsed from nba-event-codes.xml(received from NBA rep in 2016/2017)
# (2) nba play-by-play data is parsed from nba.stats.com
# the event_code in (1) does not match with the eventmsgtype in (2) directly - seems off by 2 integers
# the subevent_code in (1) seems to match with eventactionmsgtype in (2)
# (1) has 28 different types of events, (2) only have 12 or 13


import boto3
import datetime
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import shutil
import tempfile


from triple_triple_etl.constants import ATHENA_OUTPUT, META_DIR, SQL_DIR 
from triple_triple_etl.core.athena import (
    execute_athena_query,
    check_table_exists
)
from triple_triple_etl.core.s3 import (
    get_bucket_content,
    copy_bucket_contents,
    remove_bucket_contents
)
from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata
from triple_triple_etl.log import get_logger

s3 = boto3.resource('s3', region_name='us-east-1')
s3client = boto3.client('s3', region_name='us-east-1')
athena = boto3.client('athena', region_name='us-east-1')

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


def update_metadata(
        df_uploaded: pd.DataFrame,
        file: str,
        uploadFLG: bool,
        params: dict,
):

    # maybe use regular expression for this
    season = [file for file in file.split('/') if 'season' in file][0].split('=')[1]
    gameid = [file for file in file.split('/') if 'gameid' in file][0].split('=')[1]
    today = datetime.datetime.utcnow().strftime('%F %TZ')

    # gameid idx
    file_idx = get_file_idx_in_uploaded(
        df_uploaded=df_uploaded,
        gameid=gameid
    )

    # update and return df_uploaded
    df_uploaded.loc[file_idx] = [season, gameid, params, uploadFLG, today]
    return df_uploaded


class PlayerActionsETL(object):
    def __init__(
            self, 
            season: str,
            gameid_bounds: list, # [lower_bound, upper_bound]
            source_bucket: str = ATHENA_OUTPUT,
            destination_bucket: str = 'nba-game-info',
            database: str = 'nba'
    ):
        self.season = season
        self.gameid_bounds = gameid_bounds
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.database = database
        self.query_create_player_action_table = None
        self.s3keys = {}
        self.df_uploaded = None
        
        metadata_filename = 'player_actions.parquet.snappy'
        self.uploaded_filepath = os.path.join(META_DIR, metadata_filename)

    
    def get_query(
            self,
            sql_path='player/create_table_player_actions_tmp.sql',
    ):
        logger.info('Get queries for given gameids.')

        player_action_path = os.path.join(SQL_DIR, sql_path)

        with open(player_action_path) as f:
            self.query_create_player_action_table = f.read().format(
                self.season,
                self.gameid_bounds[0],
                self.gameid_bounds[1]
            )

    def create_tmp_table(self, table: str = 'player_actions_tmp'):
        logger.info('Creating {} table'.format(table))
        output_filename = '{}_gameids_{}_to_{}'.format(
            table,
            self.gameid_bounds[0],
            self.gameid_bounds[1]
        )

        try:
            self.s3keys['create_{}'.format(table)] = execute_athena_query(
                query=self.query_create_player_action_table,
                database=self.database,
                output_filename=output_filename,
                boto3_client=athena
            )
            # wait for table to appear max 5 min
            table_exists = check_table_exists(
                database_name=self.database,
                table_name=table,
                max_time=300
            )
        
        except Exception as err:
            logger.error('Error creating {}'.format(table))

        if not table_exists:
            raise FileNotFoundError('Table {} did not load'.format(table))

    def move_tmp_to_final(self):
        all_files = get_bucket_content(
            bucket_name=self.destination_bucket,
            prefix='player_actions_tmp',
            delimiter=''
        )    
        # move player_actions_tmp to player_actions
        logger.info('Move data gameids {} to {} from tmp to final'.format(
            self.gameid_bounds[0],
            self.gameid_bounds[1]
        ))

        copy_bucket_contents(
            copy_source_keys=all_files,
            destination_bucket=self.destination_bucket,
            destination_folder='player_actions',
            s3client=s3client
        )

        return all_files


    def alter_table(self, all_files: list):
        all_gameids = [re.search('gameid=(.+?)/', key).group(1) for key in all_files]
        
        for gameid in set(all_gameids):
            location = 's3://{}/player_actions/season={}/gameid={}'.format(
                self.destination_bucket,
                self.season,
                gameid
            )
            query_path = os.path.join(SQL_DIR, 'alter_table.sql')
            
            with open(query_path) as f:
                query = f.read().format(
                    'nba.player_actions',
                    self.season,
                    gameid,
                    location
                )
                try:
                    logger.info(
                        'Adding gameid {} to player_actions table'.format(gameid))
                    self.s3keys['alter_table_{}'.format(gameid)] = execute_athena_query(
                        query=query,
                        database=self.database,
                        output_filename='alter_table_{}'.format(gameid),
                        boto3_client=athena
                    )

                except Exception as err:
                    logger.error('Error adding row to table {}'.format(gameid))
                    logger.error(err)

    def drop_tmp_table(self, table: str = 'player_actions_tmp'):
        query = "DROP TABLE IF EXISTS nba.{}".format(table)
        # execute drop table query
        self.s3keys['drop_{}'.format(table)] = execute_athena_query(
            query=query,
            database=self.database,
            output_filename='drop_table_{}'.format(table),
            boto3_client=athena
        )

    def cleanup(self):
        # meta data of df columns
        columns = ['season', 'gameid', 'params', 'uploadedFLG', 'lastuploadDTS']
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        ) 
        # tmp files
        keys_player_actions_tmp = get_bucket_content(
            bucket_name=self.destination_bucket,
            prefix='player_actions_tmp',
            delimiter=''
        )

        # delete tmp keys and update metadata
        for file in keys_player_actions_tmp:
            is_key_there = remove_bucket_contents(
                bucket=self.destination_bucket,
                key=file,
                max_time=0,
                s3client=s3client
            )
        
            self.df_uploaded = update_metadata(
                df_uploaded=self.df_uploaded,
                file=file,
                uploadFLG=is_key_there,
                params=str(self.s3keys)
            )
    
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')
  
    def run(self):
        self.get_query()
        self.create_tmp_table()
        all_files = self.move_tmp_to_final()
        self.alter_table(all_files)
        self.drop_tmp_table()
        self.cleanup()
