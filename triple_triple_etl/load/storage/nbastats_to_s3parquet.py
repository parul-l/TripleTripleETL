import datetime
import os
import pandas as pd
import tempfile
import shutil

from triple_triple_etl.constants import (
    BASE_URL_GAMELOG,
    DESTINATION_BUCKET
)
from triple_triple_etl.core.nbastats_get_data import get_data
from triple_triple_etl.log import get_logger
from triple_triple_etl.load.storage.load_helper import get_uploaded_metadata

THIS_FILENAME = os.path.splitext(os.path.basename(__file__))
LOG_FILENAME = '{}.log'.format(os.path.splitext(THIS_FILENAME)[0])

logger = get_logger(output_file=LOG_FILENAME)


def get_first_date_season(
        season: str = '2015-16',
        datefrom: str = '10/01/2015',
        dateto: str = '10/31/2016',
):
    logger.info('Get gamelogs from {} to {}'.format(datefom, dateto))
    params = {
        'DateFrom': datefrom,
        'DateTo': dateto,
        'Direction': 'ASC',
        'LeagueID': '00',
        'PlayerOrTeam': 'T',
        'Season': season,
        'SeasonType': 'Regular Season',
        'Sorter': 'DATE'
    }
    data = get_data(base_url=BASE_URL_GAMELOG, params=params)

    # index of GAME_DATE (note: direction is ASC)
    game_date_idx = data['resultSets'][0]['headers'].index('GAME_DATE')

    return data['resultSets'][0]['rowSet'][0][game_date_idx]


def add_metadata(
        df: pd.DataFrame,
        data_dict: dict
):
    for col, val in col_val.items():
        df[col] = val
    return df


def tmp_save_gamelog(df: pd.DataFrame):
    tmp_dir = tempfile.mkdtemp()
    filename = 'gamelog{}.parquet.snappy'.format(df.GAME_ID.loc[0])
    filepath = os.path.join(tempfile.mkdtemp(), filename)
    df.to_parquet(fname=filepath, compression='snappy')

    return tmp_dir


def get_game_info(df: pd.DataFrame):
    gamedate_ = df.GAME_DATE.loc[0].split('-')

    return {
        'gameid': df.GAME_ID.loc[0],
        'matchup': df.MATCHUP.loc[0].replace(' ', '').replace('.', ''),
        'gamedate': '{}{}{}'.format(gamedate_[1], gamedate_[2], gamedate_[0])
    }


def append_upload_metadata(
    df: pd.DataFrame,
    data: list,
    columns: list,
    uploadFLG: boolean
):
    new_data = data.extend([uploadFLG])
    df.append(data, columns=columns, ignore_index=True)

    return df


class NBAStatsGameLogsS3ETL(object):
    def __init__(
        self,
        datefrom: str = '01/01/2016',
        dateto: str = '01/01/2016',
        season_year: str = '2015-16',
        destination_bucket: str = DESTINATION_BUCKET
    ):

    self.datefrom = datefrom
    self.dateto = dateto
    self.season_year: str = season_year
    self.tmp_dirs = []

    # api info
    self.base_url = BASE_URL_GAMELOG
    self.params = {
        'DateFrom': self.datefrom,
        'DateTo': self.dateto,
        'Direction': 'ASC',
        'LeagueID': '00',
        'PlayerOrTeam': 'T',
        'Season': self.season,
        'SeasonType': 'Regular Season',
        'Sorter': 'DATE'
    }

    # meta data file
    meta_data_filename = os.path.join(META_DIR, THIS_FILENAME)
    self.uploaded_filepath = '{}.parquet.snappy'.format(meta_data_filename)
    
    def metadata(self):
        logger.info('Getting loaded files metadata')
        # meta data columns
        columns = [
            'base_url', 'params', 'season', 'gamedate',
            'gameid', 'lastuploadDTS','gamelog_uploadedFLG',
        ]
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )

    def extract_by_date_from_nbastats(self):
        logger.info('Get gamelogs from {} to {}'.format(self.datefom, self.dateto))
        return get_data(base_url=self.base_url, params=self.params)
        
    def load(self, data):
        loaddate = datetime.datetime.utcnow().strftime('%F %TZ')
        metadata_info = {
            'BASE_URL': self.base_url,
            'PARAMS': self.params,
            'UPDATE_DTS': loaddate
        }
        for games in data['resultSets']:
        # data['resultSets'] is a list of list. 
        # Usually data[resultSets] has 1 element
            for game in games['rowSet']:
                # create df
                df = add_metadata(
                    df=pd.DataFrame(data=[game], columns=games['headers']),
                    data_dict=metadata_info
                )
                # save to tmp dir and collect tmpdir path
                self.tmp_dirs.append(tmp_save_gamelog(df))

                # get info for metadata
                gameinfo = get_game_info(df)
                updatemetadata = [
                    self.base_url,
                    self.params,
                    self.season_year,
                    gameinfo['gamedate'],
                    gameinfo['gameid'],
                    loaddate
                ]
                try:
                    # load to s3
                    logger.info('Uploading gamelog {} to s3'.format(gameinfo['gameid']))
                    s3.upload_file(
                        Filename=filepath,
                        Bucket=self.destination_bucket,
                        Key='gamelog/season={}/gameid={}/{}{}.parquet.snappy'.format(
                            self.season_year,
                            gameinfo['gameid'],
                            gameinfo['gamedate'],
                            gameinfo['matchup']    
                        )
                    )
                    # update metadata
                    self.df_uploaded = append_upload_metadata(
                        df=self.df_uploaded,
                        data=updatemetadata,
                        columns=self.df_uploaded.columns,
                        uploadFLG=1
                    )

                except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
                    logger.error(err)
                    # update metadata
                    self.df_uploaded = append_upload_metadata(
                        df=self.df_uploaded,
                        data=updatemetadata,
                        columns=self.df_uploaded.columns,
                        uploadFLG=0
                    )
                    continue          
            
    def cleanup(self):
        # remove tmp directories
        for tmp in self.tmp_dirs:
            shutil.rmtree(tmp)
        
        # save df_uploaded
        self.df_uploaded.to_parquet(fname=self.uploaded_filepath, compression='snappy')

    def run(self):
        # etl start time
        start_time = datetime.datetime.utcnow()

        self.metadata()
        data = self.extract_by_date_from_nbastats()
        self.load(data)
        self.cleanup()

        # etl end time
        end_time = datetime.datetime.utcnow()
        time_delta = round((end_time - start_time).seconds / 60., 2)
        msg = 'It took {} minutes to get gamelogs from {} to {}'\
                .format(time_delta, self.datefrom, self.dateto)
        logger.info(msg)

        
        

            
                


