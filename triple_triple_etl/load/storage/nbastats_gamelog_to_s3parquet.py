import boto3
import datetime
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import tempfile
import shutil
import warnings

from triple_triple_etl.constants import (
    BASE_URL_GAMELOG,
    DESTINATION_BUCKET,
    META_DIR
)
from triple_triple_etl.core.nbastats_get_data import get_data
from triple_triple_etl.log import get_logger
from triple_triple_etl.load.storage.load_helper import (
    get_uploaded_metadata,
    format_year
)

warnings.simplefilter(action='ignore', category=FutureWarning)

s3 = boto3.client('s3')

THIS_FILENAME = os.path.splitext(os.path.basename(__file__))[0]
LOG_FILENAME = '{}.log'.format(os.path.splitext(THIS_FILENAME)[0])
logger = get_logger(output_file=LOG_FILENAME)


def get_first_last_date_season(
        season: str = '2015-16',
        datefrom: str = '10/01/2015',
        dateto: str = '10/31/2016',
        first: bool = 1
):
    logger.info('Get gamelogs from {} to {}'.format(datefrom, dateto))
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
    # get appropriate list index for either first or last date
    if first:
        row_idx = 0
    else:
        row_idx = -1
    date = data['resultSets'][0]['rowSet'][row_idx][game_date_idx].split('-')

    return '{}/{}/{}'.format(date[1], date[2], date[0])


def append_upload_metadata(
        df_data: pd.DataFrame,
        df_upload: pd.DataFrame,
        team1: str,
        team2: str,
        lastuploadDTS: str,
        base_url: str,
        params: str,
        uploadFLG: bool
):
    update_cols = ['season', 'game_date', 'game_id']
    df_add = df_data[update_cols]
    df_add.loc[:, 'team1'] = team1
    df_add.loc[:, 'team2'] = team2
    df_add.loc[:, 'gamelog_uploadedFLG'] = uploadFLG
    df_add.loc[:, 'lastuploadDTS'] = lastuploadDTS
    df_add.loc[:, 'base_url'] = base_url
    df_add.loc[:, 'params'] = str(params)

    return pd.concat([df_upload, df_add], axis=0, ignore_index=True)


class NBAStatsGameLogsS3ETL(object):
    def __init__(
        self,
        datefrom: str = '01/01/2016',
        dateto: str = '01/01/2016',
        season_year: str = '2015-2016',
        destination_bucket: str = DESTINATION_BUCKET
    ):

        self.datefrom = datefrom
        self.dateto = dateto
        self.season_year = season_year
        self.destination_bucket = destination_bucket
        self.tmp_dir = tempfile.mkdtemp()
        self.loaddate = datetime.datetime.utcnow().strftime('%F %TZ')

        # api info
        self.base_url = BASE_URL_GAMELOG
        self.params = {
            'DateFrom': self.datefrom,
            'DateTo': self.dateto,
            'Direction': 'ASC',
            'LeagueID': '00',
            'PlayerOrTeam': 'T',
            'Season': self.season_year,
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
            'season', 'game_date', 'game_id', 'team1', 'team2',
            'gamelog_uploadedFLG', 'lastuploadDTS',
            'base_url', 'params'
        ]
        self.df_uploaded = get_uploaded_metadata(
            self.uploaded_filepath,
            columns=columns
        )

    def extract(self):
        logger.info('Get gamelogs from {} to {}'.format(self.datefrom, self.dateto))
        return get_data(base_url=self.base_url, params=self.params)

    def transform(self, data):
        all_dfs = [
            pd.DataFrame(
                data=games['rowSet'],
                columns=[x.lower() for x in games['headers']]
            ) 
            for games in data['resultSets']
        ]
        # join all dfs (usually just one), add season
        df = pd.concat(all_dfs, ignore_index=True)
        df.loc[:, 'season'] = self.season_year

        pq.write_to_dataset(
            table=pa.Table.from_pandas(df, preserve_index=False),
            root_path=self.tmp_dir,
            partition_cols=['season', 'game_id'],
            compression='snappy',
            preserve_index=False
        )    
            
        return df        

    def load(self, df):

        try:
            table_dir = os.path.join(self.tmp_dir, 'season={}'.format(self.season_year))
            for game in os.listdir(table_dir):
                gameid = game.split('=')[1]
                df_game = df.query('game_id == @gameid')
                matchup = re.findall(r'[\w]+', df_game.matchup.iloc[0])
                gamedate = ''.join(df_game.game_date.iloc[0].split('-'))

                game_path = os.path.join(table_dir, game)
                filepath = os.path.join(game_path, os.listdir(game_path)[0])
           
                # load to s3
                logger.info('Uploading gamelog {} to s3'.format(gameid))
                s3.upload_file(
                    Filename=filepath,
                    Bucket=self.destination_bucket,
                    Key='gamelog/season={}/gameid={}/{}{}{}.parquet.snappy'.format(
                        format_year(self.season_year),
                        gameid,
                        gamedate,
                        matchup[0],
                        matchup[-1]
                    )
                )
                # update metadata
                self.df_uploaded = append_upload_metadata(
                    df_data=df_game,
                    df_upload=self.df_uploaded,
                    lastuploadDTS=self.loaddate,
                    team1=matchup[0],
                    team2=matchup[-1],
                    base_url=self.base_url,
                    params=self.params,
                    uploadFLG=1
                )

        except (boto3.exceptions.botocore.client.ClientError, FileNotFoundError) as err:
            logger.error(err)
            # update metadata
            self.df_uploaded = append_upload_metadata(
                df_data=df,
                df_upload=self.df_uploaded,
                lastuploadDTS=self.loaddate,
                team1=matchup[0],
                team2=matchup[-1],
                base_url=self.base_url,
                params=self.params,
                uploadFLG=0
            )

    def cleanup(self):
        # remove tmp directories
        shutil.rmtree(self.tmp_dir)
        
        # save df_uploaded
        self.df_uploaded.to_parquet(
            fname=self.uploaded_filepath,
            compression='snappy',
            index=False
        )

    def run(self):
        # etl start time
        start_time = datetime.datetime.utcnow()

        self.metadata()
        data = self.extract()
        df = self.transform(data)
        self.load(df)
        self.cleanup()

        # etl end time
        end_time = datetime.datetime.utcnow()
        time_delta = round((end_time - start_time).seconds / 60., 2)
        msg = 'It took {} minutes to get gamelogs from {} to {}'\
                .format(time_delta, self.datefrom, self.dateto)
        logger.info(msg)

        
