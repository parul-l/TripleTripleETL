import json
import os
import shutil
import tempfile

from triple_triple_etl.constants import (
    DATASETS_DIR,
    DATATABLES_DIR
)
from triple_triple_etl.core.s3_json2csv import get_all_s3_tables
from triple_triple_etl.core.s3 import s3download, extract2dir
from triple_triple_etl.load.postgres.postgres_connection import get_cursor
from triple_triple_etl.load.postgres.postgres_helper import  (
    csv2postgres_pkeys,
    csv2postgres_no_pkeys,
    save_all_tables
)


def create_s3_rawdata_tables():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE games (
            id VARCHAR(20)
          , start_time TIMESTAMP
          , home_team_id INTEGER
          , visitor_team_id INTEGER
          , PRIMARY KEY (id)
        );

        CREATE TABLE players (
            id INTEGER
          , last_name VARCHAR(20)
          , first_name VARCHAR(20)
          , team_id INTEGER
          , jersey_number INTEGER
          , position VARCHAR(5)
          , start_date DATE
          , end_date DATE
          , PRIMARY KEY (id, start_date)
        );

        CREATE TABLE teams (
            id INTEGER
          , name VARCHAR
          , conference VARCHAR(4)
          , division VARCHAR(15)
          , city VARCHAR(20)
          , state CHAR(2)
          , zipcode INTEGER
          , start_date DATE
          , end_date DATE
          , PRIMARY KEY (id, start_date)
        );

        CREATE TABLE game_positions (
            game_id VARCHAR(20)
          , event_id INTEGER
          , time_stamp TIMESTAMP
          , period INTEGER
          , period_clock FLOAT
          , shot_clock FLOAT
          , team_id INTEGER
          , player_id INTEGER
          , x_coordinate FLOAT
          , y_coordinate FLOAT
          , z_coordinate FLOAT
          , FOREIGN KEY (game_id) REFERENCES games (id)
        );

        END TRANSACTION;
    """
    with get_cursor() as cursor:
        cursor.execute(query)


def get_season(filename):
    years = filename.split('/')[0].split('-')
    return '-'.join([years[0], years[1][-2:]])


class S3PostgresETL(object):
    def __init__(
            self,
            filename,
            schema_file,
            game_id=None,
            bucket_base='nba-player-positions',
            raw_data_dir=DATASETS_DIR,
            storage_dir=DATATABLES_DIR
    ):
        self.filename = filename
        self.game_id = game_id
        self.schema_file = schema_file
        self.bucket_base = bucket_base
        self.raw_data_dir = raw_data_dir
        self.storage_dir = storage_dir
        self.season = get_season(filename=self.filename)
        self.tmp_dir = None

    def extract_from_s3(self):
        filepath = s3download(
            bucket_name=self.bucket_base,
            filename=self.filename
        )

        tmp_dir = tempfile.mkdtemp()
        self.tmp_dir = tmp_dir

        extract2dir(filepath, directory=tmp_dir)

        return tmp_dir

    def transform(self):        
        filepath = os.path.join(
            self.tmp_dir,
            os.listdir(self.tmp_dir)[0]
        )
        with open(filepath) as f:
            game_data_dict = json.load(f)

            # update game_id
            self.game_id = game_data_dict['gameid']

            # get game dataframes
            all_tables_dict = get_all_s3_tables(game_data_dict)
            save_all_tables(all_tables_dict, storage_dir=self.storage_dir)

            shutil.rmtree(self.tmp_dir)

    def load(self, filepath):
        tablename = os.path.basename(filepath).replace('.csv', '')
        csv2posgres_params = {
            'tablename': tablename,
            'filepath': filepath,
            'schema_file': self.schema_file
        }
        if tablename == 'game_positions':
            csv2postgres_no_pkeys(filepath=filepath)
        else:
            csv2postgres_pkeys(**csv2posgres_params)


    def run(self):
        _ = self.extract_from_s3()
        self.transform()

        all_csvs = [
            'games.csv',
            'players.csv',
            'teams.csv',
            'game_positions.csv'
        ]

        for f in all_csvs:
            filepath = os.path.join(self.storage_dir, f)
            self.load(filepath)
            os.remove(filepath)
