import contextlib
import json
import os
import shutil
import tempfile

import psycopg2

from triple_triple_etl.constants import (
    config,
    DATASETS_DIR,
    DATATABLES_DIR
)
from triple_triple_etl.core.json2csv import (
    get_all_tables_dict,
    save_all_tables
)
from triple_triple_etl.core.s3 import s3download, extract2dir


@contextlib.contextmanager
def get_connection():
    connection = psycopg2.connect(**config['postgres'])
    connection.set_client_encoding('utf-8')
    connection.autocommit = True

    try:
        yield connection
    finally:
        connection.close()


@contextlib.contextmanager
def get_cursor():
    with get_connection() as connection:
        cursor = connection.cursor()

        try:
            yield cursor
        finally:
            cursor.close()


def create_rawdata_tables():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE games (
          id VARCHAR(20),
          start_time TIMESTAMP,
          home_team_id INTEGER,
          visitor_team_id INTEGER,
          PRIMARY KEY (id)
        );

        CREATE TABLE players (
          id INTEGER,
          last_name VARCHAR(20),
          first_name VARCHAR(20),
          team_id INTEGER,
          jersey_number INTEGER,
          position VARCHAR(5),
          start_date DATE,
          end_date DATE,
          PRIMARY KEY (id, start_date)
        );

        CREATE TABLE teams (
          id INTEGER,
          name VARCHAR,
          conference VARCHAR(4),
          division VARCHAR(15),
          city VARCHAR(20),
          state CHAR(2),
          zipcode INTEGER,
          start_date DATE,
          end_date DATE,
          PRIMARY KEY (id, start_date)
        );

        CREATE TABLE game_positions (
          game_id VARCHAR(20),
          event_id INTEGER,
          time_stamp TIMESTAMP,
          period INTEGER,
          period_clock FLOAT,
          shot_clock FLOAT,
          team_id INTEGER,
          player_id INTEGER,
          x_coordinate FLOAT,
          y_coordinate FLOAT,
          z_coordinate FLOAT,
          PRIMARY KEY (time_stamp),
          FOREIGN KEY (game_id) REFERENCES games (id)
        );

        END TRANSACTION;
    """
    with get_cursor() as cursor:
        cursor.execute(query)


def csv2postgres(filepath):
    with get_cursor() as cursor:
        with open(filepath) as f:
            f.readline()
            tablename = os.path.basename(filepath).replace('.csv', '')
            cursor.copy_from(f, tablename, sep=',', null='')


class PostgresETL(object):
    def __init__(
            self,
            filename,
            bucket_base='nba-player-positions',
            raw_data_dir=DATASETS_DIR, storage_dir=DATATABLES_DIR
    ):
        self.filename = filename
        self.bucket_base = bucket_base
        self.raw_data_dir = raw_data_dir
        self.storage_dir = storage_dir

    def extract_from_s3(self):
        filepath = s3download(
            bucket_name=self.bucket_base,
            filename=self.filename
        )

        tmp_dir = tempfile.mkdtemp()
        self.tmp_dir = tmp_dir

        extract2dir(filepath, directory=tmp_dir)

        return tmp_dir

    def tables_from_json(self):
        filepath = os.path.join(
            self.tmp_dir,
            os.listdir(self.tmp_dir)[0]
        )
        with open(filepath) as f:
            all_tables_dict = get_all_tables_dict(json.load(f))
            save_all_tables(all_tables_dict, storage_dir=self.storage_dir)

            shutil.rmtree(self.tmp_dir)

    def load(self, filepath):
        with get_cursor() as cursor:
            with open(filepath) as f:
                f.readline()
                tablename = os.path.basename(filepath).replace('.csv', '')
                cursor.copy_from(f, tablename, sep=',', null='')

    def run(self):
        _ = self.extract_from_s3()
        self.tables_from_json()

        all_csvs = ['games.csv', 'players.csv',
                    'teams.csv', 'game_positions.csv']

        for f in all_csvs:
            filepath = os.path.join(self.storage_dir, f)
            self.load(filepath)
            os.remove(filepath)
