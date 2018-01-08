import os

from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.core.nbastats_get_data import get_data
from triple_triple_etl.core.nbastats_json2csv import (
    get_df_box_score,
    get_df_play_by_play
)
from triple_triple_etl.load.postgres.postgres_helper import (
    csv2postgres_no_pkeys,
    save_all_tables
)
from triple_triple_etl.load.postgres.postgres_connection import get_cursor



def create_nbastats_tables():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE play_by_play (
          game_id VARCHAR(20),
          event_id INTEGER,
          period INTEGER,
          wctimestring TIMESTAMP,
          pctimestring FLOAT,
          home_description VARCHAR(100),
          neutral_description VARCHAR(100),
          visitor_description VARCHAR(100),
          score_visitor FLOAT,
          score_home FLOAT,
          player1_type INTEGER,
          player1_id INTEGER,
          player1_name VARCHAR(50),
          player1_team_id INTEGER,
          player2_type INTEGER,
          player2_id INTEGER,
          player2_name VARCHAR(50),
          player2_team_id INTEGER,
          player3_type INTEGER,
          player3_id INTEGER,
          player3_name VARCHAR(50),
          player3_team_id INTEGER
        );

        CREATE TABLE box_score (
          game_id VARCHAR(20),
          team_id INTEGER,
          team_abbreviation VARCHAR(3),
          team_city VARCHAR(20),
          player_id INTEGER,
          player_name VARCHAR(50),
          start_position VARCHAR(5),
          comment VARCHAR(100),
          minutes INTEGER,
          field_goal_made_2 INTEGER,
          field_goal_attempts_2 INTEGER,
          field_goal_percent_2 FLOAT,
          field_goal_made_3 INTEGER,
          field_goal_attempts_3 INTEGER,
          field_goal_percent_3 FLOAT,
          free_throw_made INTEGER,
          free_throw_attempts INTEGER,
          free_throw_percent FLOAT,
          offensive_rebound INTEGER,
          defensive_rebound INTEGER,
          total_rebounds INTEGER,
          assists INTEGER,
          steals INTEGER,
          blocks INTEGER,
          turnovers INTEGER,
          personal_fouls INTEGER,
          points INTEGER,
          plus_minus FLOAT            
        );
        END TRANSACTION;
    """
    with get_cursor() as cursor:
        cursor.execute(query)


class NBAStatsPostgresETL(object):
    """
        An etl that extracts, transforms, and loads data from nbastats.com
        to a postgres database.
        
        Args:
            base_url (str): The base url for the `requests` method
            params (dict): The paramters for the 'requests' method
            data_content (bool): Refers to `play_by_play` (1) or `box_score` (0)
            storage_dir (str): The path for storing the transformed data
    """
    
    def __init__(
        self,
        base_url,
        params,
        data_content,
        storage_dir=DATATABLES_DIR
    ):
        self.base_url = base_url
        self.params = params
        self.data_content=data_content
        self.storage_dir = storage_dir
        self.data = None

    def extract_from_nbastats(self):
        nba_data =  get_data(
            base_url=self.base_url,
            params=self.params
        )
        self.data = nba_data

        return nba_data

    def transform_play_by_play(self):
        if not self.data_content:
            raise ValueError('self.data is for box_score')

        return {'play_by_play': get_df_play_by_play(self.data)}
        
    def transform_box_score(self):
        if self.data_content:
            raise ValueError('self.data is for play_by_play')

        return {'box_score': get_df_box_score(self.data)}

    def transform(self):
        if self.data_content:
            tables_dict = self.transform_play_by_play()
        else:
            tables_dict = self.transform_box_score()
        
        save_all_tables(
            tables_dict=tables_dict,
            storage_dir=self.storage_dir
        )

    def load(self, filepath):
        tablename = os.path.basename(filepath).replace('.csv', '')
        csv2postgres_no_pkeys(filepath=filepath)

    def run(self):
        _ = self.extract_from_nbastats()
        self.transform()
        
        if self.data_content:
            filename = 'play_by_play.csv'
        else:
            filename = 'box_score.csv'
        filepath = os.path.join(self.storage_dir, filename)
        
        self.load(filepath)
        os.remove(filepath)
