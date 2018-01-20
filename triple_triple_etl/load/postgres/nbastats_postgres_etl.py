import os

from triple_triple_etl.constants import DATATABLES_DIR
from triple_triple_etl.core.nbastats_get_data import get_data
from triple_triple_etl.core.nbastats_json2csv import (
    get_df_box_score,
    get_df_play_by_play
)
from triple_triple_etl.load.postgres.postgres_helper import (
    csv2postgres_pkeys,
    save_all_tables
)
from triple_triple_etl.load.postgres.postgres_connection import get_cursor



def create_nbastats_tables():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE play_by_play (
          game_id VARCHAR(20),
          event_id INTEGER,
          event_msg_type INTEGER,
          event_msg_action_type INTEGER,
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
          player3_team_id INTEGER,
          PRIMARY KEY (
              game_id
              , event_id
              , event_msg_type
              , event_msg_action_type
          )
        );

        CREATE TABLE box_score_traditional (
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
          plus_minus FLOAT,
          PRIMARY KEY (game_id, player_id)         
        );

        CREATE TABLE box_score_player_tracking (
          game_id VARCHAR(20),
          team_id INTEGER,
          team_abbreviation VARCHAR(3),
          team_city VARCHAR(20),
          player_id INTEGER,
          player_name VARCHAR(50),
          start_position VARCHAR(5),
          comment VARCHAR(100),
          minutes INTEGER,
          avg_speed_mph FLOAT,
          distance_mph FLOAT,
          offensive_rebounds_chances INTEGER,
          defensive_rebounds_chances INTEGER,
          rebound_chances INTEGER,
          touches INTEGER,
          secondary_assists INTEGER,
          free_throw_assists FLOAT,
          passes INTEGER,
          assists INTEGER,
          contested_field_goals_made INTEGER,
          contested_field_goals_attempted INTEGER,
          contested_field_goals_percent FLOAT,
          uncontested_field_goals_made INTEGER,
          uncontested_field_goals_attempted INTEGER,
          uncontested_field_goals_percent FLOAT,
          field_goal_percent FLOAT,
          field_goals_defended_at_rim_made INTEGER,
          field_goals_defended_at_rim_attempted INTEGER,
          field_goals_defended_at_rim_percent FLOAT,
          PRIMARY KEY (game_id, player_id)
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
            data_content (int):
                0 - `play_by_play`
                1 - `box_score_traditional`
                2 - `box_score_player_tracking`
            storage_dir (str): The path for storing the transformed data
    """
    
    def __init__(
        self,
        base_url,
        params,
        data_content,
        schema_file,
        storage_dir=DATATABLES_DIR
    ):
        self.base_url = base_url
        self.params = params
        self.data_content = data_content
        self.schema_file = schema_file
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
        if self.data_content != 0:
            raise ValueError('self.data is not for play_by_play')

        return {'play_by_play': get_df_play_by_play(self.data)}
        
    def transform_box_score_traditional(self):
        if self.data_content != 1:
            raise ValueError('self.data is not for box_score_traditional')

        return {'box_score_traditional': get_df_box_score(self.data, 0)}

    def transform_box_score_player_tracking(self):
        if self.data_content != 2:
            raise ValueError('self.data is not for box_score_player_tracking')

        return {'box_score_player_tracking': get_df_box_score(self.data, 1)}

    def transform(self):
        if self.data_content == 0:
            tables_dict = self.transform_play_by_play()
        elif self.data_content == 1:
            tables_dict = self.transform_box_score_traditional()
        elif self.data_content == 2:
            tables_dict = self.transform_box_score_player_tracking()
        save_all_tables(
            tables_dict=tables_dict,
            storage_dir=self.storage_dir
        )

    def load(self, filepath):
        tablename = os.path.basename(filepath).replace('.csv', '')
        csv2posgres_params = {
            'tablename': tablename,
            'filepath': filepath,
            'schema_file': self.schema_file
        }
        csv2postgres_pkeys(**csv2posgres_params)

    def run(self):
        _ = self.extract_from_nbastats()
        self.transform()
        
        if self.data_content == 0:
            filename = 'play_by_play.csv'
        elif self.data_content == 1:
            filename = 'box_score_traditional.csv'
        elif self.data_content == 2:
            filename = 'box_score_player_tracking.csv'
        
        filepath = os.path.join(self.storage_dir, filename)
        
        self.load(filepath)
        os.remove(filepath)
