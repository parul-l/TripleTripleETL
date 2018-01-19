from triple_triple_etl.load.postgres.postgres_connection import get_cursor


def create_game_possession_table():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE game_possession (
          game_id VARCHAR(20),
          event_id INTEGER,
          time_stamp TIMESTAMP,
          player_id INTEGER,
          minimum_distance_sq FLOAT,
          closest_to_ball BOOLEAN,
          PRIMARY KEY (game_id, event_id, time_stamp, player_id),
          FOREIGN KEY (
            game_id
            , event_id
            , time_stamp
            , player_id
          )
            REFERENCES game_positions (
                game_id
            , event_id
            , time_stamp
            , player_id
          )
        );
        
        END TRANSACTION;
    """
    with get_cursor() as cursor:
        cursor.execute(query)
    
    
class PossessionPostgresETL(object):
    def __init__(
        self,
        game_id
    ):
        self.game_id = game_id    
    
    def load(self):      
        query = """
        BEGIN TRANSACTION;

        CREATE TABLE game_possession_staging(LIKE game_possession);

        INSERT INTO game_possession_staging (
            WITH filtered_game_possession AS (
                SELECT
                game_id
                , event_id
                , time_stamp
                , period
                , period_clock 
                , player_id 
                , team_id 
                , x_coordinate
                , y_coordinate
                FROM game_positions
                WHERE game_id = '{game_id}'
            )
            , ball_positions AS (
                SELECT
                game_id
                , event_id
                , time_stamp
                , period
                , period_clock 
                , player_id 
                , team_id 
                , x_coordinate
                , y_coordinate
                FROM filtered_game_possession
                WHERE player_id = -1
            )
            , distance_table AS (
                SELECT
                gp.game_id
                , gp.event_id
                , gp.time_stamp
                , gp.player_id
                , (gp.x_coordinate - bp.x_coordinate)^2 + 
                  (gp.y_coordinate - bp.y_coordinate)^2
                AS distance_to_ball_sq
                FROM filtered_game_possession AS gp
                JOIN ball_positions AS bp
                USING (game_id, event_id, time_stamp)
            )
            , minimum_table AS (
                SELECT 
                dt.game_id AS game_id
                , dt.event_id AS event_id
                , dt.time_stamp AS time_stamp
                , MIN(dt.distance_to_ball_sq) AS minimum_distance_sq
                FROM distance_table AS dt
                WHERE dt.player_id != -1  
                GROUP BY game_id, event_id, time_stamp
            )
          SELECT
            dt.game_id
            , dt.event_id
            , dt.time_stamp
            , dt.player_id
            , dt.distance_to_ball_sq
            , mt.minimum_distance_sq = dt.distance_to_ball_sq AS closest_to_ball
          FROM distance_table as dt
            INNER JOIN minimum_table AS mt
            USING (game_id, event_id, time_stamp)
        );         
        
        END TRANSACTION;
        """.format(game_id=self.game_id)
        with get_cursor() as cursor:
            cursor.execute(query)

    def run(self):
        self.load()
        query = """
        BEGIN TRANSACTION;
        
        INSERT INTO game_possession_staging
          SELECT DISTINCT *
          FROM game_possession;
        
        DROP TABLE game_possession;
        
        ALTER TABLE game_possession_staging
          RENAME TO game_possession;

        END TRANSACTION;
        """
        with get_cursor() as cursor:
            cursor.execute(query)
