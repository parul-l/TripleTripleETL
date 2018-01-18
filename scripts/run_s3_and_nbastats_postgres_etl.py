from triple_triple_etl.core.s3 import get_game_files
from triple_triple_etl.load.postgres.s3_postgres_etl import S3PostgresETL
from triple_triple_etl.load.postgres.nbastats_postgres_etl import(
    NBAStatsPostgresETL
) 
from triple_triple_etl.load.postgres.s3_and_nbastats_postgres_etl import (
    S3andNBAStatsPostgresETL
)
from triple_triple_etl.constants import (
    BASE_URL_PLAY,
    BASE_URL_BOX_SCORE_TRADITIONAL,
    BASE_URL_BOX_SCORE_PLAYER_TRACKING
)

if __name__ == '__main__':
    bucket_name = 'nba-player-positions'
    params = {
        'EndPeriod': '10',
        'EndRange': '55800',    
        'GameID': None,
        'RangeType': '2',       
        'Season': None,
        'SeasonType': 'Regular Season',
        'StartPeriod': '1',     
        'StartRange': '0',      
    }

    pbp_input = {
        'base_url': BASE_URL_PLAY,
        'params': params,
        'data_content': 0
    }

    bs_traditional_input = {
        'base_url': BASE_URL_BOX_SCORE_TRADITIONAL,
        'params': params,
        'data_content': 1
    }

    bs_player_tracking_input = {
        'base_url': BASE_URL_BOX_SCORE_PLAYER_TRACKING,
        'params': params,
        'data_content': 2
    }

    all_files = get_game_files(bucket_name)

    for filename in all_files[0:1]:
        s3_etl = S3PostgresETL(
            filename=filename,
            schema_file='position_data_tables.yaml'
        )
        playbyplay_etl = NBAStatsPostgresETL(**pbp_input)
        boxscore_traditional_etl = NBAStatsPostgresETL(**bs_traditional_input)
        boxscore_player_tracking_etl = NBAStatsPostgresETL(**bs_player_tracking_input)
        
        etl = S3andNBAStatsPostgresETL(
            s3_etl=s3_etl,
            playbyplay_etl=playbyplay_etl,
            boxscore_traditional_etl=boxscore_traditional_etl,
            boxscore_player_tracking_etl=boxscore_player_tracking_etl
        )
        etl.run()
