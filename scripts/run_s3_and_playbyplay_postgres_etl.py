from triple_triple_etl.core.s3 import get_game_files
from triple_triple_etl.load.postgres.s3_postgres_etl import S3PostgresETL
from triple_triple_etl.load.postgres.nbastats_postgres_etl import(
    NBAStatsPostgresETL
) 
from triple_triple_etl.load.postgres.s3_and_playbyplay_postgres_etl import (
    S3andPlaybyPlayPostgresETL
)
from triple_triple_etl.constants import BASE_URL_PLAY

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
        'data_content': 1
    }

    all_files = get_game_files(bucket_name)

    for filename in all_files[17:18]:
        s3_etl = S3PostgresETL(
            filename=filename,
            schema_file='position_data_tables.yaml'
        )
        playbyplay_etl = NBAStatsPostgresETL(**pbp_input)
        etl = S3andPlaybyPlayPostgresETL(
            s3_etl=s3_etl,
            playbyplay_etl=playbyplay_etl
        )
        etl.run()
