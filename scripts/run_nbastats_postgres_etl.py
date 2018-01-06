from triple_triple_etl.load.postgres.nbastats_postgres_etl import NBAStatsPostgresETL
from triple_triple_etl.constants import (
    BASE_URL_PLAY,
    BASE_URL_BOX_SCORE_TRADITIONAL
)

if __name__ == '__main__':
    game_id = '0021500568'
    season = '2015-16'
    
    params = {
        'EndPeriod': '10',      # default by NBA stats (acceptablevalues: 1, 2, 3, 4)
        'EndRange': '55800',    # not sure what this is
        'GameID': game_id,
        'RangeType': '2',       # not sure what this is
        'Season': season,
        'SeasonType': 'Regular Season',
        'StartPeriod': '1',     # acceptable values: 1, 2, 3, 4
        'StartRange': '0',      # not sure what this is
    }

    pbp_input = {
        'base_url': BASE_URL_PLAY,
        'params': params,
        'data_content': 1
    }

    bs_input = {
        'base_url': BASE_URL_BOX_SCORE_TRADITIONAL,
        'params': params,
        'data_content': 0
    }

    etl_pbp = NBAStatsPostgresETL(**pbp_input)
    etl_pbp.run()

    etl_bs = NBAStatsPostgresETL(**bs_input)
    etl_bs.run()
