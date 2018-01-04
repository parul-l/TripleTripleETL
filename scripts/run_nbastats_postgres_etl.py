from triple_triple_etl.load.postgres.nbastats_postgres_etl import NBAStatsPostgresETL


if __name__ == '__main__':
    game_id = '0021500568'
    season = '2015-16'

    base_url_play = 'http://stats.nba.com/stats/playbyplayv2'
    params_play = {
        'EndPeriod': '10',      # default by NBA stats (acceptablevalues: 1, 2, 3, 4)
        'EndRange': '55800',    # not sure what this is
        'GameID': game_id,
        'RangeType': '2',       # not sure what this is
        'Season': season,
        'SeasonType': 'Regular Season',
        'StartPeriod': '1',     # acceptable values: 1, 2, 3, 4
        'StartRange': '0',      # not sure what this is
    }

    base_url_box_score_traditional = 'http://stats.nba.com/stats/boxscoretraditionalv2'

    params_box_score = {
        'EndPeriod': '10',
        'EndRange': '55800',
        'GameID': game_id,
        'RangeType': '2',
        'Season': '2015-16',
        'SeasonType': 'Regular Season',
        'StartPeriod': '1',
        'StartRange': '0'
    }

    pbp_input = {
        'base_url': base_url_play,
        'params': params_play,
        'data_content': 1
    }

    bs_input = {
        'base_url': base_url_box_score_traditional,
        'params': params_box_score,
        'data_content': 0
    }

    etl_pbp = NBAStatsPostgresETL(**pbp_input)
    etl_pbp.run()

    etl_bs = NBAStatsPostgresETL(**bs_input)
    etl_bs.run()
