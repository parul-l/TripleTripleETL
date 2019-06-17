import numpy as np

from triple_triple_etl.core.s3 import get_bucket_content
from triple_triple_etl.load.storage.closest_to_ball_rank_athena_to_s3parquet import (
    ClosestToBallETL
)


if __name__ == '__main__':
    season = '2015-2016'
    bucket_name = 'nba-game-info'
    prefix_game = 'gameposition/season={}/'.format(season)
    prefix_closest = 'closest_to_ball/season={}/'.format(season)

    gameposition_gameids = [
        x['Prefix'].split('gameid=')[1][:-1]
        for x in get_bucket_content(bucket_name, prefix_game)
    ]

    for i in range(570, len(gameposition_gameids), 10):
        block = sorted(gameposition_gameids)[i: i + 10]
        etl = ClosestToBallETL(
            season=season,
            gameid_bounds=[block[0], block[-1]]
        )
        etl.run()


