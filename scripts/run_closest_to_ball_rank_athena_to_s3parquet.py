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

    # closest_to_ball_gameids = [
    #     x['Prefix'].split('gameid=')[1][:-1]
    #     for x in get_bucket_content(bucket_name, prefix_closest)
    # ]

    # games_to_load = sorted(list(set(gameposition_gameids) - set(closest_to_ball_gameids)))

    for i in range(0, len(gameposition_gameids), 50):
        block = gameposition_gameids[i: i + 50]

        etl = ClosestToBallETL(
            season=season,
            gameid_bounds=[block[0], block[-1]]
        )
        etl.run()


    # etl = ClosestToBallETL(
    #     season=season,
    #     gameid_bounds=['0021500338', '0021500338']
    # )
    # '0021500523'


    # idx_list = list(np.arange(3, 300, 1))
    # run_all_closest_to_ball(
    #     all_games=gameposition_gameids,
    #     idx=idx_list
    # )


