import numpy as np

from triple_triple_etl.core.s3 import get_bucket_content
from triple_triple_etl.load.storage.closest_to_ball_rank_athena_to_s3parquet import (
    ClosestToBallETL,
    run_all_closest_to_ball
)


if __name__ == '__main__':
    bucket_name = 'nba-game-info'
    prefix = 'gameposition/season=2015-2016/'

    gameposition_gameids = [
        x['Prefix'].split('gameid=')[1][:-1]
        for x in get_bucket_content(bucket_name, prefix)
    ]


    idx_list = list(np.arange(3, 300, 1))
    run_all_closest_to_ball(
        all_games=gameposition_gameids,
        idx=idx_list
    )