import time
import numpy as np

from triple_triple_etl.core.s3 import get_bucket_content
from triple_triple_etl.load.storage.player_actions import (
    PlayerActionsETL
)


if __name__ == '__main__':
    season = '2015-2016'
    bucket_name = 'nba-game-info'
    prefix_game = 'playbyplay/season={}/'.format(season)
    prefix_player_actions = 'player_actions/season={}/'.format(season)

    playbyplay_gameids = [
        x['Prefix'].split('gameid=')[1][:-1]
        for x in get_bucket_content(bucket_name, prefix_game)
    ]

    
    for i in range(850, len(playbyplay_gameids), 20):
        block = sorted(playbyplay_gameids)[i: i + 10]
        etl = PlayerActionsETL(
            season=season,
            gameid_bounds=[block[0], block[-1]]
        )
        etl.run()

        time.sleep(300)
