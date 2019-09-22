import time
import numpy as np

from triple_triple_etl.core.s3 import s3client, get_bucket_content, remove_bucket_contents
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

    bad_gameids = []

    for gameid in playbyplay_gameids[970:]:
        # get player_actions content:
        try:
            keys_player_actions = get_bucket_content(
                bucket_name='nba-game-info',
                prefix='player_actions/season=2015-2016/gameid={}'.format(gameid),
                delimiter=''
            )
            for file in keys_player_actions:
                is_key_there = remove_bucket_contents(
                    bucket='nba-game-info',
                    key=file,
                    max_time=0,
                    s3client=s3client
                )
        except:
            bad_gameids.append(gameid)
        
        etl = PlayerActionsETL(
            season=season,
            gameid_bounds=[gameid, gameid]
        )
        etl.run()


# bad gameids
# ['0021500016',
#  '0021500101',
#  '0021500102',
#  '0021500108',
#  '0021500177',
#  '0021500240',
#  '0021500264',
#  '0021500327',
#  '0021500394',
#  '0021500398',
#  '0021500456',
#  '0021500489',
#  '0021500631',
#  '0021500761']
