import os
import pandas as pd

from triple_triple_etl.constants import META_DIR
from triple_triple_etl.load.storage.nbastats_to_s3parquet import upload_all_nbastats
# , NBAStatsS3ETL


if __name__ == '__main__':
    filepath = os.path.join(META_DIR, 'nbastats_gamelog_to_s3parquet.parquet.snappy')
    df_gamelog_meta = pd.read_parquet(filepath)

    # game_id = '0021500001'
    # season = '2015-16'
    # game_date = '2015-10-27'   
    # team1 = 'DET'
    # team2 = 'ATL'

    # etl = NBAStatsS3ETL(
    #     gameid=game_id,
    #     gamedate=game_date,
    #     team1=team1,
    #     team2=team2,
    #     game_data_type='boxscore_player',
    #     season=season                
    # )



    upload_all_nbastats(
        df_gamelog_meta=df_gamelog_meta,
        game_data_type='boxscore_player',
        start_date='2015-10-27',
        end_date='2016-04-14'
    ) 