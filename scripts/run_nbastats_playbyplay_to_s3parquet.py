import os
import pandas as pd

from triple_triple_etl.constants import META_DIR
from triple_triple_etl.load.storage.nbastats_playbyplay_to_s3parquet import (
    upload_all_play_by_play,
    NBAStatsPlayByPlayS3ETL
)


if __name__ == '__main__':
    filepath = os.path.join(META_DIR, 'nbastats_gamelog_to_s3parquet.parquet.snappy')
    df_gamelog_meta = pd.read_parquet(filepath)
    

    upload_all_play_by_play(
        df_gamelog_meta=df_gamelog_meta,
        start_date='2016-01-01',
        end_date='2016-04-14'
    ) 