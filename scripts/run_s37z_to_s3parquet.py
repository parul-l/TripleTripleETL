# from triple_triple_etl.core.s3 import get_game_files
import os
import pandas as pd
from triple_triple_etl.constants import META_DIR
from triple_triple_etl.load.storage.s37z_to_s3parquet import (
    S3FileFormatETL,
    transform_upload_all_games
)


if __name__ == '__main__':

    # get list of all games
    bucket_name = 'nba-player-positions'
    # all_files = get_game_files(bucket_name)
    df_all_files = pd.read_parquet(
        path=os.path.join(META_DIR, 'position_files.parquet'),
        columns=['filename']
    )

    import numpy as np
    af = np.array(df_all_files.filename)[[97]]
    etl = S3FileFormatETL(filename=af[0])
    etl.metadata()
    etl.extract_from_s3()



    transform_upload_all_games(
        bucket_name=bucket_name,
        all_files = df_all_files.filename,
        idx=list(np.arange(96, 98))
    )
    


    # for filename in df_all_files.filename[97:98]:
    #     etl = S3FileFormatETL(
    #         filename=filename,
    #         bucket_base=bucket_name,
    #         season_year='2015-2016'
    #     )
    #     etl.run()
    
    
    
