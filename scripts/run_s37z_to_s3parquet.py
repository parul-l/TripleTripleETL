import os
import pandas as pd
from triple_triple_etl.constants import META_DIR
from triple_triple_etl.load.storage.s37z_to_s3parquet import transform_upload_all_games


if __name__ == '__main__':

    # get list of all games
    df_all_files = pd.read_parquet(
        path=os.path.join(META_DIR, 'position_files.parquet'),
        columns=['filename']
    )

    transform_upload_all_games(
        season_year='2015-2016',
        all_files=df_all_files.filename[:2]
    )