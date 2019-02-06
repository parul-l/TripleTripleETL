from triple_triple_etl.core.s3 import get_game_files
from triple_triple_etl.load.storage.s37z_to_s3parquet import S3FileFormatETL


if __name__ == '__main__':

    # get list of all games
    bucket_name = 'nba-player-positions'
    all_files = get_game_files(bucket_name)

    for filename in all_files[2:3]:
        etl = S3FileFormatETL(
            filename=filename,
            bucket_base=bucket_name,
            season_year='2015-2016'
        )
        etl.run()
