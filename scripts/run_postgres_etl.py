from s3 import get_game_files
from postgres_etl import PostgresETL


if __name__ == '__main__':
    bucket_name = 'nba-player-positions'

    all_files = get_game_files(bucket_name)

    for filename in all_files[4:5]:
        etl = PostgresETL(filename=filename)
        etl.run()
