from triple_triple_etl.core.s3 import get_game_files
from triple_triple_etl.load.postgres.s3_postgres_etl import S3PostgresETL


if __name__ == '__main__':
    bucket_name = 'nba-player-positions'

    all_files = get_game_files(bucket_name)

    for filename in all_files[17:18]:
        etl = S3PostgresETL(filename=filename, schema_file='position_data_tables.yaml')
        etl.run()
