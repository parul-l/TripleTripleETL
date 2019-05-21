from triple_triple_etl.load.storage.teamshooting_athena_to_s3parquet import (
    TeamShootingSideALL,
    TeamShootingSideETL
)


if __main__ == '__name__':
    # all games on gamelog
    etl = TeamShootingSideALL()
    etl.run()

    # per game
    gameid = '0021500001'
    etl = TeamShootingSideETL(gameid=gameid)
    etl.run()
