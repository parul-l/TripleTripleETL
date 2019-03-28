from triple_triple_etl.load.storage.nbastats_gamelog_to_s3parquet import (
    get_first_last_date_season,
    NBAStatsGameLogsS3ETL
)

if __name__ == '__main__':
    first_gamedate = get_first_last_date_season(
        season='2015-16',
        datefrom='10/01/2015',
        dateto='10/31/2015',
        first=1
    )
    last_gamedate = get_first_last_date_season(
        season='2015-16',
        datefrom='04/01/2016',
        dateto='04/30/2016',
        first=0
    )
    
    etl = NBAStatsGameLogsS3ETL(
        datefrom=first_gamedate,
        dateto=last_gamedate,
        season_year='2015-16',
    )

    etl.run()