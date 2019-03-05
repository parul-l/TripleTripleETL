from triple_triple_etl.load.storage.nbastats_to_s3parquet import (
    get_first_date_season,
    NBAStatsGameLogsS3ETL
)

if __name__ == '__main__':
    # first_gamedate = get_first_date_season(
    #     season='2015-16',
    #     datefrom='10/01/2015',
    #     dateto='10/31/2016',        
    # )
    datefrom = first_gamedate
    dateto = first_gamedate
    
    etl = NBAStatsGameLogsS3ETL(
        datefrom=test_date,
        dateto=test_date,
        season_year='2015-16',
    )