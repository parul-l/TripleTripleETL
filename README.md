## An ETL for NBA player data

* Data originally stored as a .7z file on s3
    - extracted and transformed to create the following files:
        - gameinfo
        - gamelog
        - gameposition
    - stored as parquet files on s3
* Data from [NBA Stats](http://stats.nba.com)
    - extracted and transformed to create the following files:
        - playerinfo
        - teaminfo
        - playbyplay
        - boxscore_player
        - boxscore_traditional
    - stored as parquet files on s3