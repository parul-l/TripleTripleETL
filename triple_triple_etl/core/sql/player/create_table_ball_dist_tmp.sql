CREATE TABLE ball_dist_tmp WITH (
    external_location = 's3://nba-game-info/ball_dist_tmp',
    format = 'PARQUET',
    partitioned_by = ARRAY['season', 'gameid'],
    parquet_compression = 'SNAPPY'
) AS (
    SELECT 
          ball_dist.season
        , ball_dist.gameid
        , ball_dist.eventid
        , ball_dist.moment_num
        , ball_dist.timestamp_dts
        , ball_dist.timestamp_utc
        , ball_dist.period
        , ball_dist.periodclock
        , ball_dist.shotclock
        , ball_dist.teamid
        , ball_dist.playerid
        , ball_dist.x_coordinate
        , ball_dist.y_coordinate
        , ball_dist.z_coordinate
        , ball_dist.distance_from_ball_sq
    FROM (-- ball_dist --
            SELECT
            season
                , gameid
                , eventid
                , moment_num
                , timestamp_dts
                , timestamp_utc
                , period
                , periodclock
                , shotclock
                , teamid
                , playerid
                , x_coordinate
                , y_coordinate
                , z_coordinate
                , POWER(x_coordinate - x_coordinate_ball, 2) +
                  POWER(y_coordinate - y_coordinate_ball, 2)
                                                      AS distance_from_ball_sq
            FROM ( -- position_ball
                SELECT 
                    nba.gameposition.*
                    , ball_info.x_coordinate          AS x_coordinate_ball
                    , ball_info.y_coordinate          AS y_coordinate_ball
                FROM nba.gameposition
                LEFT JOIN (-- ball_info--
                        SELECT 
                            nba.gameposition.season
                            , nba.gameposition.gameid
                            , nba.gameposition.eventid
                            , nba.gameposition.moment_num
                            , nba.gameposition.x_coordinate
                            , nba.gameposition.y_coordinate
                        FROM nba.gameposition
                       -- WHERE gameid = '0021500001' AND playerid = -1
                        ) AS ball_info
                    ON  nba.gameposition.season = ball_info.season
                    AND nba.gameposition.gameid = ball_info.gameid
                    AND nba.gameposition.eventid = ball_info.eventid
                    AND nba.gameposition.moment_num = ball_info.moment_num   
               -- WHERE nba.gameposition.gameid = '0021500001'     
            ) AS position_ball
    ) AS ball_dist
    WHERE season = '{}' 
    AND gameid BEWTEEN {} AND {};