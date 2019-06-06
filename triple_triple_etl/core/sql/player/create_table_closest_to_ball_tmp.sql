CREATE TABLE IF NOT EXISTS closest_to_ball_tmp WITH (
    external_location = 's3://nba-game-info/closest_to_ball_tmp',
    format = 'PARQUET',
    partitioned_by = ARRAY['season', 'gameid'],
    parquet_compression = 'SNAPPY'
) AS (
    SELECT 
          ball_dist_tmp.eventid
        , ball_dist_tmp.moment_num
        , ball_dist_tmp.timestamp_dts
        , ball_dist_tmp.timestamp_utc
        , ball_dist_tmp.period
        , ball_dist_tmp.periodclock
        , ball_dist_tmp.shotclock
        , ball_dist_tmp.teamid
        , ball_dist_tmp.playerid
        , ball_dist_tmp.x_coordinate
        , ball_dist_tmp.y_coordinate
        , ball_dist_tmp.z_coordinate
        , ball_dist_tmp.distance_from_ball_sq
        , rank_ball_dist.closest_to_ball_rank
        , ball_dist_tmp.season
        , ball_dist_tmp.gameid
    FROM nba.ball_dist_tmp
    LEFT JOIN ( -- rank_ball_dist
        SELECT 
              ball_dist_tmp.season
            , ball_dist_tmp.gameid
            , ball_dist_tmp.eventid
            , ball_dist_tmp.moment_num
            , ball_dist_tmp.teamid
            , ball_dist_tmp.playerid
            , ball_dist_tmp.distance_from_ball_sq
            , ROW_NUMBER() OVER (
                PARTITION BY season, gameid, eventid, moment_num 
                ORDER BY distance_from_ball_sq)       AS closest_to_ball_rank   
        FROM nba.ball_dist_tmp
        WHERE playerid != -1 
        ) AS rank_ball_dist
    ON  ball_dist_tmp.season = rank_ball_dist.season
    AND ball_dist_tmp.gameid = rank_ball_dist.gameid
    AND ball_dist_tmp.eventid = rank_ball_dist.eventid
    AND ball_dist_tmp.moment_num = rank_ball_dist.moment_num
    AND ball_dist_tmp.playerid = rank_ball_dist.playerid
);