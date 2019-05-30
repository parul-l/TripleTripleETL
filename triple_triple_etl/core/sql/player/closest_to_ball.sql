/*
    For a particular game, this query ranks the players closest to the ball
    for each moment. In this query, gameid is equated to '{0}' so that we can apply 
    python's `format` function to replace it with a given gameid.

    This query is used to determine who has possession of the ball, which 
    requires assuming a 'closest distance' and 'possession block length'. 
    See possession.sql for more details.

    As in the team_shooting_side.sql files, we load tables by game since athena
    limits partitions to 100. There is probably a better work around.
*/

WITH ball_info AS (
    SELECT 
        nba.gameposition.gameid
	  , nba.gameposition.eventid
      , nba.gameposition.moment_num
      , nba.gameposition.x_coordinate
      , nba.gameposition.y_coordinate
    FROM nba.gameposition
    WHERE gameid = '{0}' AND playerid = -1
)
, position_ball AS (
    SELECT 
        nba.gameposition.*
      , ball_info.x_coordinate          AS x_coordinate_ball
      , ball_info.y_coordinate          AS y_coordinate_ball
    FROM nba.gameposition
      LEFT JOIN ball_info
        ON nba.gameposition.gameid = ball_info.gameid
        AND nba.gameposition.eventid = ball_info.eventid
        AND nba.gameposition.moment_num = ball_info.moment_num
    WHERE nba.gameposition.gameid = '{0}'
) 
, ball_dist AS (
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
    FROM position_ball
)
, rank_ball_dist AS (
    SELECT 
        ball_dist.season
      , ball_dist.gameid
      , ball_dist.eventid
      , ball_dist.moment_num
      , ball_dist.teamid
      , ball_dist.playerid
      , ball_dist.distance_from_ball_sq
      , ROW_NUMBER() OVER (
          PARTITION BY eventid, moment_num 
          ORDER BY distance_from_ball_sq)  AS closest_to_ball_rank   
    FROM ball_dist
    WHERE playerid != -1 -- remove the ball since some moments don't have it
)
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
      , rank_ball_dist.closest_to_ball_rank
    FROM ball_dist
    LEFT JOIN rank_ball_dist
      ON ball_dist.eventid = rank_ball_dist.eventid
      AND ball_dist.moment_num = rank_ball_dist.moment_num
      AND ball_dist.playerid = rank_ball_dist.playerid
    