/*
    This query uses all games currently stored in the `gamelog` table to 
    get the shooting side of each team per quarter.
*/

WITH first_bucket_rank AS (
    SELECT 
        gameid
        , period
        , score
        , player1_team_id					AS teamid
        , CAST(SUBSTRING(pctimestring, 1, STRPOS(pctimestring, ':') - 1) AS TINYINT) * 60 +
        CAST(SUBSTRING(pctimestring, STRPOS(pctimestring, ':') + 1, LENGTH(pctimestring)) AS TINYINT) 
                                            AS periodclock
        , pctimestring
        , ROW_NUMBER() OVER (PARTITION BY gameid, period ORDER BY wctimestring) 
                                            AS bucket_rank
    FROM nba.playbyplaY
    WHERE period = 1
    AND score IS NOT NULL
)
, first_bucket AS (
    SELECT 
        gameid
        , period
        , teamid
        , periodclock
    FROM first_bucket_rank
    WHERE bucket_rank = 1
)
, first_shot_shooting_side_rank AS (
    SELECT
          gameposition.season
        , gameposition.gameid
        , first_bucket.teamid
        , ROW_NUMBER() OVER (
            PARTITION BY season, first_bucket.gameid, first_bucket.teamid ORDER BY ABS(gameposition.periodclock - first_bucket.periodclock)
        ) AS clock_diff_rank
        , CASE 
            WHEN gameposition.x_coordinate < 47 THEN 'left'
            WHEN gameposition.x_coordinate > 47 THEN 'right'
        ELSE NULL END                       AS shooting_side
    FROM nba.gameposition
    INNER JOIN first_bucket 
    ON gameposition.gameid = first_bucket.gameid 
    AND gameposition.period = first_bucket.period
    AND playerid = -1
    AND first_bucket.period = 1
)
, first_shot_shooting_side AS (
    SELECT    
          season
        , gameid
        , teamid
        , shooting_side
    FROM first_shot_shooting_side_rank
    WHERE clock_diff_rank = 1
)
, team_period AS (
    SELECT DISTINCT
          playbyplay.gameid
        , playbyplay.period
        , gamelog.team_id                   AS teamid
    FROM nba.playbyplay
    INNER JOIN nba.gamelog
    ON nba.playbyplay.gameid = nba.gamelog.gameid
   -- WHERE gamelog.gameid IN ('0021500001', '0021500002')
)
    SELECT
      gamelog.game_date
    , team_period.teamid
    , gamelog.team_abbreviation
    , team_period.period
    , CASE
        WHEN team_period.teamid = first_shot_shooting_side.teamid 
            AND team_period.period IN (1, 2) THEN first_shot_shooting_side.shooting_side -- first half for first team
        WHEN team_period.teamid != first_shot_shooting_side.teamid 
            AND team_period.period IN (3, 4)
        THEN first_shot_shooting_side.shooting_side -- second half for second team
        -- second half for first team
        WHEN team_period.teamid = first_shot_shooting_side.teamid
            AND first_shot_shooting_side.shooting_side = 'left'
            AND team_period.period IN (3, 4) THEN 'right'
        WHEN team_period.teamid = first_shot_shooting_side.teamid
            AND first_shot_shooting_side.shooting_side = 'right'
            AND team_period.period IN (3, 4) THEN 'left'
        -- first half for second team
        WHEN team_period.teamid != first_shot_shooting_side.teamid
            AND first_shot_shooting_side.shooting_side = 'left'
            AND team_period.period IN (1, 2) THEN 'right'
        WHEN team_period.teamid != first_shot_shooting_side.teamid
            AND first_shot_shooting_side.shooting_side = 'right'
            AND team_period.period IN (1, 2) THEN 'left'
        ELSE NULL 
        END                       AS shooting_side
    , CURRENT_TIMESTAMP           AS lastupdate_DTS
    , gamelog.season
    , gamelog.gameid
    FROM team_period
    LEFT JOIN first_shot_shooting_side
        ON  team_period.gameid = first_shot_shooting_side.gameid
    --  AND team_period.teamid = first_shot_shooting_side.teamid (only join on gameid so that shooting side/teamid not null when no match)
    LEFT JOIN nba.gamelog
        ON  gamelog.gameid = team_period.gameid
        AND gamelog.team_id = team_period.teamid
    ORDER BY gameid, period
