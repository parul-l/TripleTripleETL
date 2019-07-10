CREATE TABLE IF NOT EXISTS nba.player_actions_tmp WITH (
    external_location = 's3://nba-game-info/player_actions_tmp',
    format = 'PARQUET',
    partitioned_by = ARRAY['season', 'gameid'],
    parquet_compression = 'SNAPPY'
) AS (
   -- SHOTS MADE -- playbyplay.eventmsgtype = 1
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action
        , nba_event_codes.event_subaction
        , CASE 
            WHEN homedescription LIKE '% 3PT %' OR visitordescription LIKE '% 3PT %' THEN '3pt made'
            ELSE '2pt made' 
          END                                               AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription 
          END                                               AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
        END                                                 AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
        AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 1
------------------------------------------------------------------------------------------
UNION ALL
-- SHOTS MISSED -- playbyplay.eventmsgtype = 2
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action
        , nba_event_codes.event_subaction
        , CASE 
            WHEN homedescription LIKE '% 3PT %' OR visitordescription LIKE '% 3PT %' THEN '3pt missed'
            ELSE '2pt missed' 
          END                                               AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
          ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
          AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 2
------------------------------------------------------------------------------------------
UNION ALL
-- ASSISTS -- (player2) playbyplay.eventmsgtype = 1
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , 25                                                AS eventmsgtype -- using nba-event-code's event_code                 
        , NULL                                              AS eventmsgactiontype                             
        , 'assist'                                          AS event_action
        , NULL                                              AS event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription 
          END                                               AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player2_id                             AS player_id
        , playbyplay.player2_name                           AS player_name
        , playbyplay.player2_team_id                        AS player_team_id
        , playbyplay.player2_team_city                      AS player_team_city
        , playbyplay.player2_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 1
        AND (homedescription LIKE '% AST)' OR visitordescription LIKE '% AST)')
------------------------------------------------------------------------------------------
UNION ALL
-- FREE THROWS -- playbyplay.eventmsgtype = 3
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , CASE 
            WHEN playbyplay.homedescription LIKE 'MISS %'
                OR playbyplay.visitordescription LIKE 'MISS %' THEN 'free throw missed' 
            ELSE 'free throw made'
          END                                               AS event_action
        , nba_event_codes.event_subaction
        , CASE
            WHEN nba_event_codes.event_subaction LIKE '% flagrant%' THEN 'flagrant'
            WHEN nba_event_codes.event_subaction LIKE '% technical%' THEN 'technical'
            WHEN nba_event_codes.event_subaction LIKE '% clear path%' THEN 'clear path'                                         
            ELSE NULL                                         
          END                                               AS event_subsubaction
        , CASE WHEN homedescription IS NOT NULL THEN homedescription 
          ELSE visitordescription END                       AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
            ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
            AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 3
        AND nba_event_codes.event_code = 1 -- free throws correspond to event_code = 1, and 2. To avoid duplicates, just choose 1
------------------------------------------------------------------------------------------
UNION ALL
-- REBOUNDS -- playbyplay.eventmsgtype = 4
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , 'rebound'                                         AS event_action -- can't decipher offenive vs defensive easily
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription 
          END                                               AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
            END                                             AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
            --AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode (event_subcode = 0 is missing in nba_event_codes)
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 4
        AND nba_event_codes.event_code = 5 -- rebounds correspond to event_code = 5, and 6. To avoid duplicates, just choose 5
------------------------------------------------------------------------------------------
UNION ALL
-- STEALS -- (player2) playbyplay.eventmsgtype = 5
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , 'steal'                                           AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player2_id                             AS player_id
        , playbyplay.player2_name                           AS player_name
        , playbyplay.player2_team_id                        AS player_team_id
        , playbyplay.player2_team_city                      AS player_team_city
        , playbyplay.player2_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
        AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 5
        AND (homedescription LIKE '% STEAL %' OR visitordescription LIKE '% STEAL %')
------------------------------------------------------------------------------------------
UNION ALL
-- TURNOVERS 1 - event_subcode = 40 is missing in nba_event_codes
-- can also include in the general query at the end
-- playbyplay.eventmsgtype = 5
    SELECT
        playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
        AND playbyplay.eventmsgactiontype = nba_event_codes.event_subcode --(code 40 missing from event_subcode)
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 5
------------------------------------------------------------------------------------------
UNION ALL
-- TURNOVERS 2 - event_subcode = 40 only -- playbyplay.eventmsgtype = 5
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription 
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 5
        AND playbyplay.eventmsgactiontype = 40 --(eventmsgactiontype 40 matches with sub_code 3)
        AND nba_event_codes.event_subcode = 3
------------------------------------------------------------------------------------------
UNION ALL
-- GOT FOULED (player2) playbyplay.eventmsgtype = 6
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , 'got fouled'                                      AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
            END                                             AS is_home
        , playbyplay.player2_id                             AS player_id
        , playbyplay.player2_name                           AS player_name
        , playbyplay.player2_team_id                        AS player_team_id
        , playbyplay.player2_team_city                      AS player_team_city
        , playbyplay.player2_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
          ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 6
        AND playbyplay.player2_id IS NOT NULL
------------------------------------------------------------------------------------------
UNION ALL
-- COMMITTED FOULED playbyplay.eventmsgtype = 6
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , 'committed fouled'                                AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
          ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 6
------------------------------------------------------------------------------------------
UNION ALL
-- SUBBED IN (player2) playbyplay.eventmsgtype = 8
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , 'subbed in'                                       AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player2_id                             AS player_id
        , playbyplay.player2_name                           AS player_name
        , playbyplay.player2_team_id                        AS player_team_id
        , playbyplay.player2_team_city                      AS player_team_city
        , playbyplay.player2_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 8
        AND playbyplay.player2_id IS NOT NULL
------------------------------------------------------------------------------------------
UNION ALL
-- SUBBED OUT playbyplay.eventmsgtype = 8
    SELECT
        playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
        CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , 'subbed out'                                      AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 8
------------------------------------------------------------------------------------------
UNION ALL
-- JUMP BALL 1 (player1) playbyplay.eventmsgtype = 10
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 10
------------------------------------------------------------------------------------------
UNION ALL
-- JUMP BALL 2 (player2) playbyplay.eventmsgtype = 10
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
        END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
        END                                                 AS is_home
        , playbyplay.player2_id                             AS player_id
        , playbyplay.player2_name                           AS player_name
        , playbyplay.player2_team_id                        AS player_team_id
        , playbyplay.player2_team_city                      AS player_team_city
        , playbyplay.player2_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 10
------------------------------------------------------------------------------------------
UNION ALL
-- JUMP BALL 3 (player3, tipped to) playbyplay.eventmsgtype = 10
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , 'jump ball tip'                                   AS event_subsubaction
        , CASE WHEN homedescription IS NOT NULL THEN homedescription 
        ELSE visitordescription END                         AS description
        , CASE WHEN homedescription IS NOT NULL THEN 1 ELSE 0
            END                                             AS is_home
        , playbyplay.player3_id                             AS player_id
        , playbyplay.player3_name                           AS player_name
        , playbyplay.player3_team_id                        AS player_team_id
        , playbyplay.player3_team_city                      AS player_team_city
        , playbyplay.player3_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype = 10
------------------------------------------------------------------------------------------
UNION ALL
-- REST OF THE EVENTS NOT IN (1, 2, 3, 4, 5, 6, 8, 10)
    SELECT
          playbyplay.eventnum
        , playbyplay.period
        , playbyplay.wctimestring
        , playbyplay.pctimestring
        , CAST(SUBSTR(pctimestring, 1, POSITION(':' IN pctimestring) - 1) AS BIGINT) * 60 +
          CAST(SUBSTR(pctimestring, POSITION(':' IN pctimestring) + 1, LENGTH(pctimestring)) AS BIGINT)
                                                            AS period_time
        , playbyplay.eventmsgtype
        , playbyplay.eventmsgactiontype
        , nba_event_codes.event_action                      AS event_action
        , nba_event_codes.event_subaction
        , NULL                                              AS event_subsubaction
        , CASE 
            WHEN homedescription IS NOT NULL THEN homedescription 
            ELSE visitordescription
          END                                               AS description
        , CASE 
            WHEN homedescription IS NOT NULL THEN 1 ELSE 0
          END                                               AS is_home
        , playbyplay.player1_id                             AS player_id
        , playbyplay.player1_name                           AS player_name
        , playbyplay.player1_team_id                        AS player_team_id
        , playbyplay.player1_team_city                      AS player_team_city
        , playbyplay.player1_team_abbreviation              AS player_team_abbreviation
        , playbyplay.season
        , playbyplay.gameid
    FROM nba.playbyplay
        LEFT JOIN nba.nba_event_codes
        ON  playbyplay.eventmsgtype = nba_event_codes.eventmsgtype
    WHERE season = '{0}'
        AND CAST(gameid AS BIGINT) BETWEEN {1} AND {2}
        AND playbyplay.eventmsgtype NOT IN (1, 2, 3, 4, 6, 8, 10)
)