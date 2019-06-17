ALTER TABLE {} ADD
    PARTITION (
        season = '{}',
        gameid = '{}'
    )
    LOCATION '{}';