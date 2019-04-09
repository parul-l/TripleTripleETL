import pandas as pd

# play by play data
rowSet_pbp = [[
    '0021500001', 
    15, 
    3, 
    11, 
    1, 
    '8:15 PM', 
    '9:42', 
    None, 
    None, 
    'Marc Morris Free Throw 1 of 2 (3 PTS)', 
    '5 - 4', 
    '-1', 
    5, 
    202694, 
    'Marcus Morris', 
    1610612765, 
    'Detroit', 
    'Pistons', 
    'DET',
    0,
    0,
    None,
    None,
    None,
    None,
    None,
    0,
    0,
    None,
    None,
    None,
    None,
    None
],
 [
    '0021500001',
    17,
    3,
    12,
    1,
    '8:15 PM',
    '9:42',
    None,
    None,
    'MISS Marc Morris Free Throw 2 of 2',
    None,
    None,
    5,
    202694,
    'Marcus Morris',
    1610612765,
    'Detroit',
    'Pistons',
    'DET',
    0,
    0,
    None,
    None,
    None,
    None,
    None,
    0,
    0,
    None,
    None,
    None,
    None,
    None
]]

headers_pbp = [
    'GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'WCTIMESTRING', 
    'PCTIMESTRING', 'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION','SCORE',
    'SCOREMARGIN','PERSON1TYPE','PLAYER1_ID','PLAYER1_NAME','PLAYER1_TEAM_ID','PLAYER1_TEAM_CITY',
    'PLAYER1_TEAM_NICKNAME', 'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE', 'PLAYER2_ID',
    'PLAYER2_NAME', 'PLAYER2_TEAM_ID', 'PLAYER2_TEAM_CITY', 'PLAYER2_TEAM_NICKNAME',
    'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE', 'PLAYER3_ID', 'PLAYER3_NAME', 'PLAYER3_TEAM_ID',
    'PLAYER3_TEAM_CITY', 'PLAYER3_TEAM_NICKNAME', 'PLAYER3_TEAM_ABBREVIATION'
]

# boxscore_traditional data
rowSet0_bs_traditional = [[
    '0021500001',
    1610612765,
    'DET',
    'Detroit',
    202694,
    'Marcus Morris',
    'F',
    '',
    '37:05',
    6,
    19,
    0.316,
    1,
    4,
    0.25,
    5,
    6,
    0.833,
    5,
    5,
    10,
    4,
    0,
    0,
    0,
    1,
    18,
    17.0
],
 [
    '0021500001',
    1610612765,
    'DET',
    'Detroit',
    101141,
    'Ersan Ilyasova',
    'F',
    '',
    '34:26',
    6,
    12,
    0.5,
    3,
    6,
    0.5,
    1,
    2,
    0.5,
    3,
    4,
    7,
    3,
    0,
    1,
    3,
    4,
    16,
    20.0
]]
rowSet1_bs_traditional = [[
    '0021500001',
    1610612765,
    'Pistons',
    'DET',
    'Detroit',
    '240:00',
    37,
    96,
    0.385,
    12,
    29,
    0.414,
    20,
    26,
    0.769,
    23,
    36,
    59,
    23,
    5,
    3,
    15,
    15,
    106,
    12.0
],
 [
    '0021500001',
    1610612737,
    'Hawks',
    'ATL',
    'Atlanta',
    '240:00',
    37,
    82,
    0.451,
    8,
    27,
    0.296,
    12,
    15,
    0.8,
    7,
    33,
    40,
    22,
    9,
    4,
    15,
    25,
    94,
    -12.0
]]
rowSet2_bs_traditional = [[
    '0021500001',
    1610612765,
    'Pistons',
    'DET',
    'Detroit',
    'Starters',
    '177:50',
    29,
    71,
    0.408,
    10,
    21,
    0.476,
    20,
    26,
    0.769,
    18,
    30,
    48,
    16,
    4,
    3,
    9,
    10,
    88
],
 [
    '0021500001',
    1610612765,
    'Pistons',
    'DET',
    'Detroit',
    'Bench',
    '177:50',
    29,
    71,
    0.408,
    10,
    21,
    0.476,
    20,
    26,
    0.769,
    18,
    30,
    48,
    16,
    4,
    3,
    9,
    10,
    88
]]


headers0_bs_traditional = [
    'GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_ID', 'PLAYER_NAME', 
    'START_POSITION', 'COMMENT', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 
    'PF', 'PTS', 'PLUS_MINUS'
]
headers1_bs_traditional = [ 
    'GAME_ID', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 
    'TEAM_CITY', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A',
    'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
    'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS'
]
headers2_bs_traditional = [
    'GAME_ID', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'STARTERS_BENCH', 
    'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA',
    'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
]

# rowSet bs_player
rowSet0_bs_player = [[
    '0021500001',
    1610612765,
    'DET',
    'Detroit',
    202694,
    'Marcus Morris',
    'F',
    '',
    '37:05',
    4.03,
    2.48,
    9,
    7,
    16,
    81,
    0,
    0,
    58,
    4,
    4,
    9,
    0.444,
    2,
    10,
    0.2,
    0.316,
    3,
    7,
    0.429
],
 [
    '0021500001',
    1610612765,
    'DET',
    'Detroit',
    101141,
    'Ersan Ilyasova',
    'F',
    '',
    '34:26',
    4.05,
    2.32,
    5,
    13,
    18,
    75,
    0,
    0,
    55,
    3,
    2,
    5,
    0.4,
    4,
    7,
    0.571,
    0.5,
    2,
    4,
    0.5
]]

rowSet1_bs_player = [[
    '0021500001',
    1610612765,
    'Pistons',
    'DET',
    'Detroit',
    '240:00',
    16.39,
    40,
    69,
    109,
    440,
    4,
    3,
    305,
    23,
    21,
    54,
    0.389,
    16,
    42,
    0.381,
    0.385,
    18,
    31,
    0.581
],
 [
    '0021500001',
    1610612737,
    'Hawks',
    'ATL',
    'Atlanta',
    '240:00',
    16.67,
    26,
    72,
    93,
    430,
    2,
    0,
    319,
    22,
    15,
    33,
    0.455,
    22,
    49,
    0.449,
    0.451,
    13,
    26,
    0.5
]]

headers0_bs_player = [
    'GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_ID',
    'PLAYER_NAME', 'START_POSITION', 'COMMENT', 'MIN', 'SPD', 'DIST', 'ORBC', 
    'DRBC', 'RBC', 'TCHS', 'SAST', 'FTAST', 'PASS', 'AST', 'CFGM', 'CFGA',
    'CFG_PCT', 'UFGM', 'UFGA', 'UFG_PCT', 'FG_PCT', 'DFGM', 'DFGA','DFG_PCT'
]
headers1_bs_player = [
    'GAME_ID', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'MIN',
    'DIST', 'ORBC', 'DRBC', 'RBC', 'TCHS', 'SAST', 'FTAST', 'PASS', 'AST',
    'CFGM', 'CFGA', 'CFG_PCT', 'UFGM', 'UFGA', 'UFG_PCT', 'FG_PCT', 'DFGM', 
    'DFGA', 'DFG_PCT'
]



# Create final json's
mock_data_pbp = {
    'parameters' : {'EndPeriod': 10, 'GameID': '0021500001', 'StartPeriod': 1},
    'resource': 'playbyplay',
    'resultSets': [
        {
            'name': 'PlayByPlay',
            'headers': headers_pbp,
            'rowSet': rowSet_pbp
        },
        {
            'headers': ['VIDEO_AVAILABLE_FLAG'],
            'name': 'AvailableVideo',
            'rowSet': [[1]]}
    ]

}

mock_data_bs_traditional = {
    'paramters': {
        'EndPeriod': 10,
        'EndRange': 55800,
        'GameID': '0021500001',
        'RangeType': 2,
        'StartPeriod': 1,
        'StartRange': 0
    },
    'resource': 'boxscore',
    'resultSets': [
        {
            'name': 'PlayerStats',
            'headers': headers0_bs_traditional,
            'rowSet': rowSet0_bs_traditional
        },
        {
            'name': 'TeamStats',
            'headers': headers1_bs_traditional,
            'rowSet': rowSet1_bs_traditional
        },
        {
            'name': 'TeamStarterBenchStats',
            'headers': headers2_bs_traditional,
            'rowSet': rowSet2_bs_traditional
        }
    ]
}

mock_data_bs_player = {
    'parameters': {'GameID': '0021500001'},
    'resource': 'boxscoresummary',
    'resultSets': [
        {
           'name': 'PlayerStats',
           'headers': headers0_bs_player,
           'rowSet': rowSet0_bs_player 
        },
        {
           'name': 'PlayerStats',
           'headers': headers1_bs_player,
           'rowSet': rowSet1_bs_player 
        },        
    ]
}


# mock uploaded
columns = [
    'season',
    'game_date',
    'game_id',
    'team1',
    'team2',
    'fileuploadedFLG',
    'lastuploadDTS',
    'base_url',
    'params'
]
data = [
    ['2015-16', '2015-10-27', '1234', 'someteam1', 'someteam2'] + 4 * ['somevalue'],
    ['2015-16', '2015-10-28', '5678', 'otherteam1', 'otherteam2'] + 4 * ['somevalue']
]

mock_df_uploaded = pd.DataFrame(data=data, columns=columns)