import pandas as pd


parameters = {
    'Counter': 0,
    'DateFrom': '10/27/2015',
    'DateTo': '10/31/2015',
    'Direction': 'ASC',
    'LeagueID': '00',
    'PlayerOrTeam': 'T',
    'Season': '2015-16',
    'SeasonType': 'Regular Season',
    'Sorter': 'DATE'
}

resource = 'leaguegamelog'
headers = [
    'SEASON_ID',
    'TEAM_ID',
    'TEAM_ABBREVIATION',
    'TEAM_NAME',
    'GAME_ID',
    'GAME_DATE',
    'MATCHUP',
    'WL',
    'MIN',
    'FGM',
    'FGA',
    'FG_PCT',
    'FG3M',
    'FG3A',
    'FG3_PCT',
    'FTM',
    'FTA',
    'FT_PCT',
    'OREB',
    'DREB',
    'REB',
    'AST',
    'STL',
    'BLK',
    'TOV',
    'PF',
    'PTS',
    'PLUS_MINUS',
    'VIDEO_AVAILABLE'
 ]
name = 'LeagueGameLog'
rowSet = [[
    '22015',
    1610612740,
    'NOP',
    'New Orleans Pelicans',
    '03',
    '2015-10-27',
    'NOP @ GSW',
    'L',
    240,
    35,
    83,
    0.422,
    6,
    18,
    0.333,
    19,
    27,
    0.704,
    8,
    25,
    33,
    21,
    9,
    3,
    19,
    26,
    95,
    -16,
    1],
  [  
    '22015',
    1610612744,
    'GSW',
    'Golden State Warriors',
    '03',
    '2015-10-27',
    'GSW vs. NOP',
    'W',
    240,
    41,
    96,
    0.427,
    9,
    30,
    0.3,
    20,
    22,
    0.909,
    21,
    35,
    56,
    29,
    8,
    7,
    20,
    29,
    111,
    16,
    1],
  [
    '22015',
    1610612741,
    'CHI',
    'Chicago Bulls',
    '02',
    '2015-10-31', # made up
    'CHI vs. CLE',
    'W',
    240,
    37,
    87,
    0.425,
    7,
    19,
    0.368,
    16,
    23,
    0.696,
    7,
    40,
    47,
    13,
    6,
    10,
    13,
    22,
    97,
    2,
    1],
  [
    '22015',
    1610612739,
    'CLE',
    'Cleveland Cavaliers',
    '02',
    '2015-10-31', # made up
    'CLE @ CHI',
    'L',
    240,
    38,
    94,
    0.404,
    9,
    29,
    0.31,
    10,
    17,
    0.588,
    11,
    39,
    50,
    26,
    5,
    7,
    11,
    21,
    95,
    -2,
    1],
]

resultSet_dict = {
    'headers': headers,
    'name': name,
    'rowSet': rowSet
}
mock_gamelog_data = {
    'resultSets': [resultSet_dict],
    'resource': resource,
    'parameters': parameters
}

# mock uploaded
columns = [
    'season',
    'game_date',
    'game_id',
    'team1',
    'team2',
    'gamelog_uploadedFLG',
    'lastuploadDTS',
    'base_url',
    'params'
]
data = [
    ['2015-16', '2015-10-27', '1234', 'someteam1', 'someteam2'] + 4 * ['somevalue'],
    ['2015-16', '2015-10-28', '5678', 'otherteam1', 'otherteam2'] + 4 * ['somevalue']
]

mock_df_uploaded = pd.DataFrame(data=data, columns=columns)