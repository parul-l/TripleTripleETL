import pandas as pd


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