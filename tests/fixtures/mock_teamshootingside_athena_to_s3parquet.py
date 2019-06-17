import numpy as np
import pandas as pd

columns = [
        'season',
        'gameid',
        'gamedate',
        'teamid1',
        'teamid2',
        'uploadedFLG',
        'lastuploadDTS'
]
data = [
    ['2015-2016', '1234', '2015-12-31', 'ABC', 'THE'] + 2 * [np.nan],
    ['2015-2016', '4321', '2015-11-20', 'APA', 'BUG'] + 2 * [np.nan]
]

mock_df_uploaded = pd.DataFrame(data=data, columns=columns)