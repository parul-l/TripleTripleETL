import numpy as np
import pandas as pd

columns = [
    'input_filename',
    'gameinfo_uploadedFLG',
    'gameposition_uploadedFLG',
    'playersinfo_uploadedFLG',
    'teamsinfo_uploadedFLG',
    'lastuploadDTS'
]
data = [
    ['thisisafile.7z'] + 5 * [np.nan],
    ['somefile.7z'] + 5 * [np.nan]
]

mock_df_uploaded = pd.DataFrame(data=data, columns=columns)