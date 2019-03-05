import os
import pandas as pd


def get_uploaded_metadata(
    filepath: str,
    columns: list
):
    if os.path.isfile(filepath):
        df = pd.read_parquet(filepath)
        if list(df.columns) != columns:
            raise ValueError('There is a column order discrepancy!')
        return df
    else:
        return pd.DataFrame(columns=columns)