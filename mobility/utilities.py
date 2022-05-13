import pandas as pd

def read_parquet(path, columns=None):
    with open(path, "rb") as f:
        df = pd.read_parquet(f, columns=columns)
    return df
    
    