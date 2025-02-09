import polars as pl

from ..ghcnd_config import daily_summary_output_dir

_df_cache = {}

def read_parquet_df(name):
    if name in _df_cache:
        return _df_cache[name]
    df = pl.read_parquet(daily_summary_output_dir + name + '.parquet')
    _df_cache[name] = df
    return df

