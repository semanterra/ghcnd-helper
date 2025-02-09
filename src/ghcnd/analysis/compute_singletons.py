from .read_parquet_file import read_parquet_df
from ..constants import DfName
import polars as pl

# - height (stations, observations)
# - width (columns)
# - number of obvalue columns
# - count of stations serving each number of obvalue columns
# - first observation date
# - median # of obvalue columns (across stations)
# - how much do locations change over time
# - how much do names change over time
# - do any attributes ever have more than one character


def compute_singletons():
    ret = {}
    flat_df = read_parquet_df(DfName.stations_flat)
    height = flat_df.select(pl.col('N_ROWS').sum()).item(0,0)
    n_locations_changed = flat_df.filter(pl.col('N_LOCATIONS') > 1).height
    n_names_changed = flat_df.filter(pl.col('N_NAMES') > 1).height
    first_date = flat_df['DATE_MIN'].min()
    median_obvalues = flat_df['N_OBVALUES'].median()
    n_obvalue_counts = flat_df['N_OBVALUES'].value_counts().sort('N_OBVALUES')

    # plot_n_obvalue_counts = n_obvalue_counts.plot.point(x='N_OBVALUES', y='count')

    describe_df = read_parquet_df(DfName.stations_describe)
    used_cols = describe_df.select(pl.col('COLUMN')).unique().cast(pl.String)['COLUMN']
    n_obvalues = used_cols.filter(used_cols.str.ends_with('ATTRIBUTES')).len()

    attr_df = read_parquet_df(DfName.stations_attr_use)
    used_values_df = attr_df.select(['ATTR','VALUE']).unique().sort(['ATTR','VALUE'])
    with pl.Config(tbl_rows=100):
        print(used_values_df)


compute_singletons()
