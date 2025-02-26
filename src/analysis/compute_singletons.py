from .read_parquet_file import read_parquet_df
from constants import DfName
from ghcnd_config import plot_dir
import polars as pl
import plotly.express as px

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

    plot_n_obvalue_counts = px.bar(n_obvalue_counts, x='N_OBVALUES', y='count', log_y=True,
                                   labels={ 'N_OBVALUES': 'Total number of different obvalues ever reported by station',
                                            'count':'Number of stations'
                                            })
    plot_n_obvalue_counts.write_image(plot_dir + 'nObvalueCountHisto.png')
    # compute number of stations reporting in each year
    start_years = flat_df['DATE_MIN'].dt.year().value_counts().sort('DATE_MIN')
    end_years = flat_df['DATE_MAX'].dt.year().value_counts().sort('DATE_MAX')
    # shift end_years ahead 1 year
    end_years = end_years.with_columns(DATE_MAX=pl.col('DATE_MAX')+1)
    year_df = pl.DataFrame(data={'YEAR':range(1763,2026), 'COUNT':0}, schema={'YEAR':pl.Int32,'COUNT':pl.UInt32})
    year_df = year_df.join(start_years, how='left', left_on='YEAR', right_on='DATE_MIN', maintain_order='left', suffix='_START')
    year_df = year_df.join(end_years, how='left', left_on='YEAR', right_on='DATE_MAX', maintain_order='left', suffix='_END')
    year_df = year_df.fill_null(0)
    year_df =year_df.with_columns(CUM=pl.col('count').cum_sum().sub(pl.col('count_END').cum_sum()))

    # PLOT : number of stations reporting in each year
    plot_stations__year = px.line(year_df, x='YEAR', y='CUM', range_x=[1763, 2024], log_y=True,
                                  labels={'YEAR':'Year', 'CUM': 'Active Stations'})
    plot_stations__year.write_image(plot_dir + 'stations_year.png')

    still_active_since_years = (flat_df.filter(pl.col('DATE_MAX').dt.year() >= 2024)
                                 .group_by(pl.col('DATE_MIN').dt.year()).len().sort('DATE_MIN')
                                 .with_columns(CUM=pl.col('len').cum_sum()))
    plot_still_active_since_years = (
        px.line(still_active_since_years, x='DATE_MIN', y='CUM', log_y=True, range_x=[1850, 2024],
          labels={'DATE_MIN':'If you need data since...', 'CUM': '... you have this many stations still active'})
        )
    plot_still_active_since_years.write_image(plot_dir + 'currentStations_startYear.png')


    describe_df = read_parquet_df(DfName.stations_describe)
    used_cols = describe_df.select(pl.col('COLUMN')).unique().cast(pl.String)['COLUMN'].sort()
    n_obvalues = used_cols.filter(used_cols.str.ends_with('ATTRIBUTES')).len()

    attr_df = read_parquet_df(DfName.stations_attr_use)
    # show count of each value used for each attribute type
    # used_values_df = attr_df.select(['ATTR','VALUE']).unique().sort(['ATTR','VALUE'])
    used_values_df = attr_df.group_by('ATTR','VALUE').agg(
            pl.col('COUNT').sum()
    ).with_columns(pl.concat_str(pl.col('ATTR').cast(pl.String), pl.col('VALUE'), separator=': ').alias('LABEL')
    ).sort(by='LABEL', descending=True)

    # px.bar(used_values_df, y='LABEL', x='COUNT', orientation='h', log_x=True).show()

    # Sample chloropleth map from https://plotly.com/python/choropleth-maps/
    # df = px.data.gapminder().query("year==2007")
    # fig = px.choropleth(df, locations="iso_alpha",
    #                     color="lifeExp", # lifeExp is a column of gapminder
    #                     hover_name="country", # column to add to hover information
    #                     color_continuous_scale=px.colors.sequential.Plasma)
    # fig.show()

    print('done')