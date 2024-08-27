from read_pressure_slice import read_pressure_slice
from config import pressure_year_ranges
import polars as pl
import math
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np

pressure_slice = read_pressure_slice()

station_ids = sorted(pressure_slice[0].keys())
range_timedeltas = [dt.datetime(rg.stop, 1, 1) - dt.datetime(rg.start, 1, 1) for rg in pressure_year_ranges]

interval_size = '3h'
interval_timedelta = dt.timedelta(hours=3)
min_ratio = .95
spike_threshold = 50 #millibars; used to filter for single samples that are out of whack

# for a given station, range, and time interval size,
# return the number and ratio of intervals that have at least one observation in them
def calc_coverage(range_idx, station_id, interval_size):
    # get observation sets for station
    df = pressure_slice[range_idx][station_id]

    # add columns with difference between each observation and the previous one ('deltaP'),
    #  and between each observation and the next one ('deltaPAhead')
    df = df.with_columns(pl.col('station_level_pressure').diff().alias('deltaP'),
                         pl.col('station_level_pressure').diff(-1).alias('deltaPAhead'))

    # filter out any observation spikes which deviate from both the previous and past by more than spike_threshold,
    # and the deviation from both is of the same sign.
    # note: this would be better if it used the length of time between observations to compute the rate of change
    df_despiked = df.filter((pl.col('deltaP').abs() < spike_threshold).or_(
                       pl.col('deltaPAhead').abs() < spike_threshold,
                       pl.col('deltaP').sign() != pl.col('deltaPAhead').sign()
    ))
    if len(df_despiked) < len(df):
        print(station_id + ': spike(s) found: ' + str(len(df)-len(df_despiked)))
        df = df_despiked

    # select the first observation in each interval
    interval_groups = df.group_by_dynamic("timestamp", start_by='window', every=interval_size)
    first_interval_obss = interval_groups.agg(pl.col('station_level_pressure').first(), pl.col('timestamp').alias('ts').first())

    # compute the ratio of intervals with samples to the total intervals in the range
    max_intervals = math.ceil(range_timedeltas[range_idx] / interval_timedelta)
    sample_count = len(first_interval_obss)
    ratio = sample_count/max_intervals

    # note: downstream code would be clearer using a dataclass (not a tuple)
    return (sample_count, ratio, first_interval_obss)

# run calc_coverage on each observation set and save results in filtered_coverages dict
filtered_coverages = []
for range_idx in [0,1]:
    obs_coverages = [(station_id,calc_coverage(range_idx, station_id, interval_size)) for station_id in station_ids]
    filtered_coverages.append({station_id: calced[2] for (station_id,calced) in obs_coverages if calced[1] >= min_ratio})

# make list of station_ids for stations with good data for both ranges
stations_in_both = [station_id for station_id in filtered_coverages[0] if station_id in filtered_coverages[1]]
coverages_in_both = []
for station_id in stations_in_both:
    coverages_in_both.append((station_id, filtered_coverages[0][station_id],filtered_coverages[1][station_id]))

# add delta columns to each observation dataframe
# todo see if existing "deltaP" column can be used instead
for station_idx,(station, df0,df1) in enumerate(coverages_in_both):
    coverages_in_both[station_idx] = (station,
                                      df0.with_columns(pl.col('station_level_pressure').diff().abs().alias('delta')),
                                      df1.with_columns(pl.col('station_level_pressure').diff().abs().alias('delta')))
# at this point the observation-set for each station should be complete

# make frame of stations containing the station_id and the 2 observation series (1 per range)
stations_df = pl.from_records(coverages_in_both,['Station_ID', 'obs0', 'obs1'], orient='row')
# add 2 sumdiff columns to each station containing the sum of the absolute deltas for each range
stations_df = stations_df.with_columns(
    pl.col('obs0').map_elements(lambda obs_df: obs_df.select(pl.sum('delta')).item()).alias('sumdiff0'),
    pl.col('obs1').map_elements(lambda obs_df: obs_df.select(pl.sum('delta')).item()).alias('sumdiff1'))

# The station with the largest sumdiff often has bad data.
# Make some diagnostic plots
largest_sumdiff1 = stations_df.select(pl.max('sumdiff1')).item()
largest_row = stations_df.row(by_predicate=(pl.col('sumdiff1')==largest_sumdiff1))

# todo convert plots to plotly

plt.hist(largest_row[1]['station_level_pressure'])
plt.show()
plt.hist(largest_row[2]['station_level_pressure'])
plt.show()
plt.plot(largest_row[1]['timestamp'], largest_row[1]['station_level_pressure'])
plt.show()
plt.plot(largest_row[2]['timestamp'], largest_row[2]['station_level_pressure'])
plt.show()
station_sumdiffs = {}
for station_idx,(station, df0,df1) in enumerate(coverages_in_both):
    sum0 = df0.select(pl.sum('delta')).item()
    sum1 = df1.select(pl.sum('delta')).item()
    sumdiff = sum1-sum0
    station_sumdiffs[station] = sumdiff
    print(station + ' sumdiff: ' + str(sumdiff))


plt.hist(stations_df.select('sumdiff0'), label='sumdiff0')
plt.show()
plt.hist(stations_df.select('sumdiff1'), label='sumdiff1')
plt.show()

total_sumdiff = sum(station_sumdiffs.values())
print( 'total_sumdiff: ' + str(total_sumdiff))

print('done')

