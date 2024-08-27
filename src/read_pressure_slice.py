from config import pressure_slice_path, ghcnh_local_path, ghcnh_year_to_filename_dict

import polars as pl
from pathlib import Path

def read_pressure_slice():
    # make list of station ids in both ranges from cache filenames, sorted
    station_ids_by_range = []
    for rangeIdx in (0,1):
        range_path = Path(pressure_slice_path + str(rangeIdx))
        station_ids = set([fn.stem for fn in range_path.iterdir()])
        station_ids_by_range.append(station_ids)
    station_ids_in_both = sorted(station_ids_by_range[0] & station_ids_by_range[1])
    print(repr(station_ids_in_both))

    # load pressure data for ranges from station parquet files
    observation_dfs_by_range_station = []
    for rangeIdx in (0,1):
        observation_dfs_by_station = {}
        # load up
        for station_id in station_ids_in_both:
            df_path = pressure_slice_path + str(rangeIdx) + '/' + station_id +'.parquet'
            station_df = pl.read_parquet(df_path)
            observation_dfs_by_station[station_id] = station_df
        observation_dfs_by_range_station.append(observation_dfs_by_station)

    print('pressure slice read!')
    return observation_dfs_by_range_station

obs = read_pressure_slice()
print('hello')