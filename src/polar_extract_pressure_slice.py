from config import pressure_year_ranges, pressure_slice_path, ghcnh_local_path, ghcnh_year_to_filename_dict

import tarfile
import polars as pl
import warnings

warnings.filterwarnings("ignore", message="Polars found a filename")

usecols = [
    'Station_ID',
    'Year', 'Month', 'Day', 'Hour', 'Minute',
    'station_level_pressure', 'station_level_pressure_Quality_Code', 'station_level_pressure_Source_Station_ID']
range_pair_data =[]
for rng_idx, rng in enumerate(pressure_year_ranges):
    stations_data = {}
    for year in rng:
        path = ghcnh_local_path +ghcnh_year_to_filename_dict[year]
        with tarfile.open(path, 'r:gz') as tf:
            print('Opened tarfile ' + path)
            while psv_info := tf.next():
                station_id = psv_info.name.split('_')[1]
                # if in second pass and no data for this station in first pass, skip over.
                # There are many more stations from 2022 onward.
                if rng_idx and station_id not in range_pair_data[0]:
                    continue

                psv_file = tf.extractfile(psv_info)
                df = pl.read_csv(psv_file, separator='|',
                    columns=usecols,
                    schema_overrides={
                        'station_level_pressure': pl.Float32,
                        'station_level_pressure_Quality_Code': pl.String },
                    null_values={
                        'station_level_pressure_Source_Station_ID': '',
                        'station_level_pressure_Quality_Code': '' }
                )
                df = df.with_columns(pl.datetime(pl.col('Year'),pl.col('Month'),pl.col('Day'),pl.col('Hour'),pl.col('Minute')).alias('timestamp'))
                station_id = df['Station_ID'][0]
                df=df.drop(['Station_ID', 'Year', 'Month', 'Day', 'Hour', 'Minute'])

                df_with_pressure = df.filter(pl.col('station_level_pressure').is_not_null())
                # filter out station_level_pressure_Quality_Code == 0,1,15
                #  (logical consistency, outlier, clean up)
                df_filtered = df_with_pressure.filter(~pl.col('station_level_pressure_Quality_Code').is_in(['0','1','15']))
                # if len(df_filtered) < len(df_with_pressure):
                #     print('Removed obs with bad quality code: ' + str(len(df_with_pressure)-len(df_filtered)))
                #     df_with_pressure = df_filtered
                # find max count group by station_level_pressure_Source_Code,
                #  then filter to that Source_Code
                substation_counts = df_with_pressure['station_level_pressure_Source_Station_ID'].value_counts(sort=True)
                if len(substation_counts)  >1:

                    ssid = substation_counts['station_level_pressure_Source_Station_ID'][0]
                    df_with_pressure = df_with_pressure.filter(pl.col('station_level_pressure_Source_Station_ID') == ssid)
                if len(df_with_pressure):
                    if station_id in stations_data:
                        stations_data[station_id] = pl.concat([stations_data[station_id], df_with_pressure])
                    else:
                        stations_data[station_id] = df_with_pressure
    range_pair_data.append(stations_data)

for range_idx in (0,1):
    range_data = range_pair_data[range_idx]
    for station_id, station_df in range_data.items():
        path = pressure_slice_path + str(range_idx) + '/' + station_id + '.parquet'
        station_df.write_parquet(path)
print('pressure slice extracted')



