
from ghcnd_config import daily_summary_path

import tarfile
import polars as pl
import io
import warnings
warnings.filterwarnings("ignore", message="Polars found a filename")

'''
There are currently (20-01-2025) 127819 station files.
'''

def read_daily_summary_gz(path, processor, max_stations=9999999):
    with tarfile.open(path, 'r:gz') as tf:
        print('Opened tarfile ' + path)
        n_stations = 0
        while psv_info := tf.next():
            n_stations += 1
            if n_stations > max_stations:
                break
            station_id = psv_info.name[0:11]
            with tf.extractfile(psv_info) as psv_reader:
#                with io.BytesIO(psv_reader.read()) as psv_buff:
                    processor(psv_info, psv_reader)

stations = []

# TODO read header line beforehand, filter to desired columns, and set columns param on read_csv
# desired_columns = ['DATE', 'PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN', ] # many more!

def test_processor(tar_info, buff):
    station_id = tar_info.name[0:11]
    stations.append([station_id])
    header = buff.peek(1000).split(b'\n')[0].decode("utf-8")
    station_df = pl.read_csv(
        buff,
        has_header=True,
        schema_overrides={
        'DATE': pl.Date,
        'ELEVATION': pl.Float32,
        'PRCP': pl.Float32,
        'SNOW': pl.Float32,

    })
    print(station_df.describe())
    print(station_df.glimpse())
    print(station_df.count())
    print(station_df.item(0,'DATE'))                        # date of first observation
    print(station_df.item(station_df.shape[0]-1, 'DATE'))   # date of last observation
    print(station_df.drop_nulls('PRCP').shape[0] if 'PRCP' in station_df.columns else 0 ) # observations with non-null precip

'''    q = (
        station_df.lazy()
        .agg(
            pl.len(),
            pl.col("PRCP").count(),
        )
    )
    print(q.collect().describe())
'''


read_daily_summary_gz(daily_summary_path, test_processor)
df = pl.DataFrame(stations, ['Station_Id'], orient='row')
print(df.describe())

# TODO scan files, keeping only first date and last line, and filtering on that.