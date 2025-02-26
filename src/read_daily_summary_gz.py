
from .ghcnd_config import daily_summary_path

import tarfile
import polars as pl
import io
import warnings
from time import time
warnings.filterwarnings("ignore", message="Polars found a filename")
from .make_daily_summary_schema import make_schema

'''
There are currently (20-01-2025) 127819 station files.
'''

def rechunk_outputs(output_dict):
    for key, df in output_dict.items():
        output_dict[key] = df.rechunk()


'''
path - location of daily_summary.tar.gz
processor - station processor function.  It takes three args:
        psv_info - the tarfile header for the station's csv file
        psv_reader - the station data's file object
        output_dict - a dict of dataframes used to accumulate from
            the stations.  The processor is responsible for appending
            data to the appropriate dfs.
'''
def read_daily_summary_gz(path, processor, output_dict, start=0, max_stations=9999999):
    chunk_start_time = time()
    with tarfile.open(path, 'r:gz') as tf:
        print('Opened tarfile ' + path)
        n_stations = 0
        while psv_info := tf.next():
            n_stations += 1
            if n_stations > max_stations:
                break
            if n_stations < start:
                continue
            station_id = psv_info.name[0:11]
            with tf.extractfile(psv_info) as psv_reader:
#                with io.BytesIO(psv_reader.read()) as psv_buff:
                    processor(psv_info, psv_reader, output_dict)

            if n_stations % 1000 == 0:
                print(str(n_stations) + ' processed')
                if n_stations % 5000 == 0:
                    rechunk_outputs(output_dict)
                    now = time()
                    print('1000-chunk time: ' + str((now-chunk_start_time)/5) + 'sec')
                    chunk_start_time = now

    rechunk_outputs(output_dict)
