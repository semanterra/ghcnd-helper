
from ghcnd_config import daily_summary_path

import tarfile
import polars as pl
import io
import warnings
warnings.filterwarnings("ignore", message="Polars found a filename")
from make_daily_summary_schema import make_schema

'''
There are currently (20-01-2025) 127819 station files.
'''

'''
path - location of daily_summary.tar.gz
processor - station processor function.  It takes three args:
        psv_info - the tarfile header for the station's csv file
        psv_reader - the station data's file object
        output_dict - a dict of dataframes used to accumulate from
            the stations.  The processor is responsible for appending
            data to the appropriate dfs.
'''
def read_daily_summary_gz(path, processor, output_dict, max_stations=9999999):
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
                    processor(psv_info, psv_reader, output_dict)
            if n_stations % 1000 is 0:
                print(str(n_stations) + ' processed')
    for key, df in output_dict.items():
        output_dict[key] = df.rechunk()

schema = make_schema()
obvalues = list(map(
    lambda col: col.removesuffix('_ATTRIBUTES'),
    filter(lambda col: col.endswith('_ATTRIBUTES'), schema.keys())
))
column_enum = pl.Enum(schema.keys())

station_flat_schema = {
    'STATION':pl.String,
    'N_COLUMNS': pl.Int32,
    'N_ROWS': pl.Int32,
    'DATE_MIN':pl.Date,
    'DATE_MAX':pl.Date
}

station_describe_schema = {
    'STATION': pl.String,
    'COLUMN':column_enum,
    'COUNT':pl.Int32,
    'MAX_FLOAT': pl.Float32,
    'MIN_FLOAT': pl.Float32,
    'NULL_COUNT': pl.Int32
}

attr_type_enum = pl.Enum(['MEASURE','QUALITY','SOURCE'])

station_attr_use_schema = {
    'STATION': pl.String,
    'OBVALUE': column_enum,
    'ATTR': attr_type_enum,
    'VALUE':pl.String,  # value within attribute to count
    'COUNT':pl.Int32,
}

def test_processor(tar_info, buff, output_dict):
    header_string = buff.peek(1000).split(b'\n')[0].decode("utf-8")
    header = list(map(lambda s: s[1:-1], header_string.split(',')))
    filtered_schema = {key: val for key, val in schema.items() if key in header}
    attribute_cols = filter(lambda col: col.endswith('_ATTRIBUTES'), filtered_schema.keys())
    station_obvalues = list(map(lambda col: col.removesuffix('_ATTRIBUTES'), attribute_cols))
    station_df = pl.read_csv(
        buff,
        has_header=True,
        schema=filtered_schema,
    )

    counts_df = station_df.count()
    attr_selects = []
    split_struct_cols = []
    for attributes_col in list(attribute_cols) :
        if counts_df.item(0,attributes_col) > 0:
            obvalue =  attributes_col.removesuffix('_ATTRIBUTES')
            split_struct_col = obvalue+'_SPLIT_ATTRS'
            split_struct_cols.append(split_struct_col)
            attr_selects.append(
                pl.col(attributes_col)
                    .str.split_exact(",", 3)
                    .struct.rename_fields([obvalue+"_MEASURE", obvalue+"_QUALITY", obvalue+"_SOURCE"])
                    .alias(split_struct_col)
            )

    station_df = station_df.with_columns(station_df.select(attr_selects).unnest(split_struct_cols))

    station = tar_info.name[:11]

    counts = station_df.count().row(0)
    max_df=station_df.max()
    maxs = max_df.row(0)
    min_df = station_df.min()
    mins = min_df.row(0)
    null_counts = station_df.null_count().row(0)

    station_flat_data = {
        'STATION':station,
        'N_COLUMNS': station_df.width,
        'N_ROWS': station_df.height,
        'DATE_MIN':min_df.item(0,'DATE'),
        'DATE_MAX':max_df.item(0,'DATE'),
    }
    station_flat_df = pl.DataFrame( station_flat_data, schema=station_flat_schema)
    output_dict['stations_flat'].vstack(station_flat_df, in_place=True)

    def clean_nums(col):
        ret = list(col)
        for i, data in enumerate(col):
            if not (type(data) == int or type(data) == float):
                ret[i] = None
        return ret

    station_describe_data = {
        'STATION': [station]*len(station_df.columns),
        'COLUMN':station_df.columns,
        'COUNT':counts,
        'MAX_FLOAT': clean_nums(maxs),
        'MIN_FLOAT': clean_nums(mins),
        'NULL_COUNT': null_counts,
    }

    station_describe_df = pl.DataFrame( station_describe_data, schema=station_describe_schema)
    output_dict['stations_describe'].vstack(station_describe_df, in_place=True)

'''
    # TODO
    station_attr_use_data = {
        'STATION': [station]*(len(station_obvalues)*3),
        'OBVALUE':pl.String, # TODO 3 consecutive copies of obvalue name for each obvalue
        'ATTR': ['MEASURE', 'QUALITY', 'SOURCE'] * len(station_obvalues),
        'VALUE':
        'COUNT':
    }
    station_attr_use_df = pl.DataFrame(station_attr_use_data, schema=station_attr_use_schema)
    output_dict['stations_attr_use'].vstack(station_attr_use_df, in_place=True)
'''
    # print(description.describe())
    # print(description)
    # print(station_df.glimpse())
    # print(station_df.count())
    # print(station_df.item(0,'DATE'))                        # date of first observation
    # print(station_df.item(station_df.shape[0]-1, 'DATE'))   # date of last observation
    # print(station_df.drop_nulls('PRCP').shape[0] if 'PRCP' in station_df.columns else 0 ) # observations with non-null precip


output_dicts = {
    'stations_flat': pl.DataFrame([], schema=station_flat_schema),
    'stations_describe': pl.DataFrame([], schema=station_describe_schema, orient='row'),
}
read_daily_summary_gz(daily_summary_path, test_processor, output_dicts)

output_dicts['stations_flat'].describe()
# TODO save dict to disk
# TODO get aggregate statistics for each column from stations_describe - # of stations for each stat
# TODO characterize column usage
# TODO characterize time coverage - add column with % of date range covered
# TODO find unused columns
# TODO figure out WT*, WV*
# TODO characterize attribute usage