
from ghcnd_config import daily_summary_path

import tarfile
import polars as pl
import io
import warnings

from line_profiler_pycharm import profile

warnings.filterwarnings("ignore", message="Polars found a filename")
from make_daily_summary_schema import make_schema
from read_daily_summary_gz import read_daily_summary_gz
'''
There are currently (20-01-2025) 127819 station files.
'''


schema = make_schema()
obvalues = list(map(
    lambda col: col.removesuffix('_ATTRIBUTES'),
    filter(lambda col: col.endswith('_ATTRIBUTES'), schema.keys())
))
column_enum = pl.Enum(schema.keys())

station_flat_schema = {
    'STATION':pl.String,
    'N_OBVALUES': pl.Int32,
    'N_ROWS': pl.Int32,
    'DATE_MIN':pl.Date,
    'DATE_MAX':pl.Date,
    'N_LOCATIONS': pl.Int32,
    'N_NAMES': pl.Int32
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

def process_station_csv(tar_info, buff, output_dict):

    def read_station_csv(buff):
        header_string = buff.peek(1000).split(b'\n')[0].decode("utf-8")
        header = list(map(lambda s: s[1:-1], header_string.split(',')))
        filtered_schema = {key: val for key, val in schema.items() if key in header}

        station_df = pl.read_csv(
            buff,
            has_header=True,
            schema=filtered_schema,
        )
        return station_df, filtered_schema


    def split_attributes(station_df):
        counts_df = station_df.count()
        attr_selects = []
        split_struct_cols = []
        for attributes_col in attribute_cols :
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

        unnested_df = station_df.select(attr_selects).unnest(split_struct_cols)
        nulled_df = unnested_df.with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
        return station_df.with_columns(nulled_df)


    def record_flat_data(station_df, output_dict):
        n_names = station_df.n_unique(subset=pl.col('NAME'))
        n_locations = station_df.n_unique(subset=pl.col(['LATITUDE','LONGITUDE','ELEVATION']))

        station_flat_data = {
            'STATION':station,
            'N_OBVALUES': len(station_obvalues),
            'N_ROWS': station_df.height,
            'DATE_MIN':min_df.item(0,'DATE'),
            'DATE_MAX':max_df.item(0,'DATE'),
            'N_LOCATIONS': n_locations,
            'N_NAMES': n_names,
        }
        station_flat_df = pl.DataFrame( station_flat_data, schema=station_flat_schema)
        output_dict['stations_flat'].vstack(station_flat_df, in_place=True)


    def record_describe_data(station_df, output_dict):

        counts = station_df.count().row(0)
        null_counts = station_df.null_count().row(0)

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


    def record_attr_use_data(station_df, output_dict):
        for attr in ['MEASURE', 'QUALITY', 'SOURCE']:
            for obvalue in station_obvalues:
                col = obvalue + '_' + attr
                value_count_df = (station_df[col].value_counts()
                     .rename({col:'VALUE', 'count':'COUNT'})
                     .filter(pl.col('VALUE').is_not_null()))
                if value_count_df.height:
                    value_count_df.insert_column(0, pl.lit(station).alias('STATION'))
                    value_count_df.insert_column(1, pl.lit(obvalue).cast(column_enum).alias('OBVALUE'))
                    value_count_df.insert_column(2, pl.lit(attr).cast(attr_type_enum).alias('ATTR'))

                    if output_dict['stations_attr_use'].height:
                        output_dict['stations_attr_use'].vstack(value_count_df, in_place=True)
                    else:
                        output_dict['stations_attr_use'] = value_count_df

    station_df, filtered_schema = read_station_csv(buff)

    attribute_cols = list(filter(lambda col: col.endswith('_ATTRIBUTES'), filtered_schema.keys()))
    station_obvalues = list(map(lambda col: col.removesuffix('_ATTRIBUTES'), attribute_cols))

    station_df = split_attributes(station_df)

    station = tar_info.name[:11]
    max_df=station_df.max()
    maxs = max_df.row(0)
    min_df = station_df.min()
    mins = min_df.row(0)

    record_flat_data(station_df, output_dict)
    record_describe_data(station_df, output_dict)
    record_attr_use_data(station_df, output_dict)
    print(output_dict)

def main():
    output_dicts = {
        'stations_flat': pl.DataFrame([], schema=station_flat_schema),
        'stations_describe': pl.DataFrame([], schema=station_describe_schema, orient='row'),
        'stations_attr_use':  pl.DataFrame([], schema=station_attr_use_schema, orient='row'),
    }
    read_daily_summary_gz(daily_summary_path, process_station_csv, output_dicts, max_stations=1002)

main()

# TODO save dict to disk
# TODO get aggregate statistics for each column from stations_describe - # of stations for each stat
# TODO characterize column usage
# TODO characterize time coverage - add column with % of date range covered
# TODO find unused columns
# TODO figure out WT*, WV*
# TODO characterize attribute usage