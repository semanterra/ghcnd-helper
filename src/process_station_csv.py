
from ghcnd_config import daily_summary_path, daily_summary_output_dir

import polars as pl
import warnings
from constants import DfName

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
    'NULL_COUNT': pl.Int32,
    'FIRST_DATE': pl.Date,
    'LAST_DATE': pl.Date,
}

attr_type_enum = pl.Enum(['MEASURE','QUALITY','SOURCE'])

station_attr_use_schema = {
    'STATION': pl.String,
    'OBVALUE': column_enum,
    'ATTR': attr_type_enum,
    'VALUE':pl.String,  # value within attribute to count
    'COUNT':pl.Int32,
}

station_wtwv_schema = {
    'STATION': pl.String,
    'COLUMN': pl.String,
    'VALUE':pl.String,  # value within attribute to count
    'COUNT':pl.UInt32,
}

station_hist_schema = {
    'STATION': pl.String,
    'DECADE': pl.Date,
    'COLUMN': column_enum,
    'COUNT': pl.UInt32,
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
        output_dict[DfName.stations_flat].vstack(station_flat_df, in_place=True)


    def record_describe_data(station_df, output_dict, station_obvalues):

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

        no_date_schema = dict(station_describe_schema)
        del no_date_schema['FIRST_DATE']
        del no_date_schema['LAST_DATE']

        station_describe_df = pl.DataFrame( station_describe_data, schema=no_date_schema)

        first_last_dfs = list(map(lambda obvalue:
                             station_df.drop_nulls([obvalue]).select(
                                 pl.lit(obvalue).cast(column_enum).alias('COLUMN'),
                                 pl.first('DATE').alias('FIRST_DATE'),
                                 pl.last('DATE').alias('LAST_DATE')
                             ), station_obvalues))
        if len(first_last_dfs):
            first_last_df = pl.concat(first_last_dfs)
            station_describe_df = station_describe_df.join(first_last_df, on='COLUMN', how='left',coalesce = True)
        else:
            station_describe_df = station_describe_df.with_columns(FIRST_DATE=pl.lit(None), LAST_DATE=pl.lit(None))
        output_dict[DfName.stations_describe].vstack(station_describe_df, in_place=True)


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

                    if output_dict[DfName.stations_attr_use].height:
                        output_dict[DfName.stations_attr_use].vstack(value_count_df, in_place=True)
                    else:
                        output_dict[DfName.stations_attr_use] = value_count_df

    def record_hist_data(station_df, output_dict):
        station_hist = (station_df.group_by_dynamic(
            index_column='DATE',
            every='10y',
            )
            .agg(pl.col(station_obvalues).count())
            .unpivot(index='DATE')
            .rename({'variable':'COLUMN'})
            .with_columns(COLUMN=pl.col('COLUMN').cast(column_enum))
            .rename({'value':'COUNT', 'DATE':'DECADE'})
            .insert_column(0, pl.lit(station).alias("STATION"))
        )
        output_dict[DfName.stations_hist].vstack(station_hist, in_place=True)

    def record_wtwv_data(station_df, output_dict, station_obvalues):
        # get list of WT and WV obvalues
        station_wtwv_obvalues = list(filter(lambda col: col.startswith('WT') or col.startswith('WV'), station_obvalues))
        if len(station_wtwv_obvalues) == 0:
            return

        wtwv_rows = (station_df
            .select(pl.col(station_wtwv_obvalues))
            .filter(~pl.all_horizontal(pl.col(station_wtwv_obvalues).is_null()))
                     )

        counts_per_col = []
        for col in station_wtwv_obvalues:
            value_counts = wtwv_rows.select(pl.col(col).value_counts())
            col_counts = (value_counts.select(pl.col(col)
                                           .struct.rename_fields(['VALUE', 'COUNT'])
                                           .struct.unnest())
                        .drop_nulls()
                        .insert_column(0, pl.lit(col).alias('COLUMN')))
            counts_per_col.append(col_counts)

        station_wtwv_df = (pl.concat(counts_per_col)
                           .insert_column(0,pl.lit(station).alias('STATION'))
                           )
        output_dict[DfName.stations_wtwv].vstack(station_wtwv_df, in_place=True)

    '''
       MAINLINE of process_station_csv
    
        Some inner functions use the variables defined below - watch out!
    '''

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
    record_describe_data(station_df, output_dict, station_obvalues)
    record_attr_use_data(station_df, output_dict)
    record_hist_data(station_df, output_dict)
    record_wtwv_data(station_df, output_dict, station_obvalues)

def main():
    output_dicts = {
        DfName.stations_flat:      pl.DataFrame([], schema=station_flat_schema),
        DfName.stations_describe:  pl.DataFrame([], schema=station_describe_schema, orient='row'),
        DfName.stations_attr_use:  pl.DataFrame([], schema=station_attr_use_schema, orient='row'),
        DfName.stations_hist:      pl.DataFrame([], schema=station_hist_schema, orient='row'),
        DfName.stations_wtwv:      pl.DataFrame([], schema=station_wtwv_schema, orient='row'),
    }
    read_daily_summary_gz(daily_summary_path, process_station_csv, output_dicts)
    print('writing')
    for name, df in output_dicts.items():
        df.write_parquet(daily_summary_output_dir + name + '.parquet')


# TODO get aggregate statistics for each column from stations_describe - # of stations for each stat
# TODO characterize column usage
# TODO characterize time coverage - add column with % of date range covered
# TODO find unused columns
# TODO figure out WT*, WV*
