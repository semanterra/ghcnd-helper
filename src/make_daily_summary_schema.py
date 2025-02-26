import polars as pl

'''
In theory, this produces a complete schema for a daily summary file.
Some of the datatypes may be off.  

If it's determined later that some fields should be pruned (which are actually present in data)
then do it as post-processing - leave the output of this complete.
'''

def make_schema():
    schema = [
        ('STATION', pl.String),
        ('DATE', pl.Date),
        ('LATITUDE', pl.Float32),
        ('LONGITUDE', pl.Float32),
        ('ELEVATION', pl.Float32),
        ('NAME', pl.String),
    ]

    schema.extend(_make_attributed([
            'PRCP',
            'SNOW', 'SNWD',
            'TMAX', 'TMIN', 'TAVG',
            'ACMC', 'ACMH', 'ACSC', 'ACSH',
            'ADPT', 'ASLP', 'ASTP', 'AWBT',
            'AWDR', 'AWND',
            'DAEV', 'DAPR', 'DASF', 'DATN', 'DATX', 'DAWM', 'DWPR',
            'EVAP',
            'FMTM',
            'FRGB', 'FRGT', 'FRTH',
            'GAHT',
            'MDEV', 'MDPR', 'MDSF', 'MDTN', 'MDTX', 'MDWM',
            'MNPN', 'MXPN',
            'PSUN',
            'RHAV', 'RHMN', 'RHMX',
            'THIC',
            'TOBS',
            'TSUN',
            'WDF1', 'WDF2', 'WDF5', 'WDFG', 'WDFI', 'WDFM',
            'WDMV',
            'WESD', 'WESF',
            'WSF1', 'WSF2', 'WSF5', 'WSFG', 'WSFI', 'WSFM'
        ]))

    schema.extend(_make_attributed([
    'DAEV', 'DAPR', 'DASF', 'DATN', 'DATX', 'DAWM', 'DWPR'
    ], pl.Int32))
    schema.extend(_make_attributed([
        'PGTM'
    ], pl.String))

    schema.extend(_make_soil_temps())
    schema.extend(_make_weather_types())
    schema.extend(_make_weather_vicinity())

    return dict(schema)

def _make_attributed(names, type=pl.Float32):
    for name in names:
        yield ((name, type))
        yield ((name + '_ATTRIBUTES', pl.String))
        yield ((name + '_MEASURE', pl.String))
        yield ((name + '_QUALITY', pl.String))
        yield ((name + '_SOURCE', pl.String))

def _make_soil_temps():
    ret = []
    for prefix in ['SN', 'SX']:
        for ground_cover in '012345678':
            for depth in '012334567':
                ret.append(prefix+ground_cover+depth)
    yield from _make_attributed(ret)

def _make_weather_types():
    ret = ('WT' + f"{i:02d}" for i in range(1,23))
    yield from _make_attributed(ret, pl.String)

def _make_weather_vicinity():
    ret = ('WV' + f"{i:02d}" for i in [1,3,7,18,20])
    yield from _make_attributed(ret, pl.String)

