import polars as pl

pl.Config.set_float_precision(1)

ghcnh_filenames = [
    'ghcn-hourly_v1.0.0_d1970_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1971_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1972_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1973_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1974_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1975_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1976_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1977_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1978_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1979_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1980_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1981_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1982_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1983_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1984_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1985_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1986_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1987_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1988_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1989_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1990_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1991_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1992_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1993_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1994_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1995_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1996_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1997_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1998_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d1999_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2000_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2001_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2002_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2003_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2004_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2005_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2006_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2007_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2008_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2009_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2010_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2011_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2012_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2013_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2014_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2015_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2016_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2017_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2018_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2019_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2020_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2021_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2022_c20240709-inv.tar.gz',
    'ghcn-hourly_v1.0.0_d2023_c20240709-inv.tar.gz',
    ]

ghcnh_year_to_filename_dict = {int(fn.split('_')[2][1:]):fn for fn in ghcnh_filenames}
_ghcnh_year_minmax =(min(ghcnh_year_to_filename_dict.keys()), max(ghcnh_year_to_filename_dict.keys()))
ghcnh_year_range = range(_ghcnh_year_minmax[0], _ghcnh_year_minmax[1] + 1)
ghcnh_local_path = '/Users/estaub/climatex/ghcnh/hourlyZips/20240709/'
ghcnh_local_doc_path = '/Users/estaub/ghcnh/doc/'
pressure_slice_path = '/Users/estaub/ghcnh/cache/pressure_slice/'
# warning using years before 1970 requires code changes to avoid Unix 1970 beginning of time
# warning 2023 observations are incomplete, probably most end around May
pressure_year_ranges = (range(1970, 1975), range(2018, 2023))
