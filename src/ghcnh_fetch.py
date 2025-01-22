# fetch ghcnh archive files from 1939-2023.
# The filenames were retrieved Aug 12 2024.
from urllib.request import urlretrieve
from config import ghcnh_filenames, ghcnh_local_path

url_prefix = 'https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/inventory/'

for file in ghcnh_filenames:
    print('downloading ' + file)
    urlretrieve(url_prefix + file, ghcnh_local_path + file)
    print('downloaded ' + file)
