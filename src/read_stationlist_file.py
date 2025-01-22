from config import ghcnh_local_doc_path

import polars as pl
from pathlib import Path

'''
ID, 1-11, Character
LATITUDE,13-20, Real LONGITUDE, 22-30, Real ELEVATION, 32-37, Real
STATE, 39-40, Character
NAME, 42-71, Character
GSN FLAG, 73-75, Character HCN/CRN FLAG, 77-79, Character WMO ID, 81-85, Character ------------------------------
These variables have the following definitions:
ID=the station identification code. Note that the first two
characters denote the FIPS country code, the third character is a network code that identifies the station numbering system used, 
and the remaining eight characters contain the actualstation ID.
See "ghcn-countries.txt" for a complete list of country codes.
See "ghcn-states.txt" for a list of state/province/territory codes.
The network code has the following potential values:
A = Retired WMO Identifier used by the USAF 14th Weather Squadron U = unspecified (station identified by up to eight
alphanumeric characters)
C = U.S. Cooperative Network identification number (last six
characters of the GHCN ID)
I = International Civil Aviation Organization (ICAO) identifier M = World Meteorological Organization ID (last five
characters of the GHCN ID)
N = Identification number used by a
National Meteorological or Hydrological Center partner
L = U.S. National Weather Service Location Identifier (NWSLI)
W = WBAN identification number (last five characters of the GHCN ID)
LATITUDE= latitude of the station (in decimal degrees). North (+); South (-) LONGITUDE=the longitude of the station (in decimal degrees). East (+)t; West (-) ELEVATION=the elevation of the station (in meters, missing = -999.9). STATE=the U.S. postal code for the state (for U.S. stations only).
NAME=the name of the station.
GSN FLAG=a flag that indicates whether the station is part of the GCOS Surface Network (GSN). The flag is assigned by cross-referencing the number in the WMO ID field with the official list of GSN stations. There are two possible values:
Blank = non-GSN station or WMO Station number not available GSN = GSN station
HCN/=a flag that indicates whether the station is part of the U.S.
CRN FLAG=Historical Climatology Network (HCN) or U.S. Climate Reference
Network (CRN). There are three possible values:
Blank = Not a member of the U.S. Historical Climatology or U.S. Climate Reference Networks
  
HCN = U.S. Historical Climatology Network station
CRN = U.S. Climate Reference Network or U.S. Regional Climate
Network Station
WMO ID=the World Meteorological Organization (WMO) number for the station. If the station has no WMO number (or one has not yet
been matched to this station), then the field is blank.
'''

def read_stationlist_file():
    df = pl.read_csv(ghcnh_local_doc_path+'ghcnh-station-list.csv', has_header=False,
        columns = [0,1,2,3,4,5,8],
        new_columns = ['ID', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATE', 'NAME', 'WMO_ID'],
        schema_overrides= {
            'LATITUDE''': pl.Float32, 'LONGITUDE': pl.Float32, 'ELEVATION': pl.Float32 },
        null_values= ['', ' ', '  ', '   ', '    ', '     ', '      ',' -999.9']  # -999.9 is elevation null value
    )
    return df

df = read_stationlist_file()
print(df.describe())
print('hello')