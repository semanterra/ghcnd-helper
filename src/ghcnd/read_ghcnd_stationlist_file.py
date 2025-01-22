from ghcnd_config import ghcnd_local_stations_list

import polars as pl
from pathlib import Path



'''
The stationlist file is much larger than the ghcnh similar file, 
and is only available in a fixed-field .txt format - no .csv.

The following is from the readme.txt:
IV. FORMAT OF "ghcnd-stations.txt"

------------------------------
Variable   Columns   Type
------------------------------
ID            1-11   Character
LATITUDE     13-20   Real
LONGITUDE    22-30   Real
ELEVATION    32-37   Real
STATE        39-40   Character
NAME         42-71   Character
GSN FLAG     73-75   Character
HCN/CRN FLAG 77-79   Character
WMO ID       81-85   Character
------------------------------

These variables have the following definitions:

ID         is the station identification code.  Note that the first two
           characters denote the FIPS  country code, the third character 
           is a network code that identifies the station numbering system 
           used, and the remaining eight characters contain the actual 
           station ID. 

           See "ghcnd-countries.txt" for a complete list of country codes.
	   See "ghcnd-states.txt" for a list of state/province/territory codes.

           The network code  has the following five values:

           0 = unspecified (station identified by up to eight 
	       alphanumeric characters)
	   1 = Community Collaborative Rain, Hail,and Snow (CoCoRaHS)
	       based identification number.  To ensure consistency with
	       with GHCN Daily, all numbers in the original CoCoRaHS IDs
	       have been left-filled to make them all four digits long. 
	       In addition, the characters "-" and "_" have been removed 
	       to ensure that the IDs do not exceed 11 characters when 
	       preceded by "US1". For example, the CoCoRaHS ID 
	       "AZ-MR-156" becomes "US1AZMR0156" in GHCN-Daily
           C = U.S. Cooperative Network identification number (last six 
               characters of the GHCN-Daily ID)
	   E = Identification number used in the ECA&D non-blended
	       dataset
	   M = World Meteorological Organization ID (last five
	       characters of the GHCN-Daily ID)
	   N = Identification number used in data supplied by a 
	       National Meteorological or Hydrological Center
           P = "Pre-Coop" (an internal identifier assigned by NCEI for station
               records collected prior to the establishment of the U.S. Weather
               Bureau and their management of the U.S. Cooperative (Coop) 
               Observer Program
	   R = U.S. Interagency Remote Automatic Weather Station (RAWS)
	       identifier
	   S = U.S. Natural Resources Conservation Service SNOwpack
	       TELemtry (SNOTEL) station identifier
           W = WBAN identification number (last five characters of the 
               GHCN-Daily ID)

LATITUDE   is latitude of the station (in decimal degrees).

LONGITUDE  is the longitude of the station (in decimal degrees).

ELEVATION  is the elevation of the station (in meters, missing = -999.9).


STATE      is the U.S. postal code for the state (for U.S. stations only).

NAME       is the name of the station.

GSN FLAG   is a flag that indicates whether the station is part of the GCOS
           Surface Network (GSN). The flag is assigned by cross-referencing 
           the number in the WMOID field with the official list of GSN 
           stations. There are two possible values:

           Blank = non-GSN station or WMO Station number not available
           GSN   = GSN station 

HCN/      is a flag that indicates whether the station is part of the U.S.
CRN FLAG  Historical Climatology Network (HCN) or U.S. Climate Refererence
          Network (CRN).  There are three possible values:

           Blank = Not a member of the U.S. Historical Climatology 
	           or U.S. Climate Reference Networks
           HCN   = U.S. Historical Climatology Network station
	   CRN   = U.S. Climate Reference Network or U.S. Regional Climate 
	           Network Station

WMO ID     is the World Meteorological Organization (WMO) number for the
           station.  If the station has no WMO number (or one has not yet 
	   been matched to this station), then the field is blank.  
 
'''

def read_stationlist_file():
    # ID            1-11   Character
    # LATITUDE     13-20   Real
    # LONGITUDE    22-30   Real
    # ELEVATION    32-37   Real
    # STATE        39-40   Character
    # NAME         42-71   Character
    # GSN FLAG     73-75   Character
    # HCN/CRN FLAG 77-79   Character
    # WMO ID       81-85   Character

    column_specs = [(0,11), (12,20), (21,30), (31,37), (38,40), (41,71), (72,75), (76,79), (80,85)]
    schema = [('ID',pl.String),
              ('LATITUDE',pl.Float64), ( 'LONGITUDE',pl.Float64), ( 'ELEVATION',pl.Float64),
              ( 'STATE',pl.String), ( 'NAME',pl.String),
              ( 'GSN_FLAG',pl.String), ( 'HCN_CRN_FLAG',pl.String),]
    is_float_col = [False, True, True, True, False, False, False, False]

    # Read the fixed-field file

    with open(ghcnd_local_stations_list, "r") as file:
        lines = file.readlines()

    # Process each line to extract the fields
    data = []
    for line in lines:
        start = 0
        row = []
        for iCol in range(len(column_specs)):
            begin,end = column_specs[iCol]

            field = line[begin:end].strip()  # Extract and strip spaces
            if is_float_col[iCol]:
                field = float(field)
            row.append(field)
        data.append(row)

    # Create a Polars DataFrame
    df = pl.DataFrame(data, schema=schema)
    return df

df = read_stationlist_file()

# Print the DataFrame
print(df.describe())
print(df.head())

