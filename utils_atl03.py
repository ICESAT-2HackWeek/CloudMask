from icepyx import icesat2data as ipd
import numpy as np
import pandas as pd
import os
import h5py

from os import listdir
from os.path import isfile, join

###

# To do:
#       - Remove the emails and do this in a more general way
#       - Use projections in the definition of delta_lat and delta_lon instead of the spherical approximation

###

earthdata_emails = {'tsnow03':'tasha.snow@colorado.edu', \
                    'fperez': 'fernando.perez@berkeley.edu', \
                    'alicecima':'alice_cima@berkeley.edu', \
                    'fsapienza': 'fsapienza@berkeley.edu'}

user = "fsapienza"

### Auxiliar Functions

def delta_lat(lat, lon, delta_m):
    return 180 * delta_m / ( np.pi * 6371000 )

def delta_lon(lat, lon, delta_m):
    return 180 * delta_m / ( np.pi * 6371000 * np.cos(lat * np.pi / 180) )

def filter(string, substr): 
    return [str for str in string if
             any(sub in str for sub in substr)] 

def df_filter (df, my_lat, my_lon, w, lat_col_name = "lat", lon_col_name = "lon"):
    
    window_lat = delta_lat(my_lat, my_lon, w)
    window_lon = delta_lon(my_lat, my_lon, w) 
    
    return df [ (df[lat_col_name] < my_lat + window_lat) & (df[lon_col_name] < my_lon + window_lon) &
                (df[lat_col_name] > my_lat - window_lat) & (df[lon_col_name] > my_lon - window_lon) ]

def file_in_dir(path):
    return [f for f in listdir(path) if isfile(join(path, f))]


### ATL03 Retrieval

def read_atl03 (lat, lon, date_range, delta_m, path = "new_ATL03", extent = None):
    
    """
    Read a ATL03 file based and retieve individual photons in a window around a
    desired latitide, longitud and a range of dates. 
    """
    
    # Spatial extend
    
    if extent == None:

        window_lat = delta_lat(lat, lon, delta_m)
        window_lon = delta_lon(lat, lon, delta_m)
    
        spatial_extent = [ lon - window_lon, lat - window_lat, lon + window_lon, lat + window_lat ]
    
    else:

        spatial_extent = extent 
    
    spatial_extent = [ float(x) for x in spatial_extent ]  # This line has to be remove after solving Issue 82 in Icepyx
    
    # Retreiving the data 
    
    region_a = ipd.Icesat2Data('ATL03', spatial_extent, date_range)
    region_a.avail_granules(ids=True)
    region_a.earthdata_login(user, earthdata_emails[user])
    region_a.order_vars.append(var_list=['lat_ph', "lon_ph", "h_ph"])
    region_a.subsetparams(Coverage=region_a.order_vars.wanted)
    region_a.order_granules()
    
    region_a.download_granules(path)
        
    flist = file_in_dir(path)
    assert len(flist) > 0, "There are not available granules for these parameters. Check that the h5 files were download in path"
        
        
    dataframes = pd.DataFrame(columns = ["h_ph", "lon_ph", "lat_ph", "ground_track"])
    
    for file in flist:
        
        fname = path + "/" + file 
        
        with h5py.File(fname, 'r') as fi: 
            
            for my_gt in filter(fi.keys(), ["gt"]):
    
                lat_ph = fi[my_gt]['heights']["lat_ph"][:]
                lon_ph = fi[my_gt]['heights']["lon_ph"][:]
                h_ph   = fi[my_gt]['heights']["h_ph"][:]

                
                df = pd.DataFrame.from_dict({"h_ph": h_ph,
                                             "lon_ph": lon_ph,
                                             "lat_ph": lat_ph,
                                             "ground_track": [my_gt] * len(h_ph) } )
    
                if extent == None:

                    df = df [ (df["lat_ph"] < lat + window_lat) & (df["lon_ph"] < lon + window_lon) &
                              (df["lat_ph"] > lat - window_lat) & (df["lon_ph"] > lon - window_lon) ]

                else:

                    df = df [ (df["lat_ph"] < extent[3]) & (df["lon_ph"] < extent[2]) &
                              (df["lat_ph"] > extent[1]) & (df["lon_ph"] > extent[0]) ]

                dataframes = dataframes.append(df, ignore_index=True)
    
    return dataframes



def multiple_read_atl03 (requests, earthdata_email = "fsapienza@berkeley.edu", earthdata_uid = "fsapienza", delta_m = 100):
    
    res = {}
    
    for i, req in enumerate(requests): 
        
        df = read_atl03( lat = req["lat"], lon = req["lon"], date_range = req["date_range"], delta_m = delta_m, path = "new_ATL03/file" + str(i+1) )
        
        res[i] = df
        
    return res