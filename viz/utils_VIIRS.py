import h5py
import numpy as np
import pandas as pd
from pathlib import Path
import xarray as xr
import hvplot.xarray
import matplotlib.pyplot as plt

# path with VIIRS's retrieval files
path_viirs_retrieve = "data/VIIRS_bash/"
# original bash file 
exe_old = "VIIRS-Greenland-download.sh"
# output bash file 
exe_new = "VIIRS-Greenland-download-filtered.sh"
# lenght of line referring to a VIIRS image
viirs_length_bash = 146


def VIIRS_select(time, minutes, max_viirs):
    """
    Select at most 'max_viirs' files that are close in time to ATL06 data
    Output: bash file with images to retrieve
    
    """
    
    hr = minutes/60
    
    file = open(path_viirs_retrieve + exe_old, 'r')
    new_file = open(path_viirs_retrieve + exe_new, 'w')

    viirs_names = []

    counter = 0
    counter_viirs = 0

    start = time - pd.DateOffset(hours=hr)
    end   = time + pd.DateOffset(hours=hr)


    for line in file:

        if ("https://ladsweb.modaps.eosdis.nasa.gov" in line) and (len(line) == viirs_length_bash):

            if len(viirs_names) < max_viirs:

                Vfile = line[:-1]

                f_t = int(Vfile[-33:-26]+Vfile[-25:-21])
                f_t = pd.to_datetime(f_t, format="%Y%j%H%M")
                if time > f_t - pd.DateOffset(hours=hr) and time < f_t + pd.DateOffset(hours=hr):
                    viirs_names.append(Vfile)
                    new_file.write(Vfile + "\n") 
                    counter_viirs += 1

                counter += 1

        else:

            new_file.write(line)

    print(">>> There are a total of", counter_viirs, "viirs files found out of", counter, "file names.")

    new_file.close()

    
def VIIRS_get(spatial_extent):  
    """
    Select downloaded images that cover area defined by 'spatial_extent'
    Output: list of xarray.DataSet, one for each VIIRS image
    
    """
    VIIRS_images = []
    count = 0
    path = Path('VIIRS_bash')
    for i in path.glob('*.nc'):
        v1 = xr.open_dataset(i, engine="h5netcdf", group='geolocation_data')
        v2 = xr.open_dataset(i, engine="h5netcdf", group= 'geophysical_data')
        v = v1.merge(v2.Integer_Cloud_Mask)
        v= v.set_coords(('latitude','longitude'))
        if ((v.latitude < spatial_extent[3]) & (v.latitude > spatial_extent[1]) & 
                            (v.longitude < spatial_extent[2]) & (v.longitude > spatial_extent[0])).any() != False:
            viirs_new = v.where((v.latitude < spatial_extent[3]) & (v.latitude > spatial_extent[1]) & 
                            (v.longitude < spatial_extent[2]) & (v.longitude > spatial_extent[0]), drop=True)
            count+=1
            print(pd.to_datetime(str(i)[-33:-26]+str(i)[-25:-21], format="%Y%j%H%M"))
            VIIRS_images.append(viirs_new)
    print(str(count) + ' images matching area')  
    
    return(VIIRS_images)