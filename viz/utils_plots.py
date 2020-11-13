import hvplot.pandas
import panel as pn
import panel.widgets as pnw
import pandas as pd

import os
import hvplot.xarray
from holoviews import dim
import holoviews as hv
from holoviews.streams import BoundsX
import xarray as xr
import datetime
import numpy as np
from dask.distributed import LocalCluster, Client
import s3fs
import boto3
import icepyx as ipx
from pathlib import Path
import h5py


def plot_all(data, alp=True, var='h_li'):
    """
    Scatter plot with x=lon, y=lat.
    Interactivity for missing data and color.
    
    """
    # Static plot
    def scatter(data=data, alp=alp, var=var):
        clean = data[data.loc[:,var]<1e+38].hvplot.scatter(x='longitude', y='latitude', c= var, s=1, alpha = 0.2, cmap='viridis', hover=False)
        missing = data[data.loc[:,var]>1e+38].hvplot.scatter(x='longitude', y='latitude', c= 'red', s=1, alpha = 0.2 if alp==True else 0, hover=False)
        return(clean*missing)
    
    # Widgets
    var  = pnw.Select(name='Color', value='h_li', options=list(data.columns))
    alp  = pnw.Checkbox(value=True, name='Missing data (red)')
    text = "<br>\n# All ATL06 data for the selected area\nSelect a color variable and whether to show missing data"
    
    # Interactive plot 
    @pn.depends(var, alp)
    def reactive(var, alp):
        return(scatter(data, alp, var))
    
    # Layout
    widgets = pn.Column(text, var, alp)
    image = pn.Row(reactive, widgets)
    return(image)



def plot_daily(data, day=None, col='ground_track'):
    """
    Panel with daily plots: lat vs lon (map view), and height vs lat (profile view)
    Interactivity on day and color
    
    """
    # Static plot
    def daily_scatter(data=data, day=day, col='ground_track'):
        mapp = data[data.loc[:,'time'].dt.date==day].hvplot.scatter(x='longitude', y='latitude', s=0.2, c='h_li', alpha=0.2, cmap='viridis')
        scatter = data[data.loc[:,'time'].dt.date==day].hvplot.scatter(x='latitude', y='h_li', s=0.2, c=col)
        global lat_bounds
        lat_bounds = BoundsX(source=scatter) # select box to define area for external data
        return(pn.Column(mapp,scatter))
    
    # Widget values
    kw = dict(col=sorted(list(data.columns)), 
              day=data['time'].dt.date.unique())
    
    global im  # used to extract selected day and retrieve external data
    
    # Interactive plot
    im = pn.interact(daily_scatter, **kw)
    
    # Layout
    text = "<br>\n# Daily ATL06 data for the selected area\nSelect a day and a color variable"
    p = pn.Row(pn.Column(text, im[0][1], im[0][0]), im[1][0])
    return(p) 


def plot_DEM_difference(data, histogram='ground_track', scatter='longitude', bins=100, subplots=False):
    """
    Panel to explore pointwise differences with ArcticDEM
    Includes a histogram, a scatter plot, and a summary table
    
    """
    # Static plot
    def difference(data=data, histogram=histogram, scatter=scatter):
        plot_hist = data.hvplot.hist('height_diff', bins=bins, by=histogram, subplots=subplots, alpha=0.5)        
        plot_scat = data.hvplot.scatter(scatter, 'height_diff', size=1, alpha=0.1)
        return(pn.Column(plot_scat,plot_hist))
    
    # Widget values
    kw = dict(histogram=['ground_track','atl06_quality_summary','bsnow_conf', 
                    'bsnow_h', 'bsnow_od', 'cloud_flg_asr',
                    'cloud_flg_atm', 'msw_flag', 'layer_flag'], 
              scatter=['segment_id', 'latitude', 'longitude', 'h_li','n_fit_photons', 
                    'w_surface_window_final', 'n_fit_photons_ratio_w',
                    'bckgrd', 'dem_h', 'geoid_h'])
    
    # Interactive plot
    i = pn.interact(difference, **kw)
    
    # Layout
    df = pd.DataFrame(data.height_diff.describe())
    text = "<br>\n# Difference in estimated height with ArcticDEM"
    p = pn.Row(pn.Column(text, i[0][1], i[0][0], df), pn.Column(i[1][0]))
    return(p) 


def variability(data, window=30):
    """
    Return table with max and mean standard deviation for height 
    computed on specified rolling window 
    
    """  
    rows =[]
    for i in data.time.dt.date.unique():
        for j in data.ground_track.unique():
            std = data.loc[(data.time.dt.date==i)&(data['ground_track']==j)].h_li.rolling(window=window).std()
            rows.append([i, j, std.max(), std.mean()])
    d = pd.DataFrame(rows, columns=['day', 'track', 'max std', 'mean std']).sort_values('max std', ascending=False)
    return(d.hvplot.table(sortable=True, selectable=True))




######### ERA5 #########

def era5(date, hour, spatial_extent):
    """
    Obtain era5 data on wind and temperature for matching spatio-temporal area
    Output: xarray dataset 
    
    """
    # Setup for AWS and Zarr
    era5_bucket = 'era5-pds'
    client = Client()
    fs = s3fs.S3FileSystem(anon=True)
    
    d = pd.to_datetime(date)  
    var = ['air_temperature_at_2_metres', 'eastward_wind_at_100_metres', 
           'eastward_wind_at_10_metres', 'northward_wind_at_100_metres', 
           'northward_wind_at_10_metres']

    year = d.strftime('%Y')
    month = d.strftime('%m')
    
    # List of xarray datasets, one for each variable 
    ds =[]
    for i in var:
        f_zarr = 'era5-pds/zarr/{year}/{month}/data/{var}.zarr/'.format(year=year, month=month, var=i)
        ds.append(xr.open_zarr(s3fs.S3Map(f_zarr, s3=fs)))
    
    # Merge datasets and slice based on given time and spatial extent
    data_area = xr.merge(ds).sel(lon=slice(spatial_extent[0]+360, spatial_extent[2]+360), 
                                 lat=slice(spatial_extent[3], spatial_extent[1]), 
                                 time0=slice(date+'T{hour:02d}'.format(hour=hour), 
                                             date+'T{hour:02d}'.format(hour=hour+1)))
    
    data_area = data_area.rename({'time0': 'Time'})
    data_area['lon'] = data_area['lon']-360
    
    # Needed for vectorfield
    data_area['rad_10'] = np.arctan(data_area.northward_wind_at_10_metres/data_area.eastward_wind_at_10_metres)
    data_area['intensity_10'] = np.sqrt(data_area.northward_wind_at_10_metres**2 + data_area.eastward_wind_at_10_metres**2)
    data_area['rad_100'] = np.arctan(data_area.northward_wind_at_100_metres/data_area.eastward_wind_at_100_metres)
    data_area['intensity_100'] = np.sqrt(data_area.northward_wind_at_100_metres**2 + data_area.eastward_wind_at_100_metres**2)
    
    data_area = data_area.compute()
    return(data_area)


def era5_static(era5_data, hour, wind):
    """
    Static wind vectorfield at specific hour and height (10/100m)
    
    """
    if wind == 10:
        angle = 'rad_10'
        mag = 'intensity_10'
    else:
        angle = 'rad_100'
        mag = 'intensity_100'
    
    return(era5_data.sel(Time=str(im[0][0].value)+'T{hour:02d}'.format(hour=hour)).hvplot.vectorfield(
        x='lon', y='lat', angle=angle, mag=mag, hover=False)).opts(
        magnitude=dim(mag)*0.01, color=mag, colorbar=True, rescale_lengths=False, ylabel='latitude')


def era5_dynamic(era5_data, hour=None, wind=None):
    """
    Interactive wind vectorfield 
    
    """
    wind  = pnw.DiscreteSlider(name='Wind height (meters)', options=[10,100])
    hour  = pnw.DiscreteSlider(name='Hour', options=[min(era5_data.Time.dt.hour).item(),
                                                     max(era5_data.Time.dt.hour).item()])
    text = "<br>\n# ERA5: wind strength and direction\nSelect a time and the wind height"
    temperature = "The average temperature is: " + str(int(era5_data.air_temperature_at_2_metres.mean().values)) +" K."

    @pn.depends(hour, wind)
    def reactive_era5(hour, wind):
        return era5_static(era5_data, hour, wind)

    widgets = pn.Column(text, hour, wind, temperature)
    image = pn.Row(widgets, reactive_era5)
    return(image)


def plot_era5(data):
    """
    Get era5 data on wind and temperature matching ATL06 spatial_extent, 
    and output interactive vectorfield
    
    """    
    # Subset input dataframe based on day and latitude bounds
    data_select=data[(data.loc[:,'time'].dt.date==im[0][0].value) & 
                     (data.loc[:,'latitude'].between(lat_bounds.boundsx[0], 
                                                     lat_bounds.boundsx[1]))]
    # Define spatial extent for ERA5
    area=[round(data_select.longitude.min()-0.5), round(data_select.latitude.min()-0.5), 
          round(data_select.longitude.max()+0.5), round(data_select.latitude.max()+0.5)]
    
    # Extract minimum hour from input dataframe
    h = min(data[data.time.dt.date==im[0][0].value].time.dt.hour)
    
    era5_data = era5(str(im[0][0].value), hour=h, spatial_extent=area)
    return(era5_dynamic(era5_data))





######### ATL03 #########

def filter(string, substr): 
    return [str for str in string if
             any(sub in str for sub in substr)] 

def get_file_in_directory(path): 
    """
    Retrieves file names from a directory \
    \n\nInput: path = directory \
    \n\nOutput: list of subdirectories
    """

    # The last conditional here is in order to ignore the /DS_store file in macs 
    return [os.path.join(path, name) for name in os.listdir(path)
            if (os.path.isfile(os.path.join(path, name)) and (not name.startswith('.')))  ]


def is_file_in_directory(fname, path):
    """
    True if a file with the name 'fname' is in 'path' 
    """
    nfiles = get_file_in_directory(path)
    
    for f in nfiles:
        if fname in f:
            return True
    
    return(False)


def atl03(area, date_range, path, user, email):
    
    """
    Retrieve atl_03 granules corresponding to selected spatio-temporal area
    """

    region_a = ipx.Query('ATL03', spatial_extent=area, date_range=date_range)
    avail_granules = region_a.avail_granules(ids=True)[0]
    print("Available Granules:", avail_granules)
    
    if all([is_file_in_directory(avail_granules[i], path) for i in range(len(avail_granules))]):
        
        print("You have already downloaded all the requested files")
        
    else:

        region_a.earthdata_login(user, email)
        #region_a.order_vars.append(var_list=['lat_ph', "lon_ph", "h_ph"])
        #region_a.subsetparams(Coverage=region_a.order_vars.wanted)
        region_a.order_granules()
        region_a.download_granules(path)
        
    dataframes = pd.DataFrame(columns = ["h_ph", "lon_ph", "lat_ph", "ground_track"])
    
    requested_files = []
    
    for fname_granules in avail_granules:
        file_is_in_folder = False
        for fname_folder in list(Path(path).glob('*.h5')):
            if fname_granules in str(fname_folder):
                requested_files.append(fname_folder)
                file_is_in_folder = True
                break
        if not file_is_in_folder:
            print(fname_granules, "not found")
            
    if len(avail_granules) != len(requested_files):
        print("You are missing some files. There are a total of", len(avail_granules), 
              "available granules but you are accessing", len(requested_files), "h5 files")
        
    
    for file in requested_files:
        
        with h5py.File(file, 'r') as fi: 
            
            for my_gt in filter(fi.keys(), ["gt"]):
    
                lat_ph = fi[my_gt]['heights']["lat_ph"][:]
                lon_ph = fi[my_gt]['heights']["lon_ph"][:]
                h_ph   = fi[my_gt]['heights']["h_ph"][:]

                
                df = pd.DataFrame.from_dict({"h_ph": h_ph,
                                             "lon_ph": lon_ph,
                                             "lat_ph": lat_ph,
                                             "ground_track": [my_gt] * len(h_ph)})

                dataframes = dataframes.append(df, ignore_index=True)
    
    return(dataframes)



def atl03_data(data,
          path = "data/new_ATL03", 
          user = 'alicecima', 
          email = 'alice_cima@berkeley.edu'):
    
    # Subset input dataframe based on day and latitude bounds
    data_select=data[(data.loc[:,'time'].dt.date==im[0][0].value) & 
                     (data.loc[:,'latitude'].between(lat_bounds.boundsx[0], 
                                                     lat_bounds.boundsx[1]))]
    
    # Define spatial extent for ATL03 
    area=[round(data_select.longitude.min()-0.5), round(data_select.latitude.min()-0.5), 
          round(data_select.longitude.max()+0.5), round(data_select.latitude.max()+0.5)]
    
    return(atl03(area, [str(im[0][0].value), str(im[0][0].value)], path, user, email))




######### VIIRS #########

def VIIRS_select(data, 
                 minutes = 120, 
                 max_viirs = 5, 
                 path_in = "data/VIIRS_bash/VIIRS-Greenland-download.sh", 
                 path_out = "data/VIIRS_bash/VIIRS-Greenland-download-filtered.sh"):
    """
    Select at most 'max_viirs' files that are close in time to ATL06 data
    Output: bash file with images to retrieve
    
    """
    
    hr = minutes/60
    
    file = open(path_in, 'r')
    new_file = open(path_out, 'w')

    viirs_names = []

    counter = 0
    counter_viirs = 0
    
    # Extract time from ATL06 selected day
    time = list(data[(data.time.dt.date==im[0][0].value)].time)[0]

    # Define time range for VIIRS
    start = time - pd.DateOffset(hours=hr)
    end   = time + pd.DateOffset(hours=hr)


    for line in file:

        if ("https://ladsweb.modaps.eosdis.nasa.gov" in line) and (len(line) == 146):

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


def VIIRS_get(data, path="data/VIIRS_bash"):  
    """
    Select downloaded images that cover area defined by 'spatial_extent'
    Output: list of xarray.DataSet, one for each VIIRS image
    
    """
    # Subset input dataframe based on day and latitude bounds
    data_select=data[(data.loc[:,'time'].dt.date==im[0][0].value) & 
                     (data.loc[:,'latitude'].between(lat_bounds.boundsx[0], 
                                                     lat_bounds.boundsx[1]))]
    # Define spatial extent for VIIRS
    spatial_extent=[round(data_select.longitude.min()-0.5), round(data_select.latitude.min()-0.5), 
                    round(data_select.longitude.max()+0.5), round(data_select.latitude.max()+0.5)]
    
    VIIRS_images = []
    count = 0
    path = Path(path)
    
    print('ATL06 data collected at ' + str(list(data[(data.time.dt.date==im[0][0].value)].time)[0]))
    
    # Create list of xarray datasets, one for each VIIRS image that matches the ATL06 area
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
