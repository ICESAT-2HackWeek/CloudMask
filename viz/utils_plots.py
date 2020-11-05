import hvplot.pandas
import panel as pn
import panel.widgets as pnw
import pandas as pd


import os
import hvplot.xarray
from holoviews import dim
import xarray as xr
import datetime
import numpy as np
from dask.distributed import LocalCluster, Client
import s3fs
import boto3



def plot_all(data, alp=True, var='height'):
    """
    Scatter plot with x=lon, y=lat.
    Interactivity for missing data and color.
    
    """
    def scatter(data=data, alp=alp, var=var):
        clean = data[data.loc[:,var]<1e+38].hvplot.scatter(x='lon', y='lat', c= var, s=1, alpha = 0.2, cmap='viridis', hover=False)
        missing = data[data.loc[:,var]>1e+38].hvplot.scatter(x='lon', y='lat', c= 'red', s=1, alpha = 0.2 if alp==True else 0, hover=False)
        return(clean*missing)
    var  = pnw.Select(name='Color', value='height', options=list(data.columns))
    alp  = pnw.Checkbox(value=True, name='Missing data (red)')
    text = "<br>\n# All ATL06 data for the selected area\nSelect a color variable and whether to show missing data"
    @pn.depends(var, alp)
    def reactive(var, alp):
        return(scatter(data, alp, var))
    widgets = pn.Column(text, var, alp)
    image = pn.Row(reactive, widgets)
    return(image)



def plot_daily(data, day=None, col='ground_track'):
    """
    Panel with daily plots: lat vs lon (map view), and height vs lat (profile view)
    Interactivity on day and color
    
    """
    def daily_scatter(data=data, day=day, col='ground_track'):
        mapp = data[data.loc[:,'time'].dt.date==day].hvplot.scatter(x='lon', y='lat', s=1, c='height', alpha=0.2, cmap='viridis')
        scatter = data[data.loc[:,'time'].dt.date==day].hvplot.scatter(x='lat', y='height', s=1, c=col)
        return(pn.Column(mapp,scatter))
    kw = dict(col=sorted(list(data.columns)), 
              day=data['time'].dt.date.unique())
    
    # Used to extract info on selected day
    global im
    
    im = pn.interact(daily_scatter, **kw)
    text = "<br>\n# Daily ATL06 data for the selected area\nSelect a day and a color variable"
    p = pn.Row(pn.Column(text, im[0][1], im[0][0]), im[1][0])
    return(p) 


def DEM_difference(data, histogram='ground_track', scatter='segment_id', bins=100, subplots=False):
    """
    Panel to explore pointwise differences with ArcticDEM
    Includes a histogram, a scatter plot, and a summary table
    
    """
    def difference(data=data, histogram=histogram, scatter=scatter):
        plot_hist = data.hvplot.hist('height_diff', bins=bins, by=histogram, subplots=subplots, alpha=0.5)        
        plot_scat = data.hvplot.scatter(scatter, 'height_diff', size=1, alpha=0.1)
        return(pn.Column(plot_scat,plot_hist))
    kw = dict(histogram=['ground_track','q_flag','blowing_snow_conf', 
                    'blowing_snow_h', 'blowing_snow_od', 'c_flg_asr',
                    'c_flg_atm', 'msw', 'layer_flag'], 
              scatter=['segment_id', 'lat', 'lon', 'height','n_fit_photons', 
                    'w_surface_window_final', 'n_fit_photons_ratio_w',
                    'background','dem_h', 'geoid_h'])
    i = pn.interact(difference, **kw)
    df = pd.DataFrame(data.height_diff.describe())
    text = "<br>\n# Difference in estimated height with Arctic DEM"
    p = pn.Row(pn.Column(text, i[0][1], i[0][0], df), pn.Column(i[1][0]))
    return(p) 





def era5(date, hour, spatial_extent):
    """
    Obtain era5 data on wind and temperature for matching spatio-temporal area
    Output: xarray dataset 
    
    """
    era5_bucket = 'era5-pds'
    client = Client()
    fs = s3fs.S3FileSystem(anon=True)
    
    d = pd.to_datetime(date)  
    var = ['air_temperature_at_2_metres', 'eastward_wind_at_100_metres', 
           'eastward_wind_at_10_metres', 'northward_wind_at_100_metres', 
           'northward_wind_at_10_metres']

    year = d.strftime('%Y')
    month = d.strftime('%m')
    ds =[]

    for i in var:
        f_zarr = 'era5-pds/zarr/{year}/{month}/data/{var}.zarr/'.format(year=year, month=month, var=i)
        ds.append(xr.open_zarr(s3fs.S3Map(f_zarr, s3=fs)))
    
    data_area = xr.merge(ds).sel(lon=slice(spatial_extent[0]+360, spatial_extent[2]+360), 
                                 lat=slice(spatial_extent[3], spatial_extent[1]), 
                                 time0=slice(date+'T{hour:02d}'.format(hour=hour), 
                                             date+'T{hour:02d}'.format(hour=hour+1)))
    
    data_area = data_area.rename({'time0': 'Time'})
    data_area['lon'] = data_area['lon']-360
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
        magnitude=dim(mag)*0.01, color=mag, colorbar=True, rescale_lengths=False, ylabel='lat')


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
    
    
def plot_era5(data, spatial_extent):
    """
    Get era5 data on wind and temperature matching ATL06 spatial_extent, 
    and output interactive vectorfield
    
    """    
    h = min(data[data.time.dt.date==im[0][0].value].time.dt.hour)
    era5_data = era5(str(im[0][0].value), hour=h, spatial_extent=spatial_extent)
    return(era5_dynamic(era5_data))


def variability(data, window=30):
    """
    Return table with max and mean standard deviation for height 
    computed on specified rolling window 
    
    """  
    rows =[]
    for i in data.time.dt.date.unique():
        for j in data.ground_track.unique():
            std = data.loc[(data.time.dt.date==i)&(data['ground_track']==j)].height.rolling(window=window).std()
            rows.append([i, j, std.max(), std.min()])
    d = pd.DataFrame(rows, columns=['day', 'track', 'max std', 'mean std']).sort_values('max std', ascending=False)
    return(d.hvplot.table(sortable=True, selectable=True))