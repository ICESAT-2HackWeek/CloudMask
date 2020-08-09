import h5py
import os
import numpy as np
import pandas as pd
from pathlib import Path
from icepyx import query as ipd
#from icepyx import icesat2data as ipd



from utils import *


def read_atl06_fromfile(fname, outdir='data', bbox=None):
    """
    Read one ATL06 file and output 6 reduced files. 
    
    Extract variables of interest and separate the ATL06 file 
    into each beam (ground track) and ascending/descending orbits.
    """

    # Each beam is a group
    group = ['/gt1l', '/gt1r', '/gt2l', '/gt2r', '/gt3l', '/gt3r']
    
    # Loop trough beams
    dataframes = []  # one dataframe per track
    
    with h5py.File(fname, 'r') as fi:
    
        # Check which ground tracks are present in this file
        gtracks = sorted(['/'+k for k in fi.keys() if k.startswith('gt')])

        for k, g in enumerate(gtracks): 
            
            # Read in data for a single beam
            data = {}
            
            # Put it first in the dict for column ordering:
            data['ground_track'] = None 
            data['t_year'] = None # difference between this and np.nan?
            data['segment_id'] = fi[g+'/segment_quality/segment_id'][:]
            
            # Ground Track
            npts = len(data['segment_id']) # we use the number of segment_id as reference for the total number of measuraments
            data['ground_track'] = [g[1:]] * npts
            
            # Time
            delta_t = fi[g+'/land_ice_segments/delta_time'][:]     # for time conversion
            t_ref = fi['/ancillary_data/atlas_sdp_gps_epoch'][:]     # single value
            t_gps = t_ref + delta_t  # Time in GPS seconds (secs since Jan 5, 1980)
            data['t_year'] = gps2dyr(t_gps) # GPS sec to datetime

            
            # Load vars into memory (include as many as you want)
            data['lat'] = fi[g+'/land_ice_segments/latitude'][:]
            data['lon'] = fi[g+'/land_ice_segments/longitude'][:]
            data['h_li'] = fi[g+'/land_ice_segments/h_li'][:]
            data['s_li'] = fi[g+'/land_ice_segments/h_li_sigma'][:]
            data['q_flag'] = fi[g+'/land_ice_segments/atl06_quality_summary'][:]
            data['s_fg'] = fi[g+'/land_ice_segments/fit_statistics/signal_selection_source'][:]
            data['snr'] = fi[g+'/land_ice_segments/fit_statistics/snr_significance'][:]
            data['h_rb'] = fi[g+'/land_ice_segments/fit_statistics/h_robust_sprd'][:]
            data['dh_fit_dx'] = fi[g+'/land_ice_segments/fit_statistics/dh_fit_dx'][:]
            data['b_snow_conf'] = fi[g+'/land_ice_segments/geophysical/bsnow_conf'][:]
            data['c_flg_asr'] = fi[g+'/land_ice_segments/geophysical/cloud_flg_asr'][:]
            data['c_flg_atm'] = fi[g+'/land_ice_segments/geophysical/cloud_flg_atm'][:]
            data['msw'] = fi[g+'/land_ice_segments/geophysical/msw_flag'][:]
            data['bsnow_h'] = fi[g+'/land_ice_segments/geophysical/bsnow_h'][:]
            data['bsnow_od'] = fi[g+'/land_ice_segments/geophysical/bsnow_od'][:]
            data['layer_flag'] = fi[g+'/land_ice_segments/geophysical/layer_flag'][:]
            data['bckgrd'] = fi[g+'/land_ice_segments/geophysical/bckgrd'][:]
            data['e_bckgrd'] = fi[g+'/land_ice_segments/geophysical/e_bckgrd'][:]
            data['n_fit_photons'] = fi[g+'/land_ice_segments/fit_statistics/n_fit_photons'][:]
            #data['end_geoseg'] = fi['/ancillary_data/end_geoseg'][:]
            data['w_surface_window_final'] = fi[g+'/land_ice_segments/fit_statistics/w_surface_window_final'][:]

            #print("date", fi["ancillary_data/data_end_utc"][:])
            
            '''
            ***Continue adding columns for these vars, repeated as above. Need to find group/file 
               structure for each in https://nsidc.org/sites/nsidc.org/files/technical-references/ICESat2_ATL06_data_dict_v003.pdf):
               cloud_flg_asr, cloud_flg_atm, msw_flag, bsnow_h, bsnow_od, layer_flag, bckgrd, 
               e_bckgrd, n_fit_photons, end_geoseg, segment_id, w_surface_window_final
            ''' 

            # Before adding this data to the dataframes, we check that there is no missing data. 
            #  1) There are cases where there is no data associated to a vaiable
            #  2) There are cases where the number of measuraments in different variable is different, and then 
            #     it is impossible to store everything in a dataframes if we don't know which measurament is missing
            # After this check, we add the data to the full dataframes
            
            if not all( [ isinstance(data[x], (list, np.ndarray)) for x in data.keys() ] ):
                print("Missing Data: There is no list stored for the variable ...")
                continue
                
            if not all( [ len(data[x]) == len(data['segment_id']) for x in data.keys() ] ):
                print("Missing Data: There are {len(data['segment_id'])} different segments in this selection but ... elements associated to the varirable ..." )
                continue
                    
            dataframes.append(pd.DataFrame.from_dict(data))
        
    return dataframes




def atl06_2_df(files):
    """
    Return a single Pandas dataframe from a list of HDF5 ATL06 data files.
    """
    
    dataframes = []
    for f in files:
        dataframes.extend(read_atl06_fromfile(f))
    
    ndfs = len(dataframes)
    i = 0
    
    # pd.concat can only work with up to 10 dataframes at a time,
    # so we need to chunk this up
    new_dfs = []
    while i <= ndfs:
        i_end = i+10 if i+10 < ndfs else ndfs
        dfs = dataframes[i:i_end]
        if not dfs:
            break
        new_dfs.append(pd.concat(dfs))
        i = i_end
    
    return pd.concat(new_dfs)





def read_atl06 (spatial_extent, 
                date_range, 
                time_start = "00:00:00",
                time_end = "23:59:59",
                path = "./new_ATL06", 
                user = 'fsapienza', 
                email = 'fsapienza@berkeley.edu'):
    
    region_a = ipd.Query("ATL06", spatial_extent, date_range, start_time = time_start, end_time = time_end)
    #region_a = ipd.Icesat2Data("ATL06", spatial_extent, date_range, start_time = time_start, end_time = time_end)

    # The last update of icepyx returns a dictonary with the total number of granules and also a list of one element that contains
    # a list with the name of the available granules. This is the reason why we have [0] at the end of the next line.
    # - check if this is always the case. For now, I will add an assert. 
    
    avail_granules = region_a.avail_granules(ids=True)[0]
    print("Available Granules:", avail_granules)

    assert region_a.avail_granules()['Number of available granules'] == len(avail_granules), "The number of avail granules does not match"
    
    if len(avail_granules) == 0:
        print("No granules for this specification")
        return None
    
    
    ### If I already donwload these files, I don't need to do it again
    
    # I should check first that such directory exists!
    
    if all( [ is_file_in_directory(avail_granules[i], path) for i in range(len(avail_granules)) ] ):
        
        print("You already donwload all the requiered files")
        
    else:
    
        region_a.earthdata_login(user, email)
        #region_a.order_vars.avail(options=True)
        region_a.order_vars.append(var_list=['latitude','longitude','h_li','h_li_sigma','atl06_quality_summary','delta_time',
                                             'signal_selection_source','snr_significance','h_robust_sprd','dh_fit_dx','bsnow_conf',
                                             'cloud_flg_asr','cloud_flg_atm','msw_flag','bsnow_h','bsnow_od','layer_flag','bckgrd',
                                             'e_bckgrd','n_fit_photons','end_geoseg','segment_id','w_surface_window_final'])
        region_a.subsetparams(Coverage=region_a.order_vars.wanted)
        region_a.order_granules()
        region_a.download_granules(path)
    
    
    ### The files I need to read should coincide with the requested granueles
    
    requested_files = []
    
    for fname_granules in avail_granules:
        file_is_in_folder = False
        for fname_folder in list(Path(path).glob('*.h5')):
            if fname_granules in str(fname_folder):
                requested_files.append(fname_folder)
                file_is_in_folder = True
                break
        if not file_is_in_folder:
            print( fname_granules, " not found")
            
    if len(avail_granules) != len(requested_files):
        print("You are missing some files. There are a total of", len(avail_granules), "available granules but you are accessing", len(requested_files), "h5 files")
        
    
    dataf = atl06_2_df(requested_files)
    #dataf = atl06_2_df(Path(path).glob('*.h5'))
    
    # reset index
    dataf = dataf.reset_index()
    del dataf['index']
    
    return dataf