import h5py
import os
import numpy as np
import pandas as pd
from pathlib import Path

import icepyx as ipx
#from icepyx import query as ipd
#from icepyx import icesat2data as ipd

from utils import *

## To do:
## - Add column for number of track and beam. Find which one is the weak and strong beam

def read_atl06_fromfile(fname, add_flags=True, outdir='data', bbox=None):
    """
    Read one ATL06 file and output a single dataframes. 
    
    Extract variables of interest and separate the ATL06 file 
    into each beam (ground track) and ascending/descending orbits.
    """
    
    print('>>> ', fname)

    # Each beam is a group
    group = ['/gt1l', '/gt1r', '/gt2l', '/gt2r', '/gt3l', '/gt3r']
    
    # Loop trough beams
    dataframes = []  # one dataframe per track
    
    with h5py.File(fname, 'r') as fi:
                    
        # Check which ground tracks are present in this file
        gtracks = sorted(['/'+k for k in fi.keys() if k.startswith('gt')])

        for k, g in enumerate(gtracks): 
            
            try:
            
                # Read in data for a single beam
                data = {}

                # Put it first in the dict for column ordering:
                data['ground_track'] = None 
                data['time'] = None # difference between this and np.nan?
                data['segment_id'] = fi[g+'/segment_quality/segment_id'][:]

                # Ground Track
                npts = len(data['segment_id']) # we use the number of segment_id as reference for the total number of measuraments
                data['ground_track'] = [g[1:]] * npts
                
                # In order to know which one is the strong and weak beam, we need to know the orientation of tha spacecraft. ICESat-2 operates 
                # between two different modes: forward and backward. For each one of this modes, the strong and weak will correspond to left and right 
                # in a different way. For more details, see Section 7.5 in ATL03 Manual. 
                icesat_orientation = fi['/orbit_info/sc_orient'][:][0]
                reference_track = g[-1]
                if (icesat_orientation == 0 and reference_track == 'l') or (icesat_orientation == 1 and reference_track == 'r'):
                    data['beam_strength'] = ['strong'] * npts
                else:
                    data['beam_strength'] = ['weak'] * npts

                # Time
                delta_t = fi[g+'/land_ice_segments/delta_time'][:]     # for time conversion
                t_ref = fi['/ancillary_data/atlas_sdp_gps_epoch'][:]     # single value
                t_gps = t_ref + delta_t  # Time in GPS seconds (secs since Jan 6, 1980)
                
                # There are two different ways to obtain a time object from the timestamp. time_2 is 19sec ahead of time
                data['time'] = [time_from_GPS(t) for t in t_gps]
                data['time_2'] = gps2dyr(t_gps) # GPS sec to datetime
                
                ### General ###

                # Latitude
                data['latitude'] = fi[g+'/land_ice_segments/latitude'][:]
                # Longitude
                data['longitude'] = fi[g+'/land_ice_segments/longitude'][:]
                # Standard land-ice segment height determined by land ice algorithm with bias corrections
                data['h_li'] = fi[g+'/land_ice_segments/h_li'][:]            
                # Along-track slope from along-track segment fit
                data['dh_fit_dx'] = fi[g+'/land_ice_segments/fit_statistics/dh_fit_dx'][:]
                # Across-track slepe from weak and strong beam
                data['dh_fit_dy'] = fi[g+'/land_ice_segments/fit_statistics/dh_fit_dy'][:]
                # Signal-to-noise ratio in the final refined window
                data['snr'] = fi[g+'/land_ice_segments/fit_statistics/snr'][:]

                ### Flags ###

                # Quality flag. Zero indicates that no data-quality tests have found a problem 
                data['atl06_quality_summary'] = fi[g+'/land_ice_segments/atl06_quality_summary'][:]
                # The quality summary will be desactivated (=0) if all the following conditions are satisfied
                # 1) h_robust_spread < 1 (meters)
                # Robust dispersion estimator of misfit between photon events heights and the along track segment fit
                data['h_robust_sprd'] = fi[g+'/land_ice_segments/fit_statistics/h_robust_sprd'][:]
                # 2) h_li_sigma < 1 (meters)
                # Propagation error due to sample error 
                data['h_li_sigma'] = fi[g+'/land_ice_segments/h_li_sigma'][:]            
                # 3) snr_significance < 0.02
                # Probability that signal-finding routine would converge to at least the observed signal-to-noise (SNR) for a random
                # noise input. 
                data['snr_significance'] = fi[g+'/land_ice_segments/fit_statistics/snr_significance'][:]
                # 4) Signal_selection_source \in {0,1}
                # this variable takes values in {0,1,2,3} based on which algorithms was used to compute the heights, if possible
                data['signal_selection_source'] = fi[g+'/land_ice_segments/fit_statistics/signal_selection_source'][:]
                # 5) n_fit_photons / W_surface_window_final > 1 Photon events / meter (weak beam)
                #    n_fit_photons / W_surface_window_final > 4 Photon events / meter (strong beam)
                data['n_fit_photons'] = fi[g+'/land_ice_segments/fit_statistics/n_fit_photons'][:]
                data['w_surface_window_final'] = fi[g+'/land_ice_segments/fit_statistics/w_surface_window_final'][:]            
                data['n_fit_photons_ratio_w'] = data['n_fit_photons'] / data['w_surface_window_final']

                ### Blowing Snow ###

                # Confidence flag from presence of blowing snow. 0 = Clear with high confidence, 1 = clear with medium confidence
                data['bsnow_conf'] = fi[g+'/land_ice_segments/geophysical/bsnow_conf'][:]
                # Blowing snow layer top height. This is zero in cases where such a layer is not detected (b_snow_conf = 1)
                data['bsnow_h'] = fi[g+'/land_ice_segments/geophysical/bsnow_h'][:]
                # Optical Tickness of blowing snow layer
                data['bsnow_od'] = fi[g+'/land_ice_segments/geophysical/bsnow_od'][:]

                ### Cloud= Flags ###
                # Cloud flag (probably) from apparent surface reflectance based on ATL09
                data['cloud_flg_asr'] = fi[g+'/land_ice_segments/geophysical/cloud_flg_asr'][:]
                # Number of layers found from the backscatter profile using DDA layer finder
                data['cloud_flg_atm'] = fi[g+'/land_ice_segments/geophysical/cloud_flg_atm'][:]

                ### More Flags ###

                # Multiple scattering warning flag
                data['msw_flag'] = fi[g+'/land_ice_segments/geophysical/msw_flag'][:]
                # Consolidated cloud flag (combination of cloud_flg_atm, cloud_flg_asr, bsnow_conf) and it takes daytime/nightime into consideration
                # 0 = likely abscence of clouds or blowing snow
                data['layer_flag'] = fi[g+'/land_ice_segments/geophysical/layer_flag'][:]

                ### Backgrounds Photons ###

                # background count rate
                data['bckgrd'] = fi[g+'/land_ice_segments/geophysical/bckgrd'][:]
                # Expected background count rate
                data['e_bckgrd'] = fi[g+'/land_ice_segments/geophysical/e_bckgrd'][:]


                #data['end_geoseg'] = fi['/ancillary_data/end_geoseg'][:]

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
                    print("Missing Data: The total number of segments stored in the different variables in the h5 file for ATL06 have different size. This prohibits us from building the dataframes and match the different segments." )
                    continue

                df = pd.DataFrame.from_dict(data)

                if add_flags:

                    df['q_flag_1'] = df.apply(lambda row: 1 if (row.h_robust_sprd >= 1) else 0, axis = 1)
                    df['q_flag_2'] = df.apply(lambda row: 1 if (row.h_li_sigma >= 1) else 0, axis = 1)
                    df['q_flag_3'] = df.apply(lambda row: 1 if (row.snr_significance >= 0.02) else 0, axis = 1)
                    df['q_flag_4'] = df.apply(lambda row: 1 if (row.signal_selection_source > 1) else 0, axis = 1)
                    df['q_flag_5'] = df.apply(lambda row: 1 if ( (row.beam_strength == 'strong' and row.n_fit_photons_ratio_w <= 4) or (row.beam_strength == 'weak'   and row.n_fit_photons_ratio_w <= 1) ) else 0, axis = 1)

                dataframes.append(df)
            
            except:
                
                print("There was an error with the file ", fname, ' and the ground track ', g)
        
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
    
    try:
        region_a = ipx.Query("ATL06", spatial_extent, date_range, start_time = time_start, end_time = time_end)
    except:
        print("No granules for this specification")
        return None
    
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

        # This lines were commented after the last update of icepyx. See Issue https://github.com/icesat2py/icepyx/issues/145
    
        #region_a.order_vars.append(var_list=['latitude','longitude','h_li','h_li_sigma','atl06_quality_summary','delta_time',
        #                                     'signal_selection_source', 'snr', 'snr_significance','h_robust_sprd','dh_fit_dx','dh_fit_dy','bsnow_conf',
        #                                     'cloud_flg_asr','cloud_flg_atm','msw_flag','bsnow_h','bsnow_od','layer_flag','bckgrd',
        #                                     'e_bckgrd','n_fit_photons','end_geoseg','segment_id','w_surface_window_final', 'sc_orient'])

        #region_a.subsetparams(Coverage=region_a.order_vars.wanted)
    
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
            print( fname_granules, "not found")
            
    if len(avail_granules) != len(requested_files):
        print("You are missing some files. There are a total of", len(avail_granules), "available granules but you are accessing", len(requested_files), "h5 files")
        
    
    dataf = atl06_2_df(requested_files)
    #dataf = atl06_2_df(Path(path).glob('*.h5'))
    
    # reset index
    dataf = dataf.reset_index()
    del dataf['index']
    
    return dataf