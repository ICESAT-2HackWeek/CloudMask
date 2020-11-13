from ipyleaflet import Map, basemaps, basemap_to_tiles, DrawControl
import ipywidgets as widgets
from ipywidgets import GridspecLayout, AppLayout, VBox, HBox, HTML, Label
import datetime

import h5py
import os
import numpy as np
import pandas as pd
from astropy.time import Time
import matplotlib.pyplot as plt

from pathlib import Path
import icepyx as ipx

import warnings
warnings.filterwarnings("ignore")

def area():
    """
    Provide map and options to choose area of interest
    """

    center = [65.73, -50.71]
    zoom = 4
    m = Map(center=center, zoom=zoom)
    
    global dc, start, end, file, lon_l, lat_l, lon_r, lat_r
    
    # Pick date
    start = widgets.DatePicker(disabled=False)
    end = widgets.DatePicker(disabled=False)
    
    # Select from map
    dc = DrawControl(rectangle={'shapeOptions': {'color': '#0000FF'}}, 
                     polyline={}, polygon={}, circlemarker={})
    m.add_control(dc)
    
    # Shapefile
    file = widgets.FileUpload(accept='.shp', multiple=False)
    
    # Bounding box
    lon_l = widgets.FloatText(description="lon")
    lat_l = widgets.FloatText(description="lat")
    lon_r = widgets.FloatText(description="lon")
    lat_r = widgets.FloatText(description="lat")
    
    return(AppLayout(header = VBox([HTML("<h1>Select area (time and space)</h1>"),
                         HBox([Label("Start Date:"), start, 
                               Label("End Date:"), end])]),
                     center = m,
                     right_sidebar = VBox([HTML("<h3>or upload shapefile<h3>"), file, HTML("<h3> <h3>"), HTML("<h3>or bounding box<h3>"), 
                             Label("Bottom-left corner"), lon_l, lat_l, Label("Upper-right corner"), lon_r, lat_r])))


def atl06_data(path="./data/new_ATL06", 
               user='alicecima', 
               email='alice_cima@berkeley.edu'):
    """
    Return a Pandas dataframe with ATL06 data for selected area 
    """
    
    global spatial_extent
    
    if file.value != {}: 
        spatial_extent = file
        method = 'shapefile'
    elif dc.last_action != 'deleted' and dc.last_action !='':
        spatial_extent = dc.last_draw['geometry']['coordinates'][0]
        method = 'map'
    else:
        spatial_extent = [lon_l.value, lat_l.value, lon_r.value, lat_r.value]
        method = 'box corners'
        if lon_l.value>lon_r.value or lat_l.value>lat_r.value:
            raise Exception('Wrong specification of lat lon')

    print('The area was selected using: ' + method)
    
    df = read_atl06(spatial_extent, 
                date_range=[str(start.value), str(end.value)], 
                time_start = "00:00:00",
                time_end = "23:59:59",
                path = path, 
                user = user, 
                email = email)
    return(df)
    
    
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


def gps2dyr(time):
    """
    Convert GPS time to decimal years.
    """
    return Time(time, format='gps').datetime



def read_atl06_fromfile(fname, outdir='data', bbox=None):
    """
    Read one ATL06 file and output a single dataframes. 
    
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
                t_gps = t_ref + delta_t  # Time in GPS seconds (secs since Jan 5, 1980)
                data['time'] = gps2dyr(t_gps) # GPS sec to datetime
                data['start_rgt'] = np.repeat(fi['/ancillary_data/start_rgt'][:][0], len(delta_t))

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
                # data['h_robust_sprd'] = fi[g+'/land_ice_segments/fit_statistics/h_robust_sprd'][:]
                # 2) h_li_sigma < 1 (meters)
                # Propagation error due to sample error 
                # data['h_li_sigma'] = fi[g+'/land_ice_segments/h_li_sigma'][:]            
                # 3) snr_significance < 0.02
                # Probability that signal-finding routine would converge to at least the observed signal-to-noise (SNR) for a random
                # noise input. 
                # data['snr_significance'] = fi[g+'/land_ice_segments/fit_statistics/snr_significance'][:]
                # 4) Signal_selection_source \in {0,1}
                # this variable takes values in {0,1,2,3} based on which algorithms was used to compute the heights, if possible
                # data['signal_selection_source'] = fi[g+'/land_ice_segments/fit_statistics/signal_selection_source'][:]
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
                data['dem_flag'] = fi[g+'/land_ice_segments/dem/dem_flag'][:]
                data['dem_h'] = fi[g+'/land_ice_segments/dem/dem_h'][:]
                data['geoid_h'] = fi[g+'/land_ice_segments/dem/geoid_h'][:]

                ### Backgrounds Photons ###

                # background count rate
                data['bckgrd'] = fi[g+'/land_ice_segments/geophysical/bckgrd'][:]
                # Expected background count rate
                #data['e_bckgrd'] = fi[g+'/land_ice_segments/geophysical/e_bckgrd'][:]


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

                df = pd.DataFrame.from_dict(data)

                dataframes.append(df)
                
            except:
                pass
                #print("There was an error with the file ", fname, ' and the ground track ', g)
        
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
                path = "./data/new_ATL06", 
                user = 'alicecima', 
                email = 'alice_cima@berkeley.edu'):
    
    region_a = ipx.Query("ATL06", spatial_extent, date_range, start_time = time_start, end_time = time_end)
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
    
    
    ### If I already downloaded these files, I don't need to do it again
    
    # I should check first that such directory exists!
    
    if all( [ is_file_in_directory(avail_granules[i], path) for i in range(len(avail_granules)) ] ):
        
        print("You have already downloaded all the requested files")
        
    else:

        region_a.earthdata_login(user, email)

        # This lines were commented after the last update of icepyx. See Issue https://github.com/icesat2py/icepyx/issues/145
    
        #region_a.order_vars.append(var_list=['latitude','longitude','h_li','atl06_quality_summary','delta_time',
        #                                     'signal_selection_source', 'snr', 'snr_significance','h_robust_sprd','dh_fit_dx','dh_fit_dy','bsnow_conf',
        #                                     'cloud_flg_asr','cloud_flg_atm','msw_flag','bsnow_h','bsnow_od','layer_flag','bckgrd',
        #                                     'n_fit_photons','end_geoseg','segment_id','w_surface_window_final', 'sc_orient'])

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
            print(fname_granules, "not found")
            
    if len(avail_granules) != len(requested_files):
        print("You are missing some files. There are a total of", len(avail_granules), 
              "available granules but you are accessing", len(requested_files), "h5 files")
        
    
    dataf = atl06_2_df(requested_files)
    #dataf = atl06_2_df(Path(path).glob('*.h5'))
    
    # reset index
    dataf = dataf.reset_index()
    del dataf['index']
    
    return(dataf)




def clean(data):
    """
    Drop outliers and create 'height_diff' column
    """
    d = data[data.loc[:,'h_li']<1e+38]
    d['height_diff'] = d['h_li'] - d['dem_h']
    return(d)


