import h5py
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import seaborn as sns
from sklearn.neighbors import BallTree
from astropy.time import Time
from sklearn.utils import shuffle
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, f1_score, recall_score
from shapely import geometry

##### Directories ######

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
    
    return False 

##### Time #######

def gps2dyr(time):
    """
    Converte GPS time to decimal years.
    """
    #return Time(time, format='gps').decimalyear
    return Time(time, format='gps').datetime

def time_from_GPS(my_timestamp):
    """
    Given the total seconds from 1980-01-06 00:00 at UTC (i.e. GPS) it return the corresponding datatime object
    """
    
    time_GPS = datetime(1980, 1, 6, 0, 0, 0, 0)
    # This timestamp coincides with the one we obtain from https://www.unixtimestamp.com/index.php
    time_GPS_timestamp = time_GPS.replace(tzinfo=timezone.utc).timestamp()

    adquisition_timestamp = time_GPS_timestamp + my_timestamp

    my_time = datetime.fromtimestamp(adquisition_timestamp, tz=timezone.utc)
    
    return my_time


def time_from_TAI93(my_timestamp):
    """
    Given the total seconds from 1993-01-01 00:00 at UTC it return the corresponding datatime object
    """
    
    time_TAI93 = datetime(1993, 1, 1, 0, 0, 0, 0)
    # This timestamp coincides with the one we obtain from https://www.unixtimestamp.com/index.php
    time_TAI93_timestamp = time_TAI93.replace(tzinfo=timezone.utc).timestamp()

    adquisition_timestamp = time_TAI93_timestamp + my_timestamp

    my_time = datetime.fromtimestamp(adquisition_timestamp, tz=timezone.utc)
    
    return my_time


#######

def merge_df_from_dict(dictonary, entries_to_merge = "all", shuff = False):
    """
    Takes a dictonary with keys associated to a pandas dataframe with the same column and marges all the dataframes into a single one
    """
    
    if entries_to_merge == "all":
        entries = list(dictonary.keys())
    else:
        entries = entries_to_merge
        
    dfs = []
    for x in entries:
        dfs.append(dictonary[x])
        
    df = pd.concat(dfs, ignore_index=True)
    
    if shuff:
        df = shuffle(df).reset_index(drop=True)
        
    return df


def hist_df(df, var, by, bins = 50):
    """
    Plots an histogram grouping by 'by' using the column 'var' of a pandas dataframes
    """
    classes = np.unique(df[by])
    
    for c in classes:
        _ = plt.hist( list(df[ df[by] == c ][var] ), bins, alpha = 0.5, label = c )

    plt.title("Histogram of " + str(var) + " grouped by " + str(by))
    plt.legend()
    plt.show
    return None


def print_attrs_h5(h5file, counter = 0):
    '''
    Print all the atrivbutes from a h5 file
    '''
    for key in h5file.keys():
    
        try:
            print_attrs_h5(h5file[key], counter + 1)
            
        except:
            print('\t'*counter, end = '')
            print(key)
            
def p_a_cond_b (df, a, b):
    """
    Given a dataframes df, it computes the empirial conditionl probability
    """    
    
    assert all(np.unique(df[a]) == [0,1]), "Variable must be binary"
    assert all(np.unique(df[b]) == [0,1]), "Variable must be binary"
    
    p_b = df[df[b] == 1].shape[0]
    p_a_b = df[(df[a] == 1) & (df[b] == 1)].shape[0]
    
    return p_a_b / p_b

def conditional_heatplot(df, variables, plot = True):
    """
    Given a dataframes and a list of binary column names, it returns the matrix of all the conditional probabilities
    """
    
    res = np.zeros((len(variables), len(variables)))
    
    for i, x in enumerate(variables):
        for j, y in enumerate(variables):
            if i == j:
                res[i,j] = np.nan
                continue
            res[i,j] = p_a_cond_b(df, a = x, b = y)
            
    if plot:
        ax = sns.heatmap(res, annot=True, cmap = sns.color_palette("Blues"))        
    
    return res


def fit_scores(y_true, y_fit):
    
    res = {}
    
    res['accuracy'] = accuracy_score(y_true, y_fit)
    res['f1'] = f1_score(y_true, y_fit)
    res['recall'] = recall_score(y_true, y_fit)
    
    return res



def drainage_basin(basin = 6.2, 
                   polygon_size = 'full',
                   path_drainage_basin = '/glade/u/home/fsapienza/CloudMask/drainage_basin/GrnDrainageSystems_Ekholm.txt'):
    
    """
    This function retuns an array with the vertices in (longitude, latitude) of a polygon of size polygon_size that approximates
    the drainage basin with id basin. 
    If polygon_size = "full", then it returns the full drainage basin, with all its vertices.
    
    """
    
    zwally = pd.read_csv(path_drainage_basin, sep='\s+', names=['basin', 'lat','long'])
    
    LL = zip(zwally[zwally.basin == float(basin)].long, zwally[zwally.basin == float(basin)].lat)
    LL = list(LL)
    
    longitudes = [x[0] for x in LL]
    # Since icepyx just accept longitudes in [-180, 180], we need to modify those longitudes in [180, 360]
    longitudes = [x if ( (x >= 0) & (x <= 180)) else x - 360 for x in longitudes]

    latitudes = [x[1] for x in LL]
    
    LL = [(longitudes[i], latitudes[i]) for i in range(len(longitudes))]
    
    if polygon_size == 'full':
        step = 1
    else:
        step = int(len(LL) / (polygon_size - 1))
        
    spatial_extent = LL[::step]

    # Add first vertice to the end of the array
    spatial_extent.append(spatial_extent[0])
    
    # Build shapely polygon
    poly = geometry.Polygon(spatial_extent)

    return poly