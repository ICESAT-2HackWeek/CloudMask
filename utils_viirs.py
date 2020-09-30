import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree


def viirs_date(viirs_file_name):
    """
    Given the name of a VIIRS file, it returns the date
    """
    
    date = int(viirs_file_name[-33:-26] + viirs_file_name[-25:-21] )
    return pd.to_datetime(date, format="%Y%j%H%M")


def associate(rad_1, rad_2, k_nn = 1):
    
    """
    Given two grids rad_1 and rad_2, this associates each point in rad_2 to the k-nearest neighbours in
    rad_1.
    """
    
    # Room to improvement:
    # - Run the Ball tree on the smallest net
    # - Use something more efficient than a Ball Tree, like a binary search.
    
    # Build Ball Tree
    Ball = BallTree(rad_1, metric='haversine')
    
    # Searching Data
    distances, indices = Ball.query(rad_2, 
                                    k = k_nn,
                                    breadth_first = True,
                                    return_distance = True) 
    
    assert rad_2.shape[0] == indices.shape[0]
    
    return distances, indices


#def associate(dataframe, swath, variable='Integer_Cloud_Mask'):
#    """
#    Takes a dataframe, and a satellite file, and extracts the
#    selected variable as a new column.
#
#    Assumes that data coordinates are in lat/lon
#    Assumes dataframe higher resolution than swath
#    """
    
    # grab swath coordinates
    # TODO; make general for inferring lat/lon paths
#    latS = np.array(swath['geolocation_data']['latitude'])
#    lonS = np.array(swath['geolocation_data']['longitude'])

#    S_rad = np.vstack([lonS[:].ravel(),latS[:].ravel()]).T
#    S_rad *= np.pi / 180.

    # grab dataframe coords
#    latF = dataframe.lat.values
#    lonF = dataframe.lon.values

#    F_rad = np.vstack([lonF[:].ravel(),latF[:].ravel()]).T
#    F_rad *= np.pi / 180.

    # build spatial tree; find matches
#    print("building tree")
#    S_Ball = BallTree(S_rad,metric='haversine')
#    print("searching data")
#    indicies = S_Ball.query(F_rad, k=1,
#                            breadth_first=True,
#                            return_distance=False)
    
#    extract = swath['geophysical_data'][variable].value
#    new_column = extract.ravel()[indicies]
#    dataframe[variable] = new_column
#    return dataframe

