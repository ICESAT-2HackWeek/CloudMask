import h5py
import numpy as np
from sklearn.neighbors import BallTree


def associate(dataframe, swath, variable='Integer_Cloud_Mask'):
    """Takes a dataframe, and a satellite file, and extracts the
    selected variable as a new column.

    Assumes that data coordinates are in lat/lon
    Assumes dataframe higher resolution than swath"""
    
    # grab swath coordinates
    # TODO; make general for inferring lat/lon paths
    latS = np.array(f['geolocation_data']['latitude'])
    lonS = np.array(f['geolocation_data']['longitude'])

    S_rad = np.vstack([lonS[:].ravel(),latS[:].ravel()]).T
    S_rad *= np.pi / 180.

    # grab dataframe coords
    latF = dataframe.lat.values
    lonF = dataframe.lon.values

    F_rad = np.vstack([lonF[:].ravel(),latF[:].ravel()]).T
    F_rad *= np.pi / 180.

    # build spatial tree; find matches
    print("building tree")
    S_Ball = BallTree(S_rad,metric='haversine')
    print("searching data")
    indicies = S_Ball.query(F_rad, k=1,
                            breadth_first=True,
                            return_distance=False)
    
    extract = swath['geophysical_data'][variable].value
    new_column = extract.ravel()[indicies]
    dataframe[variable] = new_column
    return dataframe

