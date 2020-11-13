import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree


def CLDMSK_date(file_name):
    """
    Given the name of a VIIRS or MODIS Aqua CLDMSK product, it returns the date
    
    Example of a valid VIIRS file:
    CLDMSK_L2_VIIRS_SNPP.A2020163.1642.001.2020164010320.nc
    Example of a valid MODIS Aqua file:
    CLDMSK_L2_MODIS_Aqua.A2020139.1755.001.2020140163713.nc
    
    A full description of the product could be found here:
    https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/CLDMSK_L2_VIIRS_SNPP/
    """
    assert len(file_name) == 55, "The name of the CLDMSK file is in an incorrect format"
    date = int(file_name[-33:-26] + file_name[-25:-21] )
    return pd.to_datetime(date, format="%Y%j%H%M")

  
    

def viirs_date(viirs_file_name):
    """
    Given the name of a VIIRS file, it returns the date
    
    Example of a valid VIIRS file:
    CLDMSK_L2_VIIRS_SNPP.A2020163.1642.001.2020164010320.nc
    
    A full description of the product could be found here:
    https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/CLDMSK_L2_VIIRS_SNPP/
    """
    print('I recommend to use CLDMSK_date instead of viirs_date')
    assert len(viirs_file_name) == 55, "The name of the VIIRS file is in an incorrect format"
    date = int(viirs_file_name[-33:-26] + viirs_file_name[-25:-21] )
    return pd.to_datetime(date, format="%Y%j%H%M")


#def modis_date(modis_file_name):
#    """
#    Given the name of a MODIS file, it returns the date
#    
#    Example of a valid MODIS file:
#    MYD35_L2.A2020140.0350.061.2020140153933.hdf
    
#    A full description of the product could be found here:
    
#    """
#    assert len(modis_file_name) == 44, "The name of the MODIS file is in an incorrect format"
    
#    l = modis_file_name.split('.')
#    year = l[1][1:5]
#    day = l[1][5:8]
#    hour_min = l[2]
#    date = year + day + hour_min
#    return pd.to_datetime(date, format="%Y%j%H%M")
    
    #date = int(modis_file_name[-17:-4])
    #return pd.to_datetime(date, format="%Y%j%H%M%S")
    

def associate(rad_1, rad_2, k_nn = 1):
    
    """
    Given two grids rad_1 and rad_2, this associates each point in rad_2 to the k-nearest neighbours in
    rad_1.
    Pairs of the form [latitude, longitude]
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

