import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
from shapely import geometry
from shapely.ops import unary_union


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



def image_convexHull (lonA, latA):
    """
    Given the longitude and latitude 2-dimensional arrays of a image, it returns a shapely polygon object
    corresponding to the convex hull of the image. 
    """
    
    assert lonA.shape == latA.shape
    
    N, M = lonA.shape
    
    contour_points = []
    
    for i in range(N): 
        contour_points.append( (lonA[i, 0], latA[i, 0]) )
        contour_points.append( (lonA[i, M-1], latA[i, M-1]) )
    for i in range(M):
        contour_points.append( (lonA[0, i], latA[0,i]) )
        contour_points.append( (lonA[N-1, i], latA[N-1, i]) )
        
    contour_points = [x for x in contour_points if ( ( -180 <= x[0] ) 
                                                   & ( x[0] <= 180 )
                                                   & ( -90 <= x[1] )
                                                   & ( x[1] <= 90) )]
    
    points = geometry.MultiPoint(contour_points)
    
    cloud_contour = points.convex_hull
    cloud_contour = cloud_contour.simplify(tolerance = 0.01)
    
    return cloud_contour