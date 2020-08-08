import h5py
import os
import numpy as np
from sklearn.neighbors import BallTree
from astropy.time import Time


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


def gps2dyr(time):
    """
    Converte GPS time to decimal years.
    """
    #return Time(time, format='gps').decimalyear
    return Time(time, format='gps').datetime


