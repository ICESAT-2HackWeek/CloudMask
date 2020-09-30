import h5py
import os
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.neighbors import BallTree
from astropy.time import Time
from sklearn.utils import shuffle
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, f1_score, recall_score


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