
# coding: utf-8

# NUTS Averaging V3
# if running on C3S_Energy VM, activate conda before running: source activate c3s_wemc
# luke.sanger@wemcouncil.org

# import packages
import iris
import geopandas as gpd
import pandas as pd
import numpy as np
import iris.pandas
import iris.analysis.cartography
import shapely
import glob, os
import warnings
import time

# import c3s filename utilites
import sys
sys.path.append(os.path.abspath("/data/private/resources/lookup/"))
from filename_utilities import print_structure, print_elements, check_filename

# place this file in directory where ERA5 files are located and define path below:
path = r'/data/private/wemc/TA/testfile'
allFiles = [os.path.basename(x) for x in glob.glob(path + r"/*.nc")]

# IMPORTANT: specify one nuts level for processing .nc files (Must be nut0 or nut2)
# nuts = 'nut0'
nuts = 'nut2'

if nuts == 'nut0':
# load nuts0 country regions for processing
    path2 = r'/data/private/resources/nuts0_masked_nc/'
    nutsFiles = [os.path.basename(x) for x in glob.glob(path2 + r"/*.nc")]
elif nuts == 'nut2':
# load nuts2 sub-country regions for processing
    path2 = r'/data/private/resources/nuts2_masked_nc/'
    nutsFiles = [os.path.basename(x) for x in glob.glob(path2 + r"/*.nc")]

# get first cube in list to get cosine weights
nutslist2 = iris.load(path2 + nutsFiles[0])
nutcube2 = nutslist2[0]
nutcube2 = nutcube2.intersection(longitude=(-180, 180))
lats2 = nutcube2.coord('latitude').points
lons2 = nutcube2.coord('longitude').points

# get array of latitudes from nutcube
cos_lat = iris.analysis.cartography.cosine_latitude_weights(nutcube2)

# hide pointless iris warning message about lat/lon
warnings.filterwarnings("ignore", category=UserWarning)

# run timer for measuring operation speed
start = time.time()

# literate through nc files for processing
for file_ in allFiles:
    cubelist = iris.load(file_)
    cube = cubelist[0]
    cube = cube.intersection(longitude=(-180, 180))
    df = pd.DataFrame()
    name = os.path.splitext(file_)[0]
    print('loaded ' + name + ' for ' + nuts + ' averaging')

    # iterate through nuts regions 
    for nut in nutsFiles:
        nutslist = iris.load(path2 + nut)
        nutcube = nutslist[0]
        nutcube = nutcube.intersection(longitude=(-180, 180))
        lats = nutcube.coord('latitude').points
        lons = nutcube.coord('longitude').points

        # get NUTSID from filename for column naming
        if nuts == 'nut0':     
            edit = str(nut)
            NUTSID = edit[:-15]
            print('processing ' + NUTSID + ' area, level = ' + nuts)
        elif nuts == 'nut2':
            edit = str(nut)
            NUTSID = edit[:-13]
            print('processing ' + NUTSID + ' area, level = ' + nuts)    
        
        # multiply region by cosine lats
        lsm_cos_lat = nutcube.copy()
        lsm_cos_lat.data *= cos_lat
        
        # apply the lsm_cos_lat to the main cube
        cube_lsm_cos_lat = cube.copy()
        cube_lsm_cos_lat.data *= lsm_cos_lat.data
        
        #sum of lsm_cos_lat 
        lsm_cos_lat_sum = lsm_cos_lat.collapsed(['latitude','longitude'], iris.analysis.SUM, weights=None)

        # sum of mc_lsm_cos_lat
        cube_lsm_cos_lat_sum = cube_lsm_cos_lat.collapsed(['latitude','longitude'], iris.analysis.SUM, weights=None)
        
        # divide sum of cube_lsm_cos_lat by sum of lsm_cos_lat
        end_nuts_lsm_cos_lat_sum = cube_lsm_cos_lat_sum.copy()
        end_nuts_lsm_cos_lat_sum.data /= lsm_cos_lat_sum.data
        
        # save as series, rename colume to NUTSID and concat to dataframe
        dfs = iris.pandas.as_series(end_nuts_lsm_cos_lat_sum, copy=True)
        dfs.rename(columns={1: NUTSID}, inplace=True) 
        df = pd.concat((df, dfs.rename(NUTSID)), axis=1)
    
    # organise dataframe columns alphabetically
    df1 = df.groupby(axis=1, level=0).first() 
    
    # save dataframe as csv
    n1 = str(name)
    n2 = n1.rstrip(".nc")
    csv_name = n2[:32] + nuts + n2[36:] + ".csv"
    df1.to_csv(csv_name)
    print(csv_name +' created')
    check_filename(csv_name)
    
print(nuts + ' processing complete')
end = time.time()
print('time elapsed in seconds:')
print(end - start)

