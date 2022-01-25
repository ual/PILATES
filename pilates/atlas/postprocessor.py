import h5py
import numpy as np
import pandas as pd
from pandas import HDFStore
import os
import logging

# Commented commands for only for debugging
import yaml
import argparse
with open('settings.yaml') as file:
    settings = yaml.load(file, Loader=yaml.FullLoader)

h5fname =  'custom_mpo_48197301_model_data.h5' # 'custom_mpo_06197001_model_data.h5'  #
year = 2010


logger = logging.getLogger(__name__)




# update "cars" and "hh_cars" columns in urbansim h5 outputs
# now this update is in-situ to minimize code changes needed

warm_start = True

    
def atlas_update_h5_vehicle(settings, year, warm_start=False):
    # use atlas outputs in year provided and update "cars" & "hh_cars"
    # columns in urbansim h5 files
    logger.info('ATLAS is updating urbansim outputs for Year {}'.format(year))

    # read and format atlas vehicle ownership output
    atlas_output_path = settings['atlas_host_output_folder'] # 'pilates/atlas/atlas_output'  #
    fname = 'householdv_{}.csv'.format(year)
    df = pd.read_csv(os.path.join(atlas_output_path, fname))
    df = df.rename(columns={'nvehicles':'cars'}).set_index('household_id')['cars'].sort_index(ascending=True)
    df_hh = pd.cut(df, bins=[-0.5, 0.5, 1.5, np.inf], labels=['none', 'one', 'two or more'])

    # set which h5 file to update
    h5path = settings['usim_local_data_folder']
    if warm_start:
        region = settings['region']
        region_id = settings['region_to_region_id'][region]
        h5fname = 'custom_mpo_{}_model_data.h5'.format(region_id)
    else:
        h5fname = 'model_data_{}.h5'.format(year)

    # read original h5 files
    with pd.HDFStore(os.path.join(h5path, h5fname), mode='r+') as h5:
        # if in main loop, update "model_data_*.h5", which has three layers ({$year}/households/cars)
        if not warm_start:
            olddf = h5[str(year)]['households']['cars']
            h5[str(year)]['households']['cars'] = df
            h5[str(year)]['households']['hh_cars'] = df_hh
            if olddf.shape != df.shape:
                logger.error('household_id mismatch found when ATLAS updates h5 vehicle info')
        
        # if in warm start, update "custom_mpo_***.h5", which has two layers (households/cars)
        else:
            olddf = h5['households']['cars']
            h5['households']['cars'] = df
            h5['households']['hh_cars'] = df_hh
            if olddf.shape != df.shape:
                logger.error('household_id mismatch found when ATLAS updates h5 vehicle info')





def atlas_add_vehileTypeId(settings, year, warm_start=False):
    # add a "vehicleTypeId" column in atlas output vehicles_{$year}.csv,
    # which will be read by beam preprocessor
    # vehicleTypeId = conc "bodytype"-"vintage_category"-"pred_power"

    atlas_output_path = settings['atlas_host_output_folder']
    fname = 'vehicles_{}.csv'.format(year)

    # read original atlas output "vehicles_*.csv" as dataframe
    df = pd.read_csv(os.path.join(atlas_output_path, fname))

    # add "vehicleTypeId" column in dataframe
    df['vehicleTypeId']=df[['bodytype', 'vintage_category', 'pred_power']].agg('-'.join, axis=1)

    # overwrite the original csv file
    df.to_csv(os.path.join(atlas_output_path, fname), index=False)


        
