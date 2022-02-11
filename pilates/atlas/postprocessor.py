import h5py
import numpy as np
import pandas as pd
from pandas import HDFStore
import os
import logging

# # Commented commands for only for debugging
# import yaml
# import argparse
# with open('settings.yaml') as file:
#     settings = yaml.load(file, Loader=yaml.FullLoader)


logger = logging.getLogger(__name__)


def _get_usim_datastore_fname(settings, io, year=None):
    # reference: asim postprocessor
    if io == 'output':
        datastore_name = settings['usim_formattable_output_file_name'].format(
            year=year)
    elif io == 'input':
        region = settings['region']
        region_id = settings['region_to_region_id'][region]
        usim_base_fname = settings['usim_formattable_input_file_name']
        datastore_name = usim_base_fname.format(region_id=region_id)
    return datastore_name



def atlas_update_h5_vehicle(settings, output_year, warm_start=False):
    # use atlas outputs in year provided and update "cars" & "hh_cars"
    # columns in urbansim h5 files
    logger.info('ATLAS is updating urbansim outputs for Year {}'.format(output_year))

    # read and format atlas vehicle ownership output
    atlas_output_path = settings['atlas_host_output_folder'] # 'pilates/atlas/atlas_output'  #
    fname = 'householdv_{}.csv'.format(output_year)
    df = pd.read_csv(os.path.join(atlas_output_path, fname))
    df = df.rename(columns={'nvehicles':'cars'}).set_index('household_id')['cars'].sort_index(ascending=True)
    df_hh = pd.cut(df, bins=[-0.5, 0.5, 1.5, np.inf], labels=['none', 'one', 'two or more'])

    # set which h5 file to update
    h5path = settings['usim_local_data_folder']
    if warm_start:
        h5fname = _get_usim_datastore_fname(settings, io = 'input')
    else:
        h5fname = _get_usim_datastore_fname(settings, io = 'output', year = output_year)

    # read original h5 files
    with pd.HDFStore(os.path.join(h5path, h5fname), mode='r+') as h5:

        # if in main loop, update "model_data_*.h5", which has three layers ({$year}/households/cars)
        if not warm_start:
            olddf = h5['/{}/households'.format(output_year)]['cars']
            if olddf.shape != df.shape:
                logger.error('ATLAS household_id mismatch found - NOT update h5 datastore')
            else:
                h5['/{}/households'.format(output_year)]['cars'] = df
                h5['/{}/households'.format(output_year)]['hh_cars'] = df_hh
                logger.info('ATLAS update h5 datastore - done')
            del df, df_hh, olddf
        
        # if in warm start, update "custom_mpo_***.h5", which has two layers (households/cars)
        else:
            olddf = h5['households']['cars']
            if olddf.shape != df.shape:
                logger.error('ATLAS household_id mismatch found - NOT update h5 datastore')
            else:
                h5['households']['cars'] = df
                h5['households']['hh_cars'] = df_hh
                logger.info('ATLAS update h5 datastore - done')
            del df, df_hh, olddf



def atlas_add_vehileTypeId(settings, output_year):
    # add a "vehicleTypeId" column in atlas output vehicles_{$year}.csv,
    # write as vehicles2_{$year}.csv
    # which will be read by beam preprocessor
    # vehicleTypeId = conc "bodytype"-"vintage_category"-"pred_power"

    atlas_output_path = settings['atlas_host_output_folder']
    fname = 'vehicles_{}.csv'.format(output_year)

    # read original atlas output "vehicles_*.csv" as dataframe
    df = pd.read_csv(os.path.join(atlas_output_path, fname))

    # add "vehicleTypeId" column in dataframe
    df['vehicleTypeId']=df[['bodytype', 'vintage_category', 'pred_power']].agg('-'.join, axis=1)

    # write to a new file vehicles2_*.csv 
    # because original file cannot be overwritten (root-owned)
    # may revise later
    df.to_csv(os.path.join(atlas_output_path, 'vehicles2_{}.csv'.format(output_year)), index=False)


        
