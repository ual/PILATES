import h5py
import numpy as np
import pandas as pd
from pandas import HDFStore
import os
import logging


# import yaml
# import argparse
# with open('settings.yaml') as file:
#     settings = yaml.load(file, Loader=yaml.FullLoader)

logger = logging.getLogger(__name__)


def prepare_atlas_inputs(settings, year, warm_start=False):
    # set where to find urbansim output 
    urbansim_output_path = settings['usim_local_data_folder']
    if warm_start:
        # if warm start, read custom_mpo h5
        region = settings['region']
        region_id = settings['region_to_region_id'][region]
        urbansim_output_fname = 'custom_mpo_{}_model_data.h5'.format(region_id)
    else:
        # if in main loop, read urbansim-generated h5 
        urbansim_output_fname = 'model_data_{}.h5'.format(year)
    urbansim_output = os.path.join(urbansim_output_path,urbansim_output_fname)
    
    # set where to put atlas csv inputs (processed from urbansim outputs)
    atlas_input_path = settings['atlas_host_input_folder'] + "/year{}".format(year)

    # if atlas input path does not exist, create one
    if not os.path.exists(atlas_input_path):
        os.makedirs(atlas_input_path)
        logger.info('ATLAS Input Path Created for Year {}'.format(year))
    
    # read urbansim h5 outputs
    with pd.HDFStore(urbansim_output,mode='r') as data:
        if not warm_start:
            data = data['/{}'.format(year)]

        try:
            # prepare households atlas input
            households = data['/households']
            households.to_csv('{}/households.csv'.format(atlas_input_path))

            # prepare blocks atlas input
            blocks = data['/blocks']
            blocks.to_csv('{}/blocks.csv'.format(atlas_input_path))          

            # prepare persons atlas input
            persons = data['/persons']
            persons.to_csv('{}/persons.csv'.format(atlas_input_path))

            # prepare residential unit atlas input
            residential_units = data['/residential_units']
            residential_units.to_csv('{}/residential.csv'.format(atlas_input_path))

            # prepare jobs atlas input
            jobs = data['/jobs']
            jobs.to_csv('{}/jobs.csv'.format(atlas_input_path))

        except: 
            logger.error('Urbansim Year {} Output Was Not Loaded Correctly by ATLAS')

