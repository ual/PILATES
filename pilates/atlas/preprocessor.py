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


def prepare_atlas_inputs(settings, year, warm_start=False):
    # set where to find urbansim output 
    urbansim_output_path = settings['usim_local_data_folder']
    if warm_start:
        # if warm start, read custom_mpo h5
        urbansim_output_fname = _get_usim_datastore_fname(settings, io = 'input')
    else:
        # if in main loop, read urbansim-generated h5 
        urbansim_output_fname = _get_usim_datastore_fname(settings, io = 'output', year = year)
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

            logger.info('Preparing ATLAS Year {} Input from Urbansim Output'.format(year))

        except: 
            logger.error('Urbansim Year {} Output Was Not Loaded Correctly by ATLAS'.format(year))

