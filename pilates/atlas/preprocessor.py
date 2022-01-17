import h5py
import numpy as np
import pandas as pd
from pandas import HDFStore
import os


# import yaml
# import argparse
# def parse_args_and_settings(settings_file='settings.yaml'):

#     # read settings from config file
#     with open(settings_file) as file:
#         settings = yaml.load(file, Loader=yaml.FullLoader)
#     return settings
# settings = parse_args_and_settings()


logger = logging.getLogger(__name__)

def prepare_atlas_inputs(settings, year):
    # set where to find urbansim output 
    urbansim_output_path = settings['usim_local_data_folder']
    urbansim_output_fname = 'model_data_{}.h5'.format(year)
    urbansim_output = os.path.join(urbansim_output_path,urbansim_output_fname)
    
    # set where to put atlas csv inputs (processed from urbansim outputs)
    atlas_input_path = settings['atlas_host_input_folder'] + "/year{}".format(year)

    # if atlas input path does not exist, create one
    if not os.path.exists(atlas_input_path):
        os.makedirs(atlas_input_path)
        print('ATLAS Input Path Created for Year {}'.format(year))
    
    # read urbansim h5 outputs
    with pd.HDFStore(urbansim_output,mode='r') as data:
        try:
            # prepare households atlas input
            households = data['{}/households'.format(year)]
            households.to_csv('{}/households.csv'.format(atlas_input_path))

            # prepare blocks atlas input
            blocks = data['{}/blocks'.format(year)]
            blocks.to_csv('{}/blocks.csv'.format(atlas_input_path))          

            # prepare persons atlas input
            persons = data['{}/persons'.format(year)]
            persons.to_csv('{}/persons.csv'.format(atlas_input_path))

            # prepare residential unit atlas input
            residential_units = data['{}/residential_units'.format(year)]
            residential_units.to_csv('{}/residential.csv'.format(atlas_input_path))

            # prepare jobs atlas input
            jobs = data['{}/jobs'.format(year)]
            jobs.to_csv('{}/jobs.csv'.format(atlas_input_path))

        except: 
            print('Urbansim Year {} Output Was Not Loaded Correctly by ATLAS')

