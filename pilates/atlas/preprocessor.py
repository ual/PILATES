import h5py
from dataclasses import dataclass
from operator import index
import numpy as np
import pandas as pd
from pandas import HDFStore
import os
import logging
import openmatrix as omx
import yaml
import argparse

with open('settings.yaml') as file:
    settings = yaml.load(file, Loader=yaml.FullLoader)


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
            try:
                # prepare households atlas input
                households = data['/{}/households'.format(year)]
                households.to_csv('{}/households.csv'.format(atlas_input_path))

                # prepare blocks atlas input
                blocks = data['/{}/blocks'.format(year)]
                blocks.to_csv('{}/blocks.csv'.format(atlas_input_path))          

                # prepare persons atlas input
                persons = data['/{}/persons'.format(year)]
                persons.to_csv('{}/persons.csv'.format(atlas_input_path))

                # prepare residential unit atlas input
                residential_units = data['/{}/residential_units'.format(year)]
                residential_units.to_csv('{}/residential.csv'.format(atlas_input_path))

                # prepare jobs atlas input
                jobs = data['/{}/jobs'.format(year)]
                jobs.to_csv('{}/jobs.csv'.format(atlas_input_path))

                logger.info('Preparing ATLAS Year {} Input from Urbansim Output'.format(year))

            except: 
                logger.error('Urbansim Year {} Output Was Not Loaded Correctly by ATLAS'.format(year))
        
        else:
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



logger = logging.getLogger(__name__)

def compute_accessibility(path_list, measure_list, settings, year, threshold=500):
    # set where to put atlas csv inputs (processed from urbansim outputs)
    atlas_input_path = settings['atlas_host_input_folder'] + "/year{}".format(year)
    
    # for each OD, compute minimum time taken by public transit
    # inf means no public transit available; unit = minute
    ODmatrix = _get_time_ODmatrix(settings, path_list, measure_list, threshold)
    
    # assign values = 1 if time taken by public transit <= 30min; 0 if not
    ODmatrix = ODmatrix<=30
    
    # read and format geoid_to_zoneid mapping list
    mapping = pd.read_csv('pilates/utils/data/{}/beam/geoid_to_zone.csv'.format(settings['region']))
    mapping.index = mapping['GEOID']
    mapping = mapping['zone_id'].to_dict()
    
    # read OD matrix size (i.e., range of zone_id)
    zone_count = ODmatrix.shape[0]
    
    # read in jobs data (keep low_memory=False to solve dtypeerror)
    jobs = pd.read_csv("{}/year{}/jobs.csv".format(settings['atlas_host_input_folder'], year), low_memory=False)
    
    # map jobs geoid to zone id in OD matrix
    jobs['zone_id'] = jobs['block_id'].map(mapping)
    
    # count number of jobs for each block_id
    jobs_vector = jobs.groupby('block_id').agg({'job_id':'size','zone_id':'max'}).rename(columns={'job_id':'access_sum'})
    
    # average # of jobs per block for each taz
    jobs_vector = jobs_vector.groupby('zone_id').agg({'access_sum':'mean'})
    
    # make sure every zone id has a row in jobs_vector
    jobs_vector = jobs_vector.reindex(list(range(1,zone_count+1)), fill_value=0)
    
    # multiply OD matrix (o*d) with jobs vector (d*1)
    # to get number of jobs accessible by public transit within 30min
    accessibility = np.matmul(ODmatrix, jobs_vector)
    accessibility.index.name = 'zone_id'
    accessibility.index = accessibility.index + 1
    
    # # calculate taz-level zscore
    # accessibility['access_zscore'] = (accessibility['access_sum'] - accessibility['access_sum'].mean())/accessibility['access_sum'].std()
    
    # # write taz-level accessibility data
    # accessibility.to_csv('{}/accessibility_{}_taz.csv'.format(atlas_input_path, year))
    
    # read in taz_to_tract conversion matrix (1454*1588)
    taz_to_tract = pd.read_csv('{}/taz_to_tract_{}.csv'.format(settings['atlas_host_input_folder'], settings['region']), index_col=0)
    
    # convert taz- to tract-level accessibility data
    accessibility_tract = np.matmul(np.transpose(accessibility), np.array(taz_to_tract.values))
    accessibility_tract.columns = taz_to_tract.columns
    accessibility_tract = accessibility_tract.transpose()
    
    # calculate tract-level zscore
    accessibility_tract['access_zscore'] = (accessibility_tract['access_sum'] - accessibility_tract['access_sum'].mean())/accessibility_tract['access_sum'].std()
    
    # format before writing
    accessibility_tract.index.name = 'tract'
    accessibility_tract['urban_cbsa'] = 1 ## all sfbay tracts belong to cbsa

    # write tract-level accessibility data
    accessibility_tract.to_csv('{}/accessibility_{}_tract.csv'.format(atlas_input_path, year))


def _get_time_ODmatrix(settings, path_list, measure_list, threshold):   
    # open skims file
    skims_dir = settings['asim_local_input_folder']
    skims = omx.open_file(os.path.join(skims_dir, 'skims.omx'), mode = 'r') 
    
    # find the path with minimum time for each o-d
    ODmatrix = np.ones(skims.shape()) * np.inf
    
    for path in path_list:
        tmp_path = np.zeros(skims.shape())
        
        # sum total time taken for each specific path
        for measure in measure_list:
            tmp_measure = np.zeros(skims.shape())
            
            # extract data from skims.omx
            key = '{}_{}__AM'.format(path, measure)
            try:
                tmp_measure = np.array(skims[key])
            except:
                tmp_measure = np.zeros(skims.shape())
                # logger.error('{} not found in skims'.format(key)) 
            
            # sum up time taken for each path           
            tmp_path = tmp_path + tmp_measure
        
        # filter out paths with unreasonable TOTIVT (no available transit)
        tmp_path[tmp_path<=threshold] = 1E6
        
        # find the path with minimum total time taken
        ODmatrix = np.minimum(ODmatrix, tmp_path) 
    
    # divide by 100 to get minute values before returning
    ODmatrix = ODmatrix / 100  
    
    return ODmatrix       




