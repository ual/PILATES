import pandas as pd 
import numpy as np 

import yaml
import geopandas as gpd
import pandas as pd
import shutil

from pilates.utils.geog import get_transit_stations_blocks
from pilates.utils.io import read_yaml, save_yaml

def read_model_data(settings, data_dir = None):
    region = settings['region']
    region_id = settings['region_to_region_id'][region]
    usim_model_data_fname = settings['usim_formattable_input_file_name']
    model_data_fname = usim_model_data_fname(region_id)
    
    if not data_dir:
        data_dir = settings['usim_local_data_folder']
    
    model_data_fpath = os.path.join(data_dir, model_data_fname)
   
    if not os.path.exists(model_data_fpath):
        raise ValueError('No input data found at {0}'.format(
            model_data_fpath))
    
    store = pd.HDFStore(model_data_fpath)
    return store

def modify_block_capacity(blocks, capacity_col, factor, mask = None):
    """
    Returns blocks tables with capacity_col modify by "factor". 
    If mask is define, it only modifies the blocks include in mask list. 
    
    Parameters: 
    -----------
    - blocks: DataFrame. Blocks tables. Block ID is set at index 
    - capacity_col: 
    - factor: 
    - mask: list. List of blocks ID to modify capacities. 
    """
    
    assert blocks.index.name == 'block_id'
    assert capacity_col in blocks.columns
    
    blocks = df.copy()
    if mask is None: 
        df[capacity_col] = df[capacity_col] * factor
        
    else: 
        mask_col
        mask_col = df.index.isin(mask).astype(bool)
        df[capacity_col] = df[capacity_col].mask(mask_col,df[capacity_col] * factor)
    return df

def tod_scenario(land_use, factor):
    
    # Create new file
    settings = read_yaml('settings.yaml')
    
    old_usim_formattable_input_file_name = settings['usim_formattable_input_file_name']
    new_usim_formattable_input_file_name = 'custom_mpo_{region_id}_model_data_' + land_use + '.h5'
    
    settings['region_to_asim_subdir']['sfbay'] = 'bay_area'
    settings['usim_formattable_input_file_name'] = new_usim_formattable_input_file_name
    save_yaml(settings_fpath, settings)
                
    copyfile(os.path.join('pilates', 'urbansim','data',old_usim_formattable_input_file_name), 
             os.path.join('pilates', 'urbansim','data',new_usim_formattable_input_file_name))
    
    # Modify new file
    if land_use == 'tod_employment':
        capacity = 'employment_capacity'
    elif land_use == 'tod_residential':
        capacity = 'residential_unit_capacity'
    
    store = read_model_data(settings)
    blocks = store['/blocks']
    
    mass_transit_block = get_transit_stations_blocks(settings)
    
    new_blocks = modify_block_capacity(blocks, capacity, 
                                       factor, mask = mass_transit_block)

    #save blocks table in store
    store.put('/blocks', new_blocks)
    store.close()