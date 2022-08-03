import os
import logging
import pandas as pd
from pilates.utils.io import read_datastore

logger = logging.getLogger(__name__)


def _get_usim_datastore_fname(settings, io, year=None):

    if io == 'output':
        datastore_name = settings['usim_formattable_output_file_name'].format(
            year=year)
    elif io == 'input':
        region = settings['region']
        region_id = settings['region_to_region_id'][region]
        usim_base_fname = settings['usim_formattable_input_file_name']
        datastore_name = usim_base_fname.format(region_id=region_id)

    return datastore_name


def create_next_iter_usim_data(settings, year, forecast_year):

    data_dir = settings['usim_local_data_folder']

    # Move UrbanSim input store (e.g. custom_mpo_193482435_model_data.h5)
    # to archive (e.g. input_data_for_2015_outputs.h5) because otherwise
    # it will be overwritten in the next step.
    input_datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(data_dir, input_datastore_name)
    if os.path.exists(input_store_path):
        archive_fname = 'input_data_for_{0}_outputs.h5'.format(forecast_year)
        logger.info(
            "Moving urbansim inputs from the previous iteration to {0}".format(
                archive_fname))
        new_input_store_path = input_store_path.replace(
            input_datastore_name, archive_fname)
        os.rename(input_store_path, new_input_store_path)

    og_input_store = pd.HDFStore(new_input_store_path)
    new_input_store = pd.HDFStore(input_store_path)
    assert len(new_input_store.keys()) == 0
    updated_tables = []

    # load last iter output data
    output_datastore_name = _get_usim_datastore_fname(settings, 'output', forecast_year)
    output_store_path = os.path.join(data_dir, output_datastore_name)
    
    # copy usim outputs into new input data store
    logger.info(
        'Merging results back into UrbanSim and storing as .h5!')
    output_store, table_prefix_year = read_datastore(settings, forecast_year)

    for h5_key in output_store.keys():
        table_name = h5_key.split('/')[-1]
        if os.path.join('/', table_prefix_year, table_name) == h5_key:
            updated_tables.append(table_name)
            new_input_store[table_name] = output_store[h5_key]
        
    # copy missing tables from original usim inputs into new input data store
    for h5_key in og_input_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            logger.info(
                "Copying {0} input table to output store!".format(
                    table_name))
            new_input_store[table_name] = og_input_store[h5_key]

    assert new_input_store.keys() == og_input_store.keys()
    og_input_store.close()
    new_input_store.close()
    output_store.close()
    
