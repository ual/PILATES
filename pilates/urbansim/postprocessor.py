import os
import logging
import pandas as pd


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


def create_next_iter_usim_data(settings, year):

    data_dir = settings['usim_local_data_folder']

    # load original input data
    input_datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(data_dir, input_datastore_name)
    if os.path.exists(input_store_path):
        archive_fname = 'input_data_for_{0}_outputs.h5'.format(year)
        logger.info(
            "Moving urbansim inputs from the previous iteration to {0}".format(
                archive_fname))
        new_input_store_path = input_store_path.replace(
            input_datastore_name, archive_fname)
        os.rename(input_store_path, new_input_store_path)

    og_input_store = pd.HDFStore(new_input_store_path)

    # load last iter output data
    output_datastore_name = _get_usim_datastore_fname(settings, 'output', year)
    output_store_path = os.path.join(data_dir, output_datastore_name)
    output_store = pd.HDFStore(output_store_path)

    logger.info(
        'Merging results back into UrbanSim and storing as .h5!')

    # copy usim outputs into new input data store
    new_input_store = pd.HDFStore(input_store_path)
    updated_tables = []
    for h5_key in output_store.keys():
        table_name = h5_key.split('/')[-1]
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
