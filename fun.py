# fun

# the functions are copied from asim.post and revised
# only for testing atlas run
# goal is to update usim input without asim running
# so we can do atlas+usim multi-year run

import logging
import pandas as pd
import os

logger = logging.getLogger(__name__)


def create_usim_input_data_for_test(
        settings, input_year, forecast_year):



    """
    Creates UrbanSim input data for the next iteration.

    Populates an .h5 datastore from three sources in order: 1. ActivitySim
    outputs; 2. UrbanSim outputs; 3. UrbanSim inputs. The three sources will
    have tables in common, so care must be taken to use only the most
    recently updated version of each table. In a given iteration, ActivitySim
    runs last, thus UrbanSim outputs are only passed on if they weren't found
    in the ActivitySim outputs. Likewise, UrbanSim *inputs* are only passed
    on to the next iteration if they were not found in the UrbanSim *outputs*.
    """

    # parse settings
    data_dir = settings['usim_local_data_folder']

    # Move UrbanSim input store (e.g. custom_mpo_193482435_model_data.h5)
    # to archive (e.g. input_data_for_2015_outputs.h5) because otherwise
    # it will be overwritten shortly.
    input_datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(data_dir, input_datastore_name)
    archive_fname = 'input_data_for_{0}_outputs.h5'.format(input_year)
    archive_path = input_store_path.replace(
        input_datastore_name, archive_fname)
    if os.path.exists(input_store_path):
        logger.info(
            "Moving urbansim inputs from the previous iteration to {0}".format(
                archive_fname))
        os.rename(input_store_path, archive_path)
    elif not os.path.exists(archive_fname):
        raise ValueError('No input data found at {0} or {1}.'.format(
            input_store_path, archive_path))

    # load last iter UrbanSim input data
    og_input_store = pd.HDFStore(archive_path)

    # load last iter UrbanSim output data
    usim_output_datastore_name = _get_usim_datastore_fname(
        settings, 'output', forecast_year)
    usim_output_store_path = os.path.join(data_dir, usim_output_datastore_name)
    if not os.path.exists(usim_output_store_path):
        raise ValueError('No output data found at {0}.'.format(
            usim_output_store_path))
    usim_output_store = pd.HDFStore(usim_output_store_path)

    logger.info(
        'Merging results back into UrbanSim format and storing as .h5!')

    # instantiate empty .h5 store (e.g. custom_mpo_321487234_model_data.h5)
    new_input_store = pd.HDFStore(input_store_path)
    assert len(new_input_store.keys()) == 0

    # Keep track of which tables have already been added (i.e. updated)
    updated_tables = []

    # 1. copy ASIM OUTPUTS into new input data store
    # logger.info(
    #     "Copying ActivitySim outputs to the new Urbansim input store!")
    # for table_name in tables_updated_by_asim:
    #     new_input_store[table_name] = asim_output_dict[table_name]
    #     updated_tables.append(table_name)

    # 2. copy USIM OUTPUTS into new input data store if not present already
    logger.info((
        "Passing last set of UrbanSim outputs through to the new "
        "Urbansim input store!"))
    for h5_key in usim_output_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            new_input_store[table_name] = usim_output_store[h5_key]
            updated_tables.append(table_name)

    # 3. copy USIM INPUTS into new input data store if not present already
    logger.info((
        "Passing static UrbanSim inputs through to the new Urbansim "
        "input store!").format(table_name))
    for h5_key in og_input_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            new_input_store[table_name] = og_input_store[h5_key]

    og_input_store.close()
    new_input_store.close()
    usim_output_store.close()

    return




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