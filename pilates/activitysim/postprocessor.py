import logging
import pandas as pd
import zipfile
import os
logger = logging.getLogger(__name__)


def _load_asim_outputs(settings):
    output_tables_settings = settings['asim_output_tables']
    prefix = output_tables_settings['prefix']
    output_tables = output_tables_settings['tables']
    asim_output_dict = {}
    for table_name in output_tables:
        file_name = "%s%s.csv" % (prefix, table_name)
        file_path = os.path.join(
            settings['asim_local_output_folder'], file_name)
        if table_name == 'persons':
            index_col = 'person_id'
        elif table_name == 'households':
            index_col = 'household_id'
        else:
            index_col = None
        asim_output_dict[table_name] = pd.read_csv(
            file_path, index_col=index_col)

    return asim_output_dict


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


def _prepare_updated_tables(
        settings, year, asim_output_dict, updated_tables, prefix=None):

    data_dir = settings['usim_local_data_folder']
    datastore_name = _get_usim_datastore_fname(
        settings, io='output', year=year)
    output_store = pd.HDFStore(os.path.join(data_dir, datastore_name))
    required_cols = {}
    for table_name in updated_tables:
        h5_key = table_name
        if prefix:
            h5_key = os.path.join(str(prefix), h5_key)
        required_cols[table_name] = list(output_store[h5_key].columns)

    logger.info("Preparing persons table!")
    # new columns to persist: workplace_taz, school_taz
    p_names_dict = {'PNUM': 'member_id'}
    p_cols_to_include = required_cols['persons']
    if 'persons' in asim_output_dict.keys():
        asim_output_dict['persons'].rename(columns=p_names_dict, inplace=True)
        # only preserve original usim columns and two new columns
        for col in ['workplace_taz', 'school_taz']:
            if col not in asim_output_dict['persons'].columns:
                p_cols_to_include.append(col)
        asim_output_dict['persons'] = asim_output_dict['persons'][
            p_cols_to_include]

    logger.info("Preparing households table!")
    # no new columns to persist, just convert column names
    hh_names_dict = {
        'hhsize': 'persons',
        'num_workers': 'workers',
        'auto_ownership': 'cars',
        'PNUM': 'member_id'}

    if 'households' in asim_output_dict.keys():
        asim_output_dict['households'].rename(
            columns=hh_names_dict, inplace=True)
        # only preserve original usim columns
        asim_output_dict['households'] = asim_output_dict[
            'households'][required_cols['households']]

    for table_name in updated_tables:
        h5_key = table_name
        if prefix:
            h5_key = os.path.join(str(prefix), h5_key)
        logger.info(
            "Validating data schemas for table {0}.".format(table_name))

        # make sure all required columns are present
        if not all([
                col in asim_output_dict[table_name].columns
                for col in required_cols[table_name]]):
            raise KeyError(
                "Not all required columns are in the {0} table!".format(
                    table_name))

        # make sure data types match
        else:
            dtypes = output_store[h5_key].dtypes.to_dict()
            for col in required_cols[table_name]:
                if asim_output_dict[table_name][col].dtype != dtypes[col]:
                    asim_output_dict[table_name][col] = asim_output_dict[
                        table_name][col].astype(dtypes[col])

    output_store.close()

    # specific dtype required conversions
    asim_output_dict['households']['block_id'] = asim_output_dict[
        'households']['block_id'].astype(str)

    return asim_output_dict


def create_beam_input_data(settings, year, asim_output_dict):

    asim_output_data_dir = settings['asim_local_output_folder']
    archive_name = 'asim_outputs_{0}.zip'.format(year)
    outpath = os.path.join(asim_output_data_dir, archive_name)
    logger.info(
        'Merging results back into UrbanSim format and storing as .zip!')

    with zipfile.ZipFile(outpath, 'w') as csv_zip:

        # I DONT THINK WE NEED TO INCLUDE URBANSIM DATA HERE ANYMORE
        # # copy usim static inputs into archive
        # for h5_key in usim_datastore.keys():
        #     table_name = h5_key.split('/')[-1]
        #     if prefix:
        #         if str(prefix) not in h5_key:
        #             continue
        #     if table_name not in updated_tables:
        #         logger.info(
        #         "Zipping {0} input table to output archive!".format(
        #             table_name))
        #         df = usim_datastore[h5_key].reset_index()
        #         csv_zip.writestr(
        #             "{0}.csv".format(table_name), pd.DataFrame(df).to_csv())

        # copy asim outputs into archive
        for table_name in asim_output_dict.keys():
            logger.info(
                "Zipping {0} asim table to output archive!".format(table_name))
            csv_zip.writestr(
                table_name + ".csv", asim_output_dict[table_name].to_csv())
    logger.info("Done creating .zip archive!")


def create_usim_input_data(settings, year, asim_output_dict, updated_tables):

    data_dir = settings['usim_local_data_folder']
    datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(data_dir, datastore_name)

    if os.path.exists(input_store_path):
        archive_fname = 'last_iter_model_data.h5'
        logger.info(
            "Moving urbansim inputs from the previous iteration to {0}".format(
                archive_fname))
        new_input_store_path = input_store_path.replace(
            datastore_name, archive_fname)
        os.rename(input_store_path, new_input_store_path)

    og_input_store = pd.HDFStore(new_input_store_path)

    logger.info(
        'Merging results back into UrbanSim format and storing as .h5!')
    new_input_store = pd.HDFStore(input_store_path)

    # copy usim static inputs into archive
    for h5_key in og_input_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            logger.info(
                "Copying {0} input table to output store!".format(
                    table_name))
            new_input_store[table_name] = og_input_store[h5_key]

    # copy asim outputs into archive
    for table_name in updated_tables:
        logger.info(
            "Copying {0} asim table to output store!".format(
                table_name))
        new_input_store[table_name] = asim_output_dict[table_name]

    og_input_store.close()
    new_input_store.close()

    return


def create_next_iter_inputs(settings, year):

    updated_tables = ['households', 'persons']
    asim_output_dict = _load_asim_outputs(settings)
    asim_output_dict = _prepare_updated_tables(
        settings, year, asim_output_dict, updated_tables, prefix=year)

    create_beam_input_data(settings, year, asim_output_dict)
    create_usim_input_data(settings, year, asim_output_dict, updated_tables)

    return
